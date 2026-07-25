package provenance_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index/sqlite"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/testutil/artifactfx"
	"scrinium.dev/testutil/storefx"
	"scrinium.dev/x/provenance"
)

// These tests drive the whole extension end to end: a real Store over a real
// sqlite index, with the provenance index registered and the behavioral
// wrapper applied. Nothing is faked, so they check the parts the unit tests
// cannot — that the core actually persists the edges into the manifest, that
// IndexManifest dispatches Index inside the write transaction, that the
// substrate's prefix scans behave as the key layout assumes, and that Delete
// dispatches Unindex so the graph shrinks with the store.

// harness builds a store whose index hosts the provenance custom index, and
// returns the store as seen through the extension's wrapper plus the index for
// direct questions. Wiring the parts by hand is equivalent to what the
// assembler does from a Config.
func harness(t *testing.T, cfg provenance.Config) (store.DataStore, *provenance.Index) {
	t.Helper()
	ctx := context.Background()

	idx, err := sqlite.NewStore(ctx, ":memory:")
	if err != nil {
		t.Fatalf("sqlite.NewStore: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	pidx := provenance.NewIndex(cfg)
	if err := idx.CustomIndexes().Register(ctx, pidx); err != nil {
		t.Fatalf("Register: %v", err)
	}

	st := storefx.Init(t, store.WithStoreIndex(idx))

	f, ok := provenance.ExtensionFor(pidx).Wrapper()
	if !ok {
		t.Fatal("provenance occupies no behavior axis")
	}
	wrapped, err := f.Wrap(st, wrapper.Deps{})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	return wrapped, pidx
}

func put(t *testing.T, s store.DataStore, body string, opts ...domain.PutOption) domain.ArtifactID {
	t.Helper()
	id, err := s.Put(context.Background(), artifactfx.Payload(body), opts...)
	if err != nil {
		t.Fatalf("Put(%q): %v", body, err)
	}
	return id
}

// The full round trip of one derivation: an origin, a derivative declaring it,
// and both halves of the record landing where they belong — edges in the
// manifest the core wrote, meaning in the index the extension keeps.
func TestIntegration_DerivationRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	origin := put(t, s, "scanned book bytes")
	text := put(t, s, "recognised text", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(origin), Rel: "text"}},
		Op:     "ocr",
		Params: []byte(`{"tool":"marker","ver":"1.2"}`),
		Repro:  true,
	}))

	// The core half: the edge is in the manifest on disk, not only in an index.
	rh, err := s.Get(ctx, text)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	m := rh.Manifest()
	_ = rh.Close()
	if len(m.HandleRefs) != 1 || m.HandleRefs[0] != domain.HandleRef(origin) {
		t.Fatalf("manifest carries no edge to the origin: %v", m.HandleRefs)
	}

	// The extension half: the graph answers in both directions.
	parents, err := pidx.Parents(ctx, text)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0].Ref != domain.HandleRef(origin) || parents[0].Rel != "text" {
		t.Fatalf("parents = %+v", parents)
	}
	kids, err := pidx.Results(ctx, origin, "text")
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0] != text {
		t.Fatalf("results of the origin = %v", kids)
	}

	// An origin has no record, and that is not an absence of data.
	if ps, err := pidx.Parents(ctx, origin); err != nil || len(ps) != 0 {
		t.Fatalf("origin has parents: %v (%v)", ps, err)
	}
}

// The pipeline's whole planning surface, over a real store: two books, one
// already chunked, and the hole query naming exactly the one that owes work.
func TestIntegration_HolesDrivePipeline(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	bookA := put(t, s, "book A")
	bookB := put(t, s, "book B")

	textOf := func(src domain.ArtifactID, body string) domain.ArtifactID {
		return put(t, s, body, provenance.WithProduction(provenance.Production{
			Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
			Op:     "ocr",
			Repro:  true,
		}))
	}
	textA := textOf(bookA, "text A")
	textB := textOf(bookB, "text B")

	put(t, s, "chunks of A", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(textA), Rel: "chunk"}},
		Op:     "chunk",
		Repro:  true,
	}))

	// Stage two asks: who has text but no chunks. Text B does; text A does not.
	var owed []domain.ArtifactID
	if err := pidx.Holes(ctx, provenance.HoleQuery{Is: "text", Missing: "chunk"}, func(id domain.ArtifactID) error {
		owed = append(owed, id)
		return nil
	}); err != nil {
		t.Fatalf("Holes: %v", err)
	}
	if len(owed) != 1 {
		t.Fatalf("holes = %v, want exactly the un-chunked text", owed)
	}
	if owed[0] == textA {
		t.Fatalf("the already-chunked text was offered work")
	}
	if owed[0] != textB {
		t.Fatalf("holes named %s, want the un-chunked text %s", owed[0], textB)
	}
}

// Doing the same work twice is the mistake idempotency exists to prevent: the
// same operation, parameters and inputs must be recognisable as already done.
func TestIntegration_WorkIsNotRepeated(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	src := put(t, s, "source")
	params := []byte(`{"tool":"marker","ver":"1.2"}`)
	out := put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Params: params,
		Repro:  true,
	}))

	pkey, err := provenance.ParamsKey("ocr", params)
	if err != nil {
		t.Fatal(err)
	}
	got, done, err := pidx.Done(ctx, pkey, []domain.HandleRef{domain.HandleRef(src)})
	if err != nil || !done || got != out {
		t.Fatalf("Done = (%s, %v, %v)", got, done, err)
	}

	// Different parameters are different work, even on the same input.
	other, err := provenance.ParamsKey("ocr", []byte(`{"tool":"marker","ver":"2.0"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, done, _ := pidx.Done(ctx, other, []domain.HandleRef{domain.HandleRef(src)}); done {
		t.Error("work with newer parameters reported as already done")
	}
}

// Replacement is a record, not a mutation: the superseded artifact stays,
// and "what is current" is resolved by walking the chain.
func TestIntegration_SupersedeChainOverRealStore(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	v1 := put(t, s, "first scan")
	v2 := put(t, s, "better scan", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(v1), Rel: provenance.DefaultSupersedeRel}},
		Op:     "rescan",
	}))

	head, err := pidx.Head(ctx, v1)
	if err != nil {
		t.Fatal(err)
	}
	if head != v2 {
		t.Fatalf("head = %s, want %s", head, v2)
	}
	// The superseded artifact is still readable — history is not overwritten.
	if _, err := s.Get(ctx, v1); err != nil {
		t.Fatalf("superseded artifact became unreadable: %v", err)
	}
}

// Delete must dispatch Unindex, or the graph would outlive the artifacts and
// start answering with ghosts.
func TestIntegration_DeleteShrinksTheGraph(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	src := put(t, s, "source")
	derived := put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))
	if kids, _ := pidx.Children(ctx, src, ""); len(kids) != 1 {
		t.Fatalf("setup: children = %v", kids)
	}

	if err := s.Delete(ctx, derived); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	kids, err := pidx.Children(ctx, src, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 0 {
		t.Fatalf("graph still lists a deleted derivative: %v", kids)
	}
	if ps, _ := pidx.Parents(ctx, derived); len(ps) != 0 {
		t.Fatalf("deleted artifact still has parents: %+v", ps)
	}
}

// With the guard armed, a source with derivatives cannot be deleted through the
// wrapped store — the interim stand-in for the core's edge accounting.
func TestIntegration_GuardProtectsSources(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	src := put(t, s, "source")
	derived := put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))

	if err := s.Delete(ctx, src); !errors.Is(err, provenance.ErrHasDerivatives) {
		t.Fatalf("deleting a source with derivatives: want ErrHasDerivatives, got %v", err)
	}
	// Removing the derivative first frees the source.
	if err := s.Delete(ctx, derived); err != nil {
		t.Fatalf("Delete derivative: %v", err)
	}
	if has, err := pidx.HasChildren(ctx, src); err != nil || has {
		t.Fatalf("source still pinned after its only derivative went: %v (%v)", has, err)
	}
	if err := s.Delete(ctx, src); err != nil {
		t.Fatalf("Delete freed source: %v", err)
	}
}

// A malformed record must be refused before anything is written, so a
// half-meant provenance can never become permanent.
func TestIntegration_BadRecordWritesNothing(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	src := put(t, s, "source")
	_, err := s.Put(ctx, artifactfx.Payload("derived"), provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src)}}, // no relation kind
		Op:     "ocr",
	}))
	if !errors.Is(err, provenance.ErrBadProduction) {
		t.Fatalf("want ErrBadProduction, got %v", err)
	}
	if kids, _ := pidx.Children(ctx, src, ""); len(kids) != 0 {
		t.Fatalf("rejected record left rows behind: %v", kids)
	}
}

// A many-input assembly over a real store: the order of the parts is data and
// must survive the manifest, the index and a read back.
func TestIntegration_AssemblyKeepsInputOrder(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{})

	pages := make([]provenance.Input, 0, 5)
	for i := 0; i < 5; i++ {
		id := put(t, s, "page "+strings.Repeat("x", i+1))
		pages = append(pages, provenance.Input{Ref: domain.HandleRef(id), Rel: "part"})
	}
	full := put(t, s, "full text", provenance.WithProduction(provenance.Production{
		Inputs: pages,
		Op:     "assemble",
		Repro:  true,
	}))

	parents, err := pidx.Parents(ctx, full)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != len(pages) {
		t.Fatalf("inputs = %d, want %d", len(parents), len(pages))
	}
	for i, p := range parents {
		if p.Ref != pages[i].Ref || p.Pos != i {
			t.Fatalf("input %d out of order: %+v", i, p)
		}
	}
}

// Eviction over a real store: the source's bytes go, its derivatives stay, and
// the receipt explains what happened. Ordinary deletion rules apply throughout —
// eviction only adds the precondition and the receipt.
func TestIntegration_EvictSourceKeepDerivatives(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	pdf := put(t, s, "scanned pdf bytes")
	text := put(t, s, "recognised text", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(pdf), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))

	// Without a receipt the source is pinned.
	if err := s.Delete(ctx, pdf); !errors.Is(err, provenance.ErrHasDerivatives) {
		t.Fatalf("unexplained delete: want ErrHasDerivatives, got %v", err)
	}

	ev, err := provenance.NewEvictor(s, pidx)
	if err != nil {
		t.Fatalf("NewEvictor: %v", err)
	}
	if err := ev.Evict(ctx, pdf, provenance.ReceiptSpec{
		Retained:  []domain.ArtifactID{text},
		Reason:    "ocr-complete",
		DecidedBy: "roman",
	}); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	// The bytes are gone.
	if _, err := s.Get(ctx, pdf); err == nil {
		t.Fatal("evicted artifact still readable")
	}
	// The derivative is untouched and still names its source: the graph is not
	// rewritten, only reachability changes.
	if _, err := s.Get(ctx, text); err != nil {
		t.Fatalf("derivative lost: %v", err)
	}
	parents, err := pidx.Parents(ctx, text)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0].Ref != domain.HandleRef(pdf) {
		t.Fatalf("derivative lost its declared source: %+v", parents)
	}

	// And the dangling reference has an explanation.
	r, has, err := ev.ReadReceipt(ctx, pdf)
	if err != nil || !has {
		t.Fatalf("ReadReceipt: has=%v err=%v", has, err)
	}
	if r.Evicted.Artifact != pdf || r.Reason != "ocr-complete" || r.DecidedBy != "roman" {
		t.Fatalf("receipt does not describe the eviction: %+v", r)
	}
	if len(r.Retained) != 1 || r.Retained[0] != text {
		t.Fatalf("retained list wrong: %v", r.Retained)
	}
}

// Eviction is idempotent, and the receipt is a standing decision: a repeat call
// after the delete already happened must not write a second receipt.
func TestIntegration_EvictIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	src := put(t, s, "source")
	_ = put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))

	ev, err := provenance.NewEvictor(s, pidx)
	if err != nil {
		t.Fatal(err)
	}
	spec := provenance.ReceiptSpec{Reason: "ocr-complete", DecidedBy: "roman"}
	if err := ev.Evict(ctx, src, spec); err != nil {
		t.Fatalf("first Evict: %v", err)
	}

	first, has, err := pidx.Receipt(ctx, src)
	if err != nil || !has {
		t.Fatalf("no receipt after eviction: %v", err)
	}
	// The artifact is already gone, so the retry can only re-attempt the delete.
	if err := ev.Evict(ctx, src, spec); err == nil {
		t.Fatal("re-evicting a gone artifact should surface the delete error")
	}
	again, _, err := pidx.Receipt(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("a second receipt was written: %s then %s", first, again)
	}
}

// A receipt without a stated reason or decider is refused before anything is
// written: an eviction that cannot be attributed is indistinguishable from loss.
func TestIntegration_EvictRefusesUnexplained(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	src := put(t, s, "source")
	_ = put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))

	ev, _ := provenance.NewEvictor(s, pidx)
	if err := ev.Evict(ctx, src, provenance.ReceiptSpec{Reason: "ocr-complete"}); !errors.Is(err, provenance.ErrBadReceipt) {
		t.Fatalf("want ErrBadReceipt, got %v", err)
	}
	if has, _ := pidx.HasReceipt(ctx, src); has {
		t.Error("a refused eviction still wrote a receipt")
	}
	if _, err := s.Get(ctx, src); err != nil {
		t.Errorf("a refused eviction deleted the artifact: %v", err)
	}
}

// The receipt survives: deleting it would leave a dangling reference with no
// account of why the bytes are gone.
func TestIntegration_ReceiptIsProtected(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	src := put(t, s, "source")
	_ = put(t, s, "derived", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))
	ev, _ := provenance.NewEvictor(s, pidx)
	if err := ev.Evict(ctx, src, provenance.ReceiptSpec{Reason: "ocr-complete", DecidedBy: "roman"}); err != nil {
		t.Fatal(err)
	}

	rid, has, err := pidx.Receipt(ctx, src)
	if err != nil || !has {
		t.Fatal("no receipt")
	}
	if err := s.Delete(ctx, rid); !errors.Is(err, provenance.ErrReceiptProtected) {
		t.Fatalf("want ErrReceiptProtected, got %v", err)
	}
	if _, err := s.Get(ctx, rid); err != nil {
		t.Errorf("receipt was deleted anyway: %v", err)
	}
}

// An evicted artifact must stop being offered work: its parent-side rows survive
// the delete, and without the receipt check a stage would keep failing on bytes
// that are gone.
func TestIntegration_EvictedIsNotOfferedWork(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	bookA, bookB := put(t, s, "book A"), put(t, s, "book B")
	for _, b := range []domain.ArtifactID{bookA, bookB} {
		put(t, s, "text of "+string(b), provenance.WithProduction(provenance.Production{
			Inputs: []provenance.Input{{Ref: domain.HandleRef(b), Rel: "text"}},
			Op:     "ocr",
			Repro:  true,
		}))
	}

	owed := func() []domain.ArtifactID {
		var out []domain.ArtifactID
		if err := pidx.Holes(ctx, provenance.HoleQuery{Has: "text", Missing: "thumb"}, func(v domain.ArtifactID) error {
			out = append(out, v)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if len(owed()) != 2 {
		t.Fatalf("setup: both books owe a thumbnail, got %v", owed())
	}

	ev, _ := provenance.NewEvictor(s, pidx)
	if err := ev.Evict(ctx, bookA, provenance.ReceiptSpec{Reason: "space", DecidedBy: "roman"}); err != nil {
		t.Fatal(err)
	}
	got := owed()
	if len(got) != 1 || got[0] != bookB {
		t.Fatalf("holes after eviction = %v, want only the live book", got)
	}
}

// Effective reproducibility over a real store: the text is a cache while the pdf
// is on disk and becomes data the moment it is evicted, though its own flag
// never changes.
func TestIntegration_CleanableAfterEviction(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	pdf := put(t, s, "scanned pdf")
	text := put(t, s, "text", provenance.WithProduction(provenance.Production{
		Inputs: []provenance.Input{{Ref: domain.HandleRef(pdf), Rel: "text"}},
		Op:     "ocr",
		Repro:  true,
	}))

	alive := func(ctx context.Context, v domain.ArtifactID) (bool, error) {
		rh, err := s.Get(ctx, v)
		if err != nil {
			return false, nil
		}
		_ = rh.Close()
		return true, nil
	}

	if ok, err := pidx.Cleanable(ctx, text, alive); err != nil || !ok {
		t.Fatalf("text should be cleanable while the pdf lives: (%v, %v)", ok, err)
	}

	ev, _ := provenance.NewEvictor(s, pidx)
	if err := ev.Evict(ctx, pdf, provenance.ReceiptSpec{
		Retained: []domain.ArtifactID{text}, Reason: "ocr-complete", DecidedBy: "roman",
	}); err != nil {
		t.Fatal(err)
	}

	if ok, err := pidx.Cleanable(ctx, text, alive); err != nil || ok {
		t.Fatalf("text must not be cleanable once its source is evicted: (%v, %v)", ok, err)
	}
}

// The receipt is written outside the caller's session on purpose: rolling back an
// eviction batch must not erase the explanations of artifacts that are already
// gone (ADR-113 П-6).
func TestIntegration_ReceiptSurvivesSessionRollback(t *testing.T) {
	ctx := context.Background()
	s, pidx := harness(t, provenance.Config{GuardDeletes: true})

	sess := domain.SessionID("eviction-batch-1")
	src := put(t, s, "source", domain.WithSession(sess))
	text := put(t, s, "text", domain.WithSession(sess),
		provenance.WithProduction(provenance.Production{
			Inputs: []provenance.Input{{Ref: domain.HandleRef(src), Rel: "text"}},
			Op:     "ocr",
			Repro:  true,
		}))

	ev, err := provenance.NewEvictor(s, pidx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ev.Evict(ctx, src, provenance.ReceiptSpec{
		Retained: []domain.ArtifactID{text}, Reason: "ocr-complete", DecidedBy: "roman",
	}); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	rid, has, err := pidx.Receipt(ctx, src)
	if err != nil || !has {
		t.Fatalf("no receipt: %v", err)
	}

	admin, ok := s.(interface {
		RollbackSession(context.Context, domain.SessionID) error
	})
	if !ok {
		t.Skip("store handle does not expose RollbackSession")
	}
	if err := admin.RollbackSession(ctx, sess); err != nil {
		t.Fatalf("RollbackSession: %v", err)
	}

	// The batch is gone; the explanation is not.
	if _, err := s.Get(ctx, rid); err != nil {
		t.Fatalf("rollback destroyed the receipt: %v", err)
	}
	r, has, err := ev.ReadReceipt(ctx, src)
	if err != nil || !has || r.Evicted.Artifact != src {
		t.Fatalf("receipt unreadable after rollback: has=%v err=%v", has, err)
	}
}
