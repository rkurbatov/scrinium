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

	f, ok := provenance.ExtensionFor(pidx, cfg).Wrapper()
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
