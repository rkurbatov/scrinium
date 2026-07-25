package provenance

import (
	"errors"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/present"
)

// --- contract ---

func TestIndex_SetupRefusesUnknownVersionAndCloses(t *testing.T) {
	idx := NewIndex(Config{})
	if err := idx.Setup(t.Context(), nil, 99); err == nil {
		t.Fatal("an unknown stored schema version was accepted")
	}
	// A refused Setup must not leave the index usable.
	if _, err := idx.Parents(t.Context(), aid("a")); err == nil {
		t.Fatal("index answered after a refused Setup")
	}

	idx, _ = newIndex(t, Config{})
	if err := idx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := idx.Parents(t.Context(), aid("a")); err == nil {
		t.Fatal("index answered after Close")
	}
}

func TestIndex_ReadsBeforeRegistrationAreRefused(t *testing.T) {
	idx := NewIndex(Config{})
	if _, err := idx.Parents(t.Context(), aid("a")); err == nil {
		t.Fatal("an unregistered index answered a query")
	}
}

// A relation kind carrying the key separator would corrupt every composite key
// in the index, so it is refused at write time.
func TestIndex_RejectsSeparatorInRelation(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	m := record(aid("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: "te\x00xt"})
	if _, err := idx.Index(t.Context(), sub, m); !errors.Is(err, ErrBadProduction) {
		t.Fatalf("want ErrBadProduction, got %v", err)
	}
}

// --- traversal ---

func TestIndex_ParentsAndChildren(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	src, page1, page2, full := aid("a"), aid("b"), aid("c"), aid("d")

	indexAll(t, idx, sub,
		record(page1, "split", OutcomeOK, Input{Ref: domain.HandleRef(src), Rel: "page"}),
		record(page2, "split", OutcomeOK, Input{Ref: domain.HandleRef(src), Rel: "page"}),
		// An assembly: many inputs, order significant.
		record(full, "assemble", OutcomeOK,
			Input{Ref: domain.HandleRef(page1), Rel: "part"},
			Input{Ref: domain.HandleRef(page2), Rel: "part"}),
	)

	kids, err := idx.Children(ctx, src, "page")
	if err != nil {
		t.Fatal(err)
	}
	if len(sortedIDs(kids)) != 2 {
		t.Fatalf("children of the source = %v", kids)
	}
	if kids, _ := idx.Children(ctx, src, "part"); len(kids) != 0 {
		t.Errorf("children of another kind leaked: %v", kids)
	}

	parents, err := idx.Parents(ctx, full)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 2 || parents[0].Ref != domain.HandleRef(page1) || parents[1].Ref != domain.HandleRef(page2) {
		t.Fatalf("assembly inputs lost their order: %+v", parents)
	}
	if parents[0].Pos != 0 || parents[1].Pos != 1 {
		t.Errorf("positions wrong: %+v", parents)
	}
}

func TestIndex_WalkUpAndDownAcrossLayers(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	pdf, text, chunk := aid("a"), aid("b"), aid("c")

	indexAll(t, idx, sub,
		record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(pdf), Rel: "derived"}),
		record(chunk, "chunk", OutcomeOK, Input{Ref: domain.HandleRef(text), Rel: "derived"}),
	)

	var down []string
	if err := idx.WalkDown(ctx, pdf, 0, func(v domain.ArtifactID, _ int) error {
		down = append(down, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(down) != 2 {
		t.Fatalf("cascade enumeration = %v", down)
	}

	var oneLevel []string
	if err := idx.WalkDown(ctx, pdf, 1, func(v domain.ArtifactID, _ int) error {
		oneLevel = append(oneLevel, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(oneLevel) != 1 || oneLevel[0] != string(text) {
		t.Fatalf("depth ignored: %v", oneLevel)
	}

	var up []string
	if err := idx.WalkUp(ctx, chunk, 0, func(v domain.ArtifactID, _ int) error {
		up = append(up, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(up) != 2 {
		t.Fatalf("path to the primary source = %v", up)
	}
}

// A cycle cannot occur in a WORM store, but a corrupt or foreign graph must not
// spin the walker.
func TestIndex_WalkTerminatesOnCycle(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	a, b := aid("a"), aid("b")
	indexAll(t, idx, sub,
		record(b, "x", OutcomeOK, Input{Ref: domain.HandleRef(a), Rel: "derived"}),
		record(a, "x", OutcomeOK, Input{Ref: domain.HandleRef(b), Rel: "derived"}),
	)
	visited := 0
	if err := idx.WalkDown(t.Context(), a, 0, func(domain.ArtifactID, int) error {
		visited++
		if visited > 10 {
			t.Fatal("walker did not terminate")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// An error from the callback is how a consumer stops a walk; it must come back
// unchanged rather than being swallowed or wrapped into something unrecognisable.
func TestIndex_WalkPropagatesCallbackError(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	a, b, c := aid("a"), aid("b"), aid("c")
	indexAll(t, idx, sub,
		record(b, "x", OutcomeOK, Input{Ref: domain.HandleRef(a), Rel: "derived"}),
		record(c, "x", OutcomeOK, Input{Ref: domain.HandleRef(b), Rel: "derived"}),
	)

	stop := errors.New("enough")
	seen := 0
	cb := func(domain.ArtifactID, int) error {
		seen++
		return stop
	}
	if err := idx.WalkDown(ctx, a, 0, cb); !errors.Is(err, stop) {
		t.Fatalf("WalkDown swallowed the callback error: %v", err)
	}
	if seen != 1 {
		t.Errorf("walk continued after the callback stopped it: %d visits", seen)
	}
	if err := idx.WalkUp(ctx, c, 0, func(domain.ArtifactID, int) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("WalkUp swallowed the callback error: %v", err)
	}
}

// --- planning ---

func TestIndex_Holes(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	bookA, bookB := aid("a"), aid("b")
	textA, textB, chunkA := aid("c"), aid("d"), aid("e")

	indexAll(t, idx, sub,
		record(textA, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookA), Rel: "text"}),
		record(textB, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookB), Rel: "text"}),
		record(chunkA, "chunk", OutcomeOK, Input{Ref: domain.HandleRef(bookA), Rel: "chunk"}),
	)

	collect := func() []string {
		var out []string
		if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "chunk"}, func(v domain.ArtifactID) error {
			out = append(out, string(v))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	owed := collect()
	if len(owed) != 1 || owed[0] != string(bookB) {
		t.Fatalf("holes = %v, want only the un-chunked book", owed)
	}

	// Filling the hole removes it from the answer — no queue entry to retire.
	indexAll(t, idx, sub, record(aid("f"), "chunk", OutcomeOK, Input{Ref: domain.HandleRef(bookB), Rel: "chunk"}))
	if owed := collect(); len(owed) != 0 {
		t.Fatalf("filled hole still reported: %v", owed)
	}
}

// The two candidate shapes are different questions, and a query must name exactly
// one: a chained pipeline asks "is a text, has no chunks", a star topology asks
// "has a text, has no thumbnail".
func TestIndex_HoleQueryShapes(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	book, text := aid("a"), aid("b")

	indexAll(t, idx, sub, record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(book), Rel: "text"}))

	collect := func(q HoleQuery) []string {
		var out []string
		if err := idx.Holes(ctx, q, func(v domain.ArtifactID) error {
			out = append(out, string(v))
			return nil
		}); err != nil {
			t.Fatalf("Holes(%+v): %v", q, err)
		}
		return out
	}

	if got := collect(HoleQuery{Is: "text", Missing: "chunk"}); len(got) != 1 || got[0] != string(text) {
		t.Errorf("Is-shape holes = %v, want the text", got)
	}
	if got := collect(HoleQuery{Has: "text", Missing: "thumb"}); len(got) != 1 || got[0] != string(book) {
		t.Errorf("Has-shape holes = %v, want the book", got)
	}

	for _, bad := range []HoleQuery{
		{Missing: "chunk"},
		{Is: "text"},
		{Is: "text", Has: "text", Missing: "chunk"},
	} {
		if err := idx.Holes(ctx, bad, func(domain.ArtifactID) error { return nil }); err == nil {
			t.Errorf("malformed query accepted: %+v", bad)
		}
	}
}

func TestIndex_HolesPropagatesCallbackError(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	indexAll(t, idx, sub,
		record(aid("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: "text"}),
	)
	stop := errors.New("enough")
	err := idx.Holes(t.Context(), HoleQuery{Is: "text", Missing: "chunk"}, func(domain.ArtifactID) error {
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Holes swallowed the callback error: %v", err)
	}
}

// Failures are records; quarantine is a threshold on their count, not a state.
func TestIndex_FailuresAndQuarantine(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	book, text := aid("a"), aid("b")

	indexAll(t, idx, sub, record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(book), Rel: "text"}))
	for _, attempt := range []string{"f", "g", "h"} {
		indexAll(t, idx, sub, record(aid(attempt), "chunk", OutcomeFailed,
			Input{Ref: domain.HandleRef(book), Rel: "chunk"}))
	}

	n, err := idx.Failures(ctx, book, "chunk", "")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("failures = %d, want 3", n)
	}

	count := func(max int) int {
		seen := 0
		if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "chunk", MaxFailures: max},
			func(domain.ArtifactID) error { seen++; return nil }); err != nil {
			t.Fatal(err)
		}
		return seen
	}
	if count(5) != 1 {
		t.Error("source below the failure threshold was not offered")
	}
	if count(3) != 0 {
		t.Error("source at the failure threshold was still offered")
	}

	// A failed attempt is not a result: it must not count as work done.
	if _, ok, err := idx.Done(ctx, mustPKey(t, "chunk"), []domain.HandleRef{domain.HandleRef(book)}); err != nil || ok {
		t.Errorf("a failure registered as completed work: ok=%v err=%v", ok, err)
	}
}

// A failed attempt is an edge in the graph but not a result: traversal shows it,
// planning does not count it.
func TestIndex_FailedAttemptIsAnEdgeButNotAResult(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	src, attempt := aid("a"), aid("b")

	indexAll(t, idx, sub, record(attempt, "ocr", OutcomeFailed,
		Input{Ref: domain.HandleRef(src), Rel: "text"}))

	kids, err := idx.Children(ctx, src, "text")
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 {
		t.Fatalf("failed attempt missing from the graph: %v", kids)
	}
	res, err := idx.Results(ctx, src, "text")
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("failed attempt counted as a result: %v", res)
	}
	if has, _ := idx.HasResultOf(ctx, src, "text"); has {
		t.Error("HasResultOf true for a failed attempt only")
	}
	if has, _ := idx.HasChildOf(ctx, src, ""); !has {
		t.Error("a failed attempt should still pin its source")
	}
}

func TestIndex_DoneIsIdempotencyOverInputsAndParams(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	src, out := aid("a"), aid("b")

	indexAll(t, idx, sub, record(out, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(src), Rel: "derived"}))

	got, ok, err := idx.Done(ctx, mustPKey(t, "ocr"), []domain.HandleRef{domain.HandleRef(src)})
	if err != nil || !ok || got != out {
		t.Fatalf("Done = (%s, %v, %v)", got, ok, err)
	}
	if _, ok, _ := idx.Done(ctx, mustPKey(t, "ocr"), []domain.HandleRef{domain.HandleRef(aid("z"))}); ok {
		t.Error("work on other inputs reported as done")
	}
	if _, ok, _ := idx.Done(ctx, mustPKey(t, "thumbnail"), []domain.HandleRef{domain.HandleRef(src)}); ok {
		t.Error("other work reported as done")
	}
	// A record with no operation has no work identity and claims nothing.
	if _, ok, err := idx.Done(ctx, "", []domain.HandleRef{domain.HandleRef(src)}); err != nil || ok {
		t.Errorf("empty work key matched something: ok=%v err=%v", ok, err)
	}
}

// --- currency ---

func TestIndex_HeadResolvesChainAndDetectsFork(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	v1, v2, v3 := aid("a"), aid("b"), aid("c")

	indexAll(t, idx, sub,
		record(v2, "merge", OutcomeOK, Input{Ref: domain.HandleRef(v1), Rel: DefaultSupersedeRel}),
		record(v3, "merge", OutcomeOK, Input{Ref: domain.HandleRef(v2), Rel: DefaultSupersedeRel}),
	)

	head, err := idx.Head(ctx, v1)
	if err != nil {
		t.Fatal(err)
	}
	if head != v3 {
		t.Fatalf("head = %s, want the last replacement %s", head, v3)
	}
	if h, err := idx.Head(ctx, v3); err != nil || h != v3 {
		t.Fatalf("Head of a live artifact = (%s, %v)", h, err)
	}

	indexAll(t, idx, sub, record(aid("d"), "merge", OutcomeOK,
		Input{Ref: domain.HandleRef(v2), Rel: DefaultSupersedeRel}))
	if _, err := idx.Head(ctx, v1); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked, got %v", err)
	}
}

func TestIndex_SupersedeRelIsConfigurable(t *testing.T) {
	idx, sub := newIndex(t, Config{SupersedeRel: "replaces"})
	prev, next := aid("a"), aid("b")
	indexAll(t, idx, sub, record(next, "merge", OutcomeOK,
		Input{Ref: domain.HandleRef(prev), Rel: "replaces"}))

	if h, err := idx.Head(t.Context(), prev); err != nil || h != next {
		t.Fatalf("configured supersede kind not followed: (%s, %v)", h, err)
	}
}

// --- derivability ---

func TestIndex_UnindexRemovesEverythingItWrote(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	m := record(aid("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: DefaultSupersedeRel})

	indexAll(t, idx, sub, m)
	for _, tbl := range allTables() {
		if sub.Rows(tbl) == 0 {
			t.Fatalf("table %s stayed empty", tbl)
		}
	}

	if err := idx.Unindex(ctx, sub, m); err != nil {
		t.Fatalf("Unindex: %v", err)
	}
	if left := sub.Tables(); len(left) != 0 {
		t.Errorf("tables still populated after Unindex: %v", left)
	}
	if err := idx.Unindex(ctx, sub, m); err != nil {
		t.Fatalf("second Unindex: %v", err)
	}
}

// Deletion must work from identity alone: at delete time the manifest body is
// gone, so the index recovers what it wrote from its own tables.
func TestIndex_UnindexFromIdentityAlone(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	m := record(aid("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: DefaultSupersedeRel})
	indexAll(t, idx, sub, m)

	bare := domain.Manifest{ArtifactID: m.ArtifactID, Digest: m.Digest}
	if err := idx.Unindex(t.Context(), sub, bare); err != nil {
		t.Fatalf("Unindex: %v", err)
	}
	if left := sub.Tables(); len(left) != 0 {
		t.Errorf("tables still populated: %v", left)
	}
}

// An origin — an artifact with no production record — is not this index's concern
// and must leave no rows.
func TestIndex_ManifestWithoutRecordIsSkipped(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	m := manifestfxBlobWithForeignExt(aid("a"))
	if _, err := idx.Index(t.Context(), sub, m); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if left := sub.Tables(); len(left) != 0 {
		t.Errorf("origin wrote rows into %v", left)
	}
}

// A stored record whose meaning does not line up with its edges is corrupt;
// indexing must refuse rather than build half a graph.
func TestIndex_RefusesMisalignedRecord(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	block := Block{V: SchemaVersion, Op: "ocr", Rel: []string{"derived"}}
	ext, err := stamp(nil, block)
	if err != nil {
		t.Fatal(err)
	}
	m := domain.Manifest{
		ArtifactID: aid("b"),
		HandleRefs: []domain.HandleRef{domain.HandleRef(aid("a")), domain.HandleRef(aid("c"))},
		Ext:        ext,
	}
	if _, err := idx.Index(t.Context(), sub, m); !errors.Is(err, ErrBadProduction) {
		t.Fatalf("want ErrBadProduction, got %v", err)
	}
}

// Every row the index writes must carry a non-empty value: the substrate stores
// values NOT NULL, so a set-membership row still needs a byte.
func TestIndex_NoEmptyValuesWritten(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	indexAll(t, idx, sub,
		record(aid("b"), "merge", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: DefaultSupersedeRel}),
		record(aid("c"), "ocr", OutcomeFailed, Input{Ref: domain.HandleRef(aid("a")), Rel: "text"}),
		receiptManifest(aid("r"), aid("a"), Config{}),
	)
	sub.Each(func(table, key string, value []byte) {
		if len(value) == 0 {
			t.Errorf("table %s holds an empty value at key %q", table, key)
		}
	})
}

// --- presentation ---

func TestIndex_PresentsItsOwnSchema(t *testing.T) {
	idx, _ := newIndex(t, Config{})
	schemas := idx.PresentedSchemas()
	if len(schemas) != 1 || schemas[0].Key != Key {
		t.Fatalf("presented schemas = %+v", schemas)
	}
	m := record(aid("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(aid("a")), Rel: "derived"})
	rep, ok, err := schemas[0].Present(m.Ext)
	if err != nil || !ok {
		t.Fatalf("Present: ok=%v err=%v", ok, err)
	}
	if rep.Title == "" || len(rep.Fields) == 0 {
		t.Errorf("empty representation: %+v", rep)
	}
	var _ present.Registry // the registry this feeds is assembled by the host
}

// compile-time: the index really is what the contract expects of it.
var (
	_ customindex.Indexer     = (*Index)(nil)
	_ present.SchemaPresenter = (*Index)(nil)
)
