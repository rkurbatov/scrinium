package provenance

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/present"
)

// --- in-memory Substrate ---

// memSub is a minimal Substrate: per-table maps with lexicographic Scan. It
// mirrors the contract the sqlite backend provides, which is all the index
// depends on.
type memSub struct {
	tables map[string]map[string][]byte
}

func newMemSub() *memSub { return &memSub{tables: map[string]map[string][]byte{}} }

func (m *memSub) Put(table, key string, value []byte) error {
	t, ok := m.tables[table]
	if !ok {
		t = map[string][]byte{}
		m.tables[table] = t
	}
	t[key] = value
	return nil
}

func (m *memSub) Get(table, key string) ([]byte, bool, error) {
	v, ok := m.tables[table][key]
	return v, ok, nil
}

func (m *memSub) Delete(table, key string) error { delete(m.tables[table], key); return nil }

func (m *memSub) DeletePrefix(table, prefix string) error {
	if prefix == "" {
		return customindex.ErrEmptyPrefix
	}
	for k := range m.tables[table] {
		if strings.HasPrefix(k, prefix) {
			delete(m.tables[table], k)
		}
	}
	return nil
}

func (m *memSub) Scan(table, prefix string, cb func(string, []byte) error) error {
	keys := make([]string, 0, len(m.tables[table]))
	for k := range m.tables[table] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := cb(k, m.tables[table][k]); err != nil {
			if errors.Is(err, customindex.ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (m *memSub) Inc(table, key string, delta int64) (int64, error) {
	return 0, customindex.ErrIncOutsideApply
}

func (m *memSub) rows(table string) int { return len(m.tables[table]) }

// --- fixtures ---

func setupIndex(t *testing.T, cfg Config) (*Index, *memSub) {
	t.Helper()
	idx, sub := NewIndex(cfg), newMemSub()
	if err := idx.Setup(context.Background(), sub, 0); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return idx, sub
}

func id(c string) domain.ArtifactID { return domain.ArtifactID(strings.Repeat(c, 8)) }

// record builds a manifest as the wrapper would have written it: edges in the
// core half, meaning in Ext.
func record(child domain.ArtifactID, op string, outcome Outcome, inputs ...Input) domain.Manifest {
	block := Block{V: SchemaVersion, Op: op, Outcome: outcome, Repro: true}
	refs := make([]domain.HandleRef, 0, len(inputs))
	for _, in := range inputs {
		block.Rel = append(block.Rel, in.Rel)
		refs = append(refs, in.Ref)
	}
	if op != "" {
		block.PKey, _ = ParamsKey(op, nil)
	}
	ext, err := stamp(nil, block)
	if err != nil {
		panic(err)
	}
	return domain.Manifest{ArtifactID: child, HandleRefs: refs, Ext: ext}
}

func indexAll(t *testing.T, idx *Index, sub *memSub, ms ...domain.Manifest) {
	t.Helper()
	for _, m := range ms {
		if _, err := idx.Index(context.Background(), sub, m); err != nil {
			t.Fatalf("Index %s: %v", m.ArtifactID, err)
		}
	}
}

func ids(list []domain.ArtifactID) []string {
	out := make([]string, len(list))
	for i, v := range list {
		out[i] = string(v)
	}
	sort.Strings(out)
	return out
}

// --- traversal in both directions ---

func TestIndex_ParentsAndChildren(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	src, page1, page2, full := id("a"), id("b"), id("c"), id("d")

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
	if got := ids(kids); len(got) != 2 {
		t.Fatalf("children of the source = %v", got)
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
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	pdf, text, chunk := id("a"), id("b"), id("c")

	indexAll(t, idx, sub,
		record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(pdf), Rel: "derived"}),
		record(chunk, "chunk", OutcomeOK, Input{Ref: domain.HandleRef(text), Rel: "derived"}),
	)

	var down []string
	if err := idx.WalkDown(ctx, pdf, 0, func(v domain.ArtifactID, level int) error {
		down = append(down, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(down) != 2 {
		t.Fatalf("cascade enumeration = %v", down)
	}

	// Depth limits the walk to one layer.
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

// A cycle cannot occur in a WORM store, but a corrupt or foreign graph must
// not spin the walker.
func TestIndex_WalkTerminatesOnCycle(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	a, b := id("a"), id("b")
	indexAll(t, idx, sub,
		record(b, "x", OutcomeOK, Input{Ref: domain.HandleRef(a), Rel: "derived"}),
		record(a, "x", OutcomeOK, Input{Ref: domain.HandleRef(b), Rel: "derived"}),
	)
	visited := 0
	if err := idx.WalkDown(context.Background(), a, 0, func(domain.ArtifactID, int) error {
		visited++
		if visited > 10 {
			t.Fatal("walker did not terminate")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// --- holes: the planner's entire state ---

func TestIndex_Holes(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	bookA, bookB := id("a"), id("b")
	textA, textB, chunkA := id("c"), id("d"), id("e")

	indexAll(t, idx, sub,
		record(textA, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookA), Rel: "text"}),
		record(textB, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookB), Rel: "text"}),
		record(chunkA, "chunk", OutcomeOK, Input{Ref: domain.HandleRef(bookA), Rel: "chunk"}),
	)

	var owed []string
	if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "chunk"}, func(v domain.ArtifactID) error {
		owed = append(owed, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(owed) != 1 || owed[0] != string(bookB) {
		t.Fatalf("holes = %v, want only the un-chunked book", owed)
	}

	// Filling the hole removes it from the answer — no queue entry to retire.
	indexAll(t, idx, sub, record(id("f"), "chunk", OutcomeOK, Input{Ref: domain.HandleRef(bookB), Rel: "chunk"}))
	owed = nil
	if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "chunk"}, func(v domain.ArtifactID) error {
		owed = append(owed, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(owed) != 0 {
		t.Fatalf("filled hole still reported: %v", owed)
	}
}

// Failures are records; quarantine is a threshold on their count, not a state.
func TestIndex_FailuresAndQuarantine(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	book, text := id("a"), id("b")

	indexAll(t, idx, sub, record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(book), Rel: "text"}))
	for _, attempt := range []string{"f", "g", "h"} {
		indexAll(t, idx, sub, record(id(attempt), "chunk", OutcomeFailed,
			Input{Ref: domain.HandleRef(book), Rel: "chunk"}))
	}

	n, err := idx.Failures(ctx, book, "chunk", "")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("failures = %d, want 3", n)
	}

	// Under the threshold the source is still offered work; at it, it drops out.
	count := func(max int) int {
		seen := 0
		if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "chunk", MaxFailures: max}, func(domain.ArtifactID) error {
			seen++
			return nil
		}); err != nil {
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

func TestIndex_DoneIsIdempotencyOverInputsAndParams(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	src, out := id("a"), id("b")

	indexAll(t, idx, sub, record(out, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(src), Rel: "derived"}))

	got, ok, err := idx.Done(ctx, mustPKey(t, "ocr"), []domain.HandleRef{domain.HandleRef(src)})
	if err != nil || !ok || got != out {
		t.Fatalf("Done = (%s, %v, %v)", got, ok, err)
	}
	// Other inputs, same work — not done.
	if _, ok, _ := idx.Done(ctx, mustPKey(t, "ocr"), []domain.HandleRef{domain.HandleRef(id("z"))}); ok {
		t.Error("work on other inputs reported as done")
	}
	// Same inputs, other operation — not done.
	if _, ok, _ := idx.Done(ctx, mustPKey(t, "thumbnail"), []domain.HandleRef{domain.HandleRef(src)}); ok {
		t.Error("other work reported as done")
	}
}

// --- supersede chains ---

func TestIndex_HeadResolvesChainAndDetectsFork(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	v1, v2, v3 := id("a"), id("b"), id("c")

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
	// An artifact nobody replaced is its own head.
	if h, err := idx.Head(ctx, v3); err != nil || h != v3 {
		t.Fatalf("Head of a live artifact = (%s, %v)", h, err)
	}

	// Two claimants: mechanical detection, no arbitration.
	indexAll(t, idx, sub, record(id("d"), "merge", OutcomeOK,
		Input{Ref: domain.HandleRef(v2), Rel: DefaultSupersedeRel}))
	if _, err := idx.Head(ctx, v1); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked, got %v", err)
	}
}

// The supersede kind is configurable; every other kind stays opaque.
func TestIndex_SupersedeRelIsConfigurable(t *testing.T) {
	idx, sub := setupIndex(t, Config{SupersedeRel: "replaces"})
	prev, next := id("a"), id("b")
	indexAll(t, idx, sub, record(next, "merge", OutcomeOK,
		Input{Ref: domain.HandleRef(prev), Rel: "replaces"}))

	if h, err := idx.Head(context.Background(), prev); err != nil || h != next {
		t.Fatalf("configured supersede kind not followed: (%s, %v)", h, err)
	}
}

// --- derivability: unindex is symmetric, rebuild replays ---

func TestIndex_UnindexRemovesEverythingItWrote(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	m := record(id("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(id("a")), Rel: DefaultSupersedeRel})

	indexAll(t, idx, sub, m)
	before := map[string]int{}
	for _, tbl := range []string{tableByParent, tableByChild, tableByRel, tableRecords, tableOps, tableHeads} {
		before[tbl] = sub.rows(tbl)
		if before[tbl] == 0 {
			t.Fatalf("table %s stayed empty", tbl)
		}
	}

	if err := idx.Unindex(ctx, sub, m); err != nil {
		t.Fatalf("Unindex: %v", err)
	}
	for tbl := range before {
		if n := sub.rows(tbl); n != 0 {
			t.Errorf("table %s still has %d rows after Unindex", tbl, n)
		}
	}

	// Idempotent: a replay after crash recovery must converge, not error.
	if err := idx.Unindex(ctx, sub, m); err != nil {
		t.Fatalf("second Unindex: %v", err)
	}
}

// An origin — an artifact with no production record — is not this index's
// concern and must leave no rows.
func TestIndex_ManifestWithoutRecordIsSkipped(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	m := domain.Manifest{ArtifactID: id("a"), Ext: json.RawMessage(`{"vfsmeta":{"path":"/x"}}`)}
	if _, err := idx.Index(context.Background(), sub, m); err != nil {
		t.Fatalf("Index: %v", err)
	}
	for _, tbl := range []string{tableByParent, tableByChild, tableByRel, tableRecords, tableOps, tableHeads} {
		if n := sub.rows(tbl); n != 0 {
			t.Errorf("origin wrote %d rows into %s", n, tbl)
		}
	}
}

// A stored record whose meaning does not line up with its edges is corrupt;
// indexing must refuse rather than build half a graph.
func TestIndex_RefusesMisalignedRecord(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	block := Block{V: SchemaVersion, Op: "ocr", Rel: []string{"derived"}}
	ext, err := stamp(nil, block)
	if err != nil {
		t.Fatal(err)
	}
	m := domain.Manifest{
		ArtifactID: id("b"),
		HandleRefs: []domain.HandleRef{domain.HandleRef(id("a")), domain.HandleRef(id("c"))},
		Ext:        ext,
	}
	if _, err := idx.Index(context.Background(), sub, m); !errors.Is(err, ErrBadProduction) {
		t.Fatalf("want ErrBadProduction, got %v", err)
	}
}

func TestIndex_ReadsBeforeRegistrationAreRefused(t *testing.T) {
	idx := NewIndex(Config{})
	if _, err := idx.Parents(context.Background(), id("a")); err == nil {
		t.Fatal("an unregistered index answered a query")
	}
}

func TestIndex_PresentsItsOwnSchema(t *testing.T) {
	idx, _ := setupIndex(t, Config{})
	schemas := idx.PresentedSchemas()
	if len(schemas) != 1 || schemas[0].Key != Key {
		t.Fatalf("presented schemas = %+v", schemas)
	}
	m := record(id("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(id("a")), Rel: "derived"})
	rep, ok, err := schemas[0].Present(m.Ext)
	if err != nil || !ok {
		t.Fatalf("Present: ok=%v err=%v", ok, err)
	}
	if rep.Title == "" || len(rep.Fields) == 0 {
		t.Errorf("empty representation: %+v", rep)
	}
	var _ present.Registry // the registry this feeds is assembled by the host
}

func mustPKey(t *testing.T, op string) string {
	t.Helper()
	k, err := ParamsKey(op, nil)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// A failed attempt is an edge in the graph but not a result: traversal shows
// it, planning does not count it. This is the distinction the hole query rests
// on — without it a stage that keeps breaking would look complete.
func TestIndex_FailedAttemptIsAnEdgeButNotAResult(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	src, attempt := id("a"), id("b")

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
	// It still pins its source: the attempt references it.
	if has, _ := idx.HasChildren(ctx, src); !has {
		t.Error("a failed attempt should still pin its source")
	}
}

// The two candidate shapes are different questions, and a query must name
// exactly one: a chained pipeline asks "is a text, has no chunks", a star
// topology asks "has a text, has no thumbnail".
func TestIndex_HoleQueryShapes(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	book, text := id("a"), id("b")

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

	// Chained: the text itself owes chunks.
	if got := collect(HoleQuery{Is: "text", Missing: "chunk"}); len(got) != 1 || got[0] != string(text) {
		t.Errorf("Is-shape holes = %v, want the text", got)
	}
	// Star: the book has a text and owes a thumbnail.
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

// Deletion must work from identity alone: at delete time the manifest body is
// gone, so the index recovers what it wrote from its own tables.
func TestIndex_UnindexFromIdentityAlone(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	ctx := context.Background()
	m := record(id("b"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(id("a")), Rel: DefaultSupersedeRel})
	indexAll(t, idx, sub, m)

	// Exactly what the core passes on delete: identity, no Ext, no edges.
	bare := domain.Manifest{ArtifactID: m.ArtifactID, Digest: m.Digest}
	if err := idx.Unindex(ctx, sub, bare); err != nil {
		t.Fatalf("Unindex: %v", err)
	}
	for _, tbl := range []string{tableByParent, tableByChild, tableByRel, tableRecords, tableOps, tableHeads} {
		if n := sub.rows(tbl); n != 0 {
			t.Errorf("table %s still has %d rows", tbl, n)
		}
	}
	if err := idx.Unindex(ctx, sub, bare); err != nil {
		t.Fatalf("second Unindex: %v", err)
	}
}

// Every row the index writes must carry a non-empty value: the substrate stores
// values NOT NULL, so a set-membership row still needs a byte.
func TestIndex_NoEmptyValuesWritten(t *testing.T) {
	idx, sub := setupIndex(t, Config{})
	indexAll(t, idx, sub,
		record(id("b"), "merge", OutcomeOK, Input{Ref: domain.HandleRef(id("a")), Rel: DefaultSupersedeRel}),
		record(id("c"), "ocr", OutcomeFailed, Input{Ref: domain.HandleRef(id("a")), Rel: "text"}),
	)
	for table, rows := range sub.tables {
		for key, value := range rows {
			if len(value) == 0 {
				t.Errorf("table %s key %q holds an empty value", table, strings.ReplaceAll(key, sep, "|"))
			}
		}
	}
}
