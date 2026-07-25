package provenance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/errs"
)

// --- fakes ---

// fakeHandle is a minimal ReadHandle; the wrapper never reads bytes.
type fakeHandle struct{ m domain.Manifest }

func (h fakeHandle) Read([]byte) (int, error)                              { return 0, io.EOF }
func (h fakeHandle) ReadAt([]byte, int64) (int, error)                     { return 0, io.EOF }
func (h fakeHandle) ReadAtCtx(context.Context, []byte, int64) (int, error) { return 0, io.EOF }
func (h fakeHandle) SupportsRandomAccess() bool                            { return false }
func (h fakeHandle) Close() error                                          { return nil }
func (h fakeHandle) Manifest() domain.Manifest                             { return h.m }

// fakeDataStore embeds the interface and defines only what the wrapper calls.
type fakeDataStore struct {
	store.DataStore
	lastExt  json.RawMessage
	lastOpts domain.PutOptions
	deleted  []domain.ArtifactID
}

func (f *fakeDataStore) Put(_ context.Context, a domain.Artifact, opts ...domain.PutOption) (domain.ArtifactID, error) {
	f.lastExt = a.Ext
	f.lastOpts = domain.ApplyPut(opts...)
	return "child-id", nil
}

func (f *fakeDataStore) Delete(_ context.Context, id domain.ArtifactID) error {
	f.deleted = append(f.deleted, id)
	return nil
}

// fakeLookup answers the guard's one question.
type fakeLookup struct {
	children map[domain.ArtifactID]bool
	err      error
}

func (l fakeLookup) HasChildren(_ context.Context, id domain.ArtifactID) (bool, error) {
	return l.children[id], l.err
}

func wrapStore(t *testing.T, cfg Config, lookup derivativeLookup, inner store.DataStore) store.DataStore {
	t.Helper()
	s, err := factory{cfg: cfg, lookup: lookup}.Wrap(inner, wrapper.Deps{})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	return s
}

func ref(c string) domain.HandleRef { return domain.HandleRef(strings.Repeat(c, 64)) }

// --- Put: the record splits into edges (core) and meaning (Ext) ---

func TestPut_StampsRecordAndPassesEdgesToCore(t *testing.T) {
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{}, nil, inner)

	src := ref("a")
	_, err := s.Put(context.Background(), domain.Artifact{},
		WithProduction(Production{
			Inputs: []Input{{Ref: src, Rel: "derived"}},
			Op:     "ocr",
			Params: json.RawMessage(`{"lang":"ru","tool":"marker"}`),
			Repro:  true,
		}))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The core half: the edge reached PutOptions, where accounting sees it.
	if got := inner.lastOpts.ParentRefs; len(got) != 1 || got[0] != src {
		t.Fatalf("edges did not reach the core: %v", got)
	}

	// The extension half: the meaning is in Ext, aligned with the edges.
	b, ok, err := Decode(inner.lastExt)
	if err != nil || !ok {
		t.Fatalf("Decode stamped ext: ok=%v err=%v", ok, err)
	}
	if len(b.Rel) != 1 || b.Rel[0] != "derived" {
		t.Errorf("relation kinds = %v", b.Rel)
	}
	if b.Op != "ocr" || !b.Repro || b.Outcome != OutcomeOK {
		t.Errorf("record fields wrong: %+v", b)
	}
	if b.PKey == "" {
		t.Error("pkey was not derived")
	}
}

// Params spelled differently must produce the same idempotency key, otherwise
// "have we already done this" would depend on formatting.
func TestPKey_IndependentOfSpelling(t *testing.T) {
	a, err := ParamsKey("ocr", json.RawMessage(`{"lang":"ru","tool":"marker"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParamsKey("ocr", json.RawMessage("{\n  \"tool\" : \"marker\",\n  \"lang\":\"ru\"\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("key depends on spelling: %s vs %s", a, b)
	}

	// A different operation with identical params is different work.
	c, err := ParamsKey("thumbnail", json.RawMessage(`{"lang":"ru","tool":"marker"}`))
	if err != nil {
		t.Fatal(err)
	}
	if c == a {
		t.Fatal("different operations collided")
	}

	// Numeric literals are preserved: 1 and 1.0 are not the same parameters.
	i, _ := ParamsKey("op", json.RawMessage(`{"n":1}`))
	fl, _ := ParamsKey("op", json.RawMessage(`{"n":1.0}`))
	if i == fl {
		t.Error("1 and 1.0 hashed alike")
	}
}

// An origin has no production record; such a Put must pass through untouched,
// because "came from outside" is meaningful, not missing.
func TestPut_NoRecordPassesThrough(t *testing.T) {
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{}, nil, inner)

	if _, err := s.Put(context.Background(), domain.Artifact{Ext: json.RawMessage(`{"nsid":"ns-1"}`)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok, _ := Decode(inner.lastExt); ok {
		t.Fatal("a Put without a record was stamped")
	}
	if string(inner.lastExt) != `{"nsid":"ns-1"}` {
		t.Errorf("Ext was modified: %s", inner.lastExt)
	}
}

// The stamp must not evict another extension's block from Ext.
func TestPut_StampPreservesOtherSchemas(t *testing.T) {
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{}, nil, inner)

	_, err := s.Put(context.Background(),
		domain.Artifact{Ext: json.RawMessage(`{"vfsmeta":{"path":"/a/b"},"nsid":"ns-1"}`)},
		WithProduction(Production{
			Inputs: []Input{{Ref: ref("a"), Rel: "derived"}},
			Op:     "extract",
		}))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(inner.lastExt, &obj); err != nil {
		t.Fatalf("stamped ext undecodable: %v", err)
	}
	for _, k := range []string{"vfsmeta", "nsid", Key} {
		if _, ok := obj[k]; !ok {
			t.Errorf("key %q missing after stamp: %s", k, inner.lastExt)
		}
	}
}

// --- Put: rejection, not repair ---

func TestPut_RejectsMalformedRecords(t *testing.T) {
	cases := []struct {
		name string
		p    Production
	}{
		{"input without a relation kind", Production{
			Inputs: []Input{{Ref: ref("a")}}, Op: "ocr",
		}},
		{"inputs but no operation", Production{
			Inputs: []Input{{Ref: ref("a"), Rel: "derived"}},
		}},
		{"same input twice", Production{
			Inputs: []Input{{Ref: ref("a"), Rel: "derived"}, {Ref: ref("a"), Rel: "derived"}},
			Op:     "assemble",
		}},
		{"empty input ref", Production{
			Inputs: []Input{{Ref: "", Rel: "derived"}}, Op: "ocr",
		}},
		{"params that are not JSON", Production{
			Inputs: []Input{{Ref: ref("a"), Rel: "derived"}},
			Op:     "ocr",
			Params: json.RawMessage(`{not json`),
		}},
		{"negative sequence", Production{
			Inputs: []Input{{Ref: ref("a"), Rel: "derived"}}, Op: "split", Seq: -1,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeDataStore{}
			s := wrapStore(t, Config{}, nil, inner)
			_, err := s.Put(context.Background(), domain.Artifact{}, WithProduction(tc.p))
			if !errors.Is(err, ErrBadProduction) {
				t.Fatalf("want ErrBadProduction, got %v", err)
			}
			if inner.lastExt != nil {
				t.Error("a rejected record still reached the inner store")
			}
		})
	}
}

// A judgement carries inputs and no payload of its own; a failed attempt is a
// record with an outcome instead of a result. Both must be writable.
func TestPut_JudgementAndFailureAreOrdinaryRecords(t *testing.T) {
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{}, nil, inner)
	ctx := context.Background()

	if _, err := s.Put(ctx, domain.Artifact{}, WithProduction(Production{
		Inputs: []Input{{Ref: ref("a"), Rel: DefaultSupersedeRel}},
		Op:     "merge-duplicates",
	})); err != nil {
		t.Fatalf("judgement: %v", err)
	}
	if b, _, _ := Decode(inner.lastExt); b.Rel[0] != DefaultSupersedeRel {
		t.Errorf("supersede judgement not recorded: %+v", b)
	}

	if _, err := s.Put(ctx, domain.Artifact{}, WithProduction(Production{
		Inputs:  []Input{{Ref: ref("b"), Rel: "derived"}},
		Op:      "ocr",
		Repro:   true,
		Outcome: OutcomeFailed,
	})); err != nil {
		t.Fatalf("failure record: %v", err)
	}
	if b, _, _ := Decode(inner.lastExt); b.Outcome != OutcomeFailed {
		t.Errorf("failure outcome lost: %+v", b)
	}
}

// --- Delete guard ---

func TestDelete_GuardRefusesSourceWithDerivatives(t *testing.T) {
	src := domain.ArtifactID("source")
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{GuardDeletes: true},
		fakeLookup{children: map[domain.ArtifactID]bool{src: true}}, inner)

	if err := s.Delete(context.Background(), src); !errors.Is(err, ErrHasDerivatives) {
		t.Fatalf("want ErrHasDerivatives, got %v", err)
	}
	if len(inner.deleted) != 0 {
		t.Error("delete reached the inner store despite the guard")
	}

	// A leaf deletes normally.
	if err := s.Delete(context.Background(), "leaf"); err != nil {
		t.Fatalf("leaf delete: %v", err)
	}
	if len(inner.deleted) != 1 || inner.deleted[0] != "leaf" {
		t.Errorf("inner deletes = %v", inner.deleted)
	}
}

// Without the guard configured the wrapper stamps but does not protect — the
// state of a deployment that installs the wrapper and not the index. It must
// be explicit, not accidental.
func TestDelete_WithoutGuardDelegates(t *testing.T) {
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{}, nil, inner)
	if err := s.Delete(context.Background(), "source"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(inner.deleted) != 1 {
		t.Error("delete did not reach the inner store")
	}
}

func TestWrap_GuardWithoutIndexIsRefused(t *testing.T) {
	if _, err := (factory{cfg: Config{GuardDeletes: true}}).Wrap(&fakeDataStore{}, wrapper.Deps{}); err == nil {
		t.Fatal("guard without an index should not assemble")
	}
}

// --- Decode: absence and unknown versions ---

func TestDecode_AbsenceIsNotAnError(t *testing.T) {
	for _, ext := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`{"vfsmeta":{}}`)} {
		if _, ok, err := Decode(ext); ok || err != nil {
			t.Errorf("Decode(%s): ok=%v err=%v", ext, ok, err)
		}
	}
	if _, _, err := Decode(json.RawMessage(`["not","an","object"]`)); err == nil {
		t.Error("a non-object Ext should be an error")
	}
	// A future version is skipped, not guessed at.
	if _, ok, err := Decode(json.RawMessage(`{"provenance":{"v":99,"op":"x"}}`)); ok || err != nil {
		t.Errorf("future version: ok=%v err=%v", ok, err)
	}
}

// The core's sentinels stay the core's: the wrapper must not shadow them.
func TestErrors_AreDistinctFromCore(t *testing.T) {
	for _, err := range []error{ErrBadProduction, ErrHasDerivatives, ErrForked} {
		if errors.Is(err, errs.ErrInvalidHandleRef) || errors.Is(err, errs.ErrTooManyRefs) {
			t.Errorf("%v collides with a core sentinel", err)
		}
	}
}
