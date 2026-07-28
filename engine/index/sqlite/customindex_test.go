package sqlite

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
)

// --- Test helpers ---

// fakeCustomIndex is a minimal CustomIndex. Per-test instances capture
// the events they observe so the test can assert dispatch.
type fakeCustomIndex struct {
	name        string
	version     int
	subscribe   []customindex.EventKind
	setupCalls  int
	setupOldVer int
	setupErr    error
	applyCalls  []customindex.EventArgs
	applyKinds  []customindex.EventKind
	applyErr    error
	closeCalls  int

	captured customindex.Substrate

	// applyFn, if set, runs inside Apply (a backend transaction). Used to
	// exercise transactional operations like Inc from their real call site.
	applyFn func(store customindex.Substrate) error
}

func (e *fakeCustomIndex) Name() string                       { return e.name }
func (e *fakeCustomIndex) SchemaVersion() int                 { return e.version }
func (e *fakeCustomIndex) Subscribe() []customindex.EventKind { return e.subscribe }
func (e *fakeCustomIndex) Setup(ctx context.Context, store customindex.Substrate, oldVersion int) error {
	e.setupCalls++
	e.setupOldVer = oldVersion
	e.captured = store
	return e.setupErr
}
func (e *fakeCustomIndex) Apply(ctx context.Context, store customindex.Substrate, kind customindex.EventKind, args customindex.EventArgs) error {
	e.applyKinds = append(e.applyKinds, kind)
	e.applyCalls = append(e.applyCalls, args)
	if e.applyErr != nil {
		return e.applyErr
	}
	if e.applyFn != nil {
		if err := e.applyFn(store); err != nil {
			return err
		}
	}
	// For ManifestIndexed, write something into the store so
	// dispatch atomicity can be verified.
	if kind == customindex.EventKindManifestIndexed {
		_ = store.Put("trace", string(args.ArtifactID), []byte("seen"))
	}
	return nil
}
func (e *fakeCustomIndex) Close() error {
	e.closeCalls++
	return nil
}

// --- prefixUpperBound ---

func TestPrefixUpperBound(t *testing.T) {
	cases := []struct {
		in       string
		want     string
		hasUpper bool
	}{
		{"foo", "fop", true},
		{"a", "b", true},
		{"\xFE", "\xFF", true},
		{"f\xFF", "g", true},
		{"foo\xFF\xFF", "fop", true},
		{"\xFF", "", false},
		{"\xFF\xFF\xFF", "", false},
	}
	for _, tc := range cases {
		got, has := prefixUpperBound(tc.in)
		if got != tc.want || has != tc.hasUpper {
			t.Errorf("prefixUpperBound(%q) = (%q, %v), want (%q, %v)",
				tc.in, got, has, tc.want, tc.hasUpper)
		}
	}
}

// --- Schema migration ---

func TestSchemaV2_TablesPresent(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	for _, table := range []string{"ext_meta", "ext_data"} {
		var n int
		err := idx.db.QueryRow(
			`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`, table,
		).Scan(&n)
		if err != nil {
			t.Errorf("table %q missing: %v", table, err)
		}
	}
}

// --- Register basics ---

func TestRegister_FirstTime_OldVersionZero(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.alpha", version: 1}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if ext.setupCalls != 1 {
		t.Errorf("Setup called %d times, want 1", ext.setupCalls)
	}
	if ext.setupOldVer != 0 {
		t.Errorf("oldVersion = %d, want 0", ext.setupOldVer)
	}
}

func TestRegister_NilCustomIndex(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()
	if err := idx.CustomIndexes().Register(context.Background(), nil); err == nil {
		t.Error("expected error for nil custom index")
	}
}

func TestRegister_EmptyName(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()
	ext := &fakeCustomIndex{name: "", version: 1}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err == nil {
		t.Error("expected error for empty name")
	}
}

func TestRegister_DuplicateName(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	a := &fakeCustomIndex{name: "test.dupe", version: 1}
	b := &fakeCustomIndex{name: "test.dupe", version: 1}
	if err := idx.CustomIndexes().Register(context.Background(), a); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := idx.CustomIndexes().Register(context.Background(), b)
	if !errors.Is(err, customindex.ErrAlreadyRegistered) {
		t.Errorf("expected ErrAlreadyRegistered, got %v", err)
	}
}

func TestRegister_SchemaRegression(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/index.db"

	// Phase 1: register a custom index at v5.
	idx1, err := NewStore(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("NewStore phase 1: %v", err)
	}
	ext1 := &fakeCustomIndex{name: "test.regression", version: 5}
	if err := idx1.CustomIndexes().Register(context.Background(), ext1); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	idx1.Close()

	// Phase 2: re-open, register the same name at v3 — must
	// reject with ErrSchemaRegression.
	idx2, err := NewStore(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("NewStore phase 2: %v", err)
	}
	defer idx2.Close()
	ext2 := &fakeCustomIndex{name: "test.regression", version: 3}
	err = idx2.CustomIndexes().Register(context.Background(), ext2)
	if !errors.Is(err, customindex.ErrSchemaRegression) {
		t.Errorf("expected ErrSchemaRegression, got %v", err)
	}
}

func TestRegister_SetupFailure_RollbackEverything(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	failure := errors.New("setup boom")
	ext := &fakeCustomIndex{name: "test.fail", version: 1, setupErr: failure}
	err := idx.CustomIndexes().Register(context.Background(), ext)
	if !errors.Is(err, failure) {
		t.Errorf("expected setup error, got %v", err)
	}
	// ext_meta must NOT contain a row for the failed customindex.
	var v int
	scanErr := idx.db.QueryRow(
		`SELECT schema_version FROM ext_meta WHERE extension = ?`,
		"test.fail",
	).Scan(&v)
	if scanErr == nil {
		t.Errorf("ext_meta row exists despite Setup failure (version %d)", v)
	}
}

func TestRegister_SchemaVersionPersisted(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.persist", version: 7}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var v int
	err := idx.db.QueryRow(
		`SELECT schema_version FROM ext_meta WHERE extension = ?`,
		"test.persist",
	).Scan(&v)
	if err != nil {
		t.Fatalf("query ext_meta: %v", err)
	}
	if v != 7 {
		t.Errorf("persisted version = %d, want 7", v)
	}
}

// --- Substrate ops ---

func TestExtStore_PutGetDelete(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.kv", version: 1}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err != nil {
		t.Fatalf("Register: %v", err)
	}
	store := ext.captured

	if err := store.Put("t1", "key1", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := store.Get("t1", "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(v) != "hello" {
		t.Errorf("Get got (%q, %v), want hello/true", v, ok)
	}

	if err := store.Delete("t1", "key1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ = store.Get("t1", "key1")
	if ok {
		t.Error("Get after Delete returned ok=true")
	}
}

func TestExtStore_Get_Absent(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.absent", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)
	_, ok, err := ext.captured.Get("t1", "nope")
	if err != nil {
		t.Errorf("Get on absent key returned error: %v", err)
	}
	if ok {
		t.Error("Get on absent key returned ok=true")
	}
}

func TestExtStore_Scan_Prefix(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.scan", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)
	store := ext.captured

	pairs := map[string]string{
		"alpha:1": "v1",
		"alpha:2": "v2",
		"alpha:3": "v3",
		"beta:1":  "vB",
	}
	for k, v := range pairs {
		store.Put("scan", k, []byte(v))
	}

	got := map[string]string{}
	err := store.Scan("scan", "alpha:", func(k string, v []byte) error {
		got[k] = string(v)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("Scan got %d keys, want 3: %v", len(got), got)
	}
	if got["alpha:1"] != "v1" || got["alpha:2"] != "v2" || got["alpha:3"] != "v3" {
		t.Errorf("Scan content mismatch: %v", got)
	}
}

func TestExtStore_Scan_StopScan(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.stop", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)
	store := ext.captured

	for i := 0; i < 5; i++ {
		store.Put("scan", string(rune('a'+i)), []byte("v"))
	}

	count := 0
	err := store.Scan("scan", "", func(k string, v []byte) error {
		count++
		if count == 2 {
			return customindex.ErrStopScan
		}
		return nil
	})
	if err != nil {
		t.Errorf("Scan returned error on ErrStopScan: %v", err)
	}
	if count != 2 {
		t.Errorf("Scan returned %d entries, want 2", count)
	}
}

func TestExtStore_DeletePrefix(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.delpref", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)
	store := ext.captured

	store.Put("t", "alpha:1", []byte("v"))
	store.Put("t", "alpha:2", []byte("v"))
	store.Put("t", "beta:1", []byte("v"))

	if err := store.DeletePrefix("t", "alpha:"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	_, ok, _ := store.Get("t", "alpha:1")
	if ok {
		t.Error("alpha:1 still present after DeletePrefix")
	}
	_, ok, _ = store.Get("t", "beta:1")
	if !ok {
		t.Error("beta:1 deleted by alpha: prefix")
	}
}

func TestExtStore_DeletePrefix_EmptyRejected(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.empty", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)

	err := ext.captured.DeletePrefix("t", "")
	if !errors.Is(err, customindex.ErrEmptyPrefix) {
		t.Errorf("expected ErrEmptyPrefix, got %v", err)
	}
}

func TestExtStore_Inc(t *testing.T) {
	ctx := t.Context()
	idx := openMemIndex(t)
	defer idx.Close()

	// Inc is transactional (atomic only inside a backend tx), so drive it from
	// Apply — its real call site — through the IndexManifest dispatch fixture.
	var got []int64
	ext := &fakeCustomIndex{
		name:      "test.inc",
		version:   1,
		subscribe: []customindex.EventKind{customindex.EventKindManifestIndexed},
		applyFn: func(store customindex.Substrate) error {
			for _, d := range []int64{5, 3, -10} {
				v, err := store.Inc("counters", "k1", d)
				if err != nil {
					return err
				}
				got = append(got, v)
			}
			return nil
		},
	}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err != nil {
		t.Fatalf("Register: %v", err)
	}

	m := makeBlobManifest("art-inc")
	addr := domain.PhysicalAddress{Path: "/blobs/inc"}
	if err := idx.IndexManifest(ctx, m, addr); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	if len(got) != 3 || got[0] != 5 || got[1] != 8 || got[2] != -2 {
		t.Errorf("Inc results = %v, want [5 8 -2]", got)
	}
}

// TestExtStore_Inc_OutsideApply asserts Inc is refused on the read-side
// substrate (after Register, no transaction): it cannot be atomic there.
func TestExtStore_Inc_OutsideApply(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{name: "test.inc.ro", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)

	_, err := ext.captured.Inc("counters", "k1", 1)
	if !errors.Is(err, customindex.ErrIncOutsideApply) {
		t.Fatalf("read-side Inc: got %v, want ErrIncOutsideApply", err)
	}
}

// --- Namespace isolation ---

func TestExtStore_NamespaceIsolation(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	a := &fakeCustomIndex{name: "test.A", version: 1}
	b := &fakeCustomIndex{name: "test.B", version: 1}
	idx.CustomIndexes().Register(context.Background(), a)
	idx.CustomIndexes().Register(context.Background(), b)

	a.captured.Put("shared", "key", []byte("from-A"))
	b.captured.Put("shared", "key", []byte("from-B"))

	v, _, _ := a.captured.Get("shared", "key")
	if string(v) != "from-A" {
		t.Errorf("ext A reads %q, want from-A", v)
	}
	v, _, _ = b.captured.Get("shared", "key")
	if string(v) != "from-B" {
		t.Errorf("ext B reads %q, want from-B", v)
	}
}

// --- Dispatch on IndexManifest ---

func TestDispatch_ManifestIndexed(t *testing.T) {
	ctx := t.Context()
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{
		name:      "test.dispatch",
		version:   1,
		subscribe: []customindex.EventKind{customindex.EventKindManifestIndexed},
	}
	if err := idx.CustomIndexes().Register(context.Background(), ext); err != nil {
		t.Fatalf("Register: %v", err)
	}

	m := makeBlobManifest("art-1")
	addr := domain.PhysicalAddress{Path: "/blobs/x"}
	if err := idx.IndexManifest(ctx, m, addr); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	if len(ext.applyCalls) != 1 {
		t.Fatalf("Apply called %d times, want 1", len(ext.applyCalls))
	}
	if ext.applyKinds[0] != customindex.EventKindManifestIndexed {
		t.Errorf("kind = %v, want ManifestIndexed", ext.applyKinds[0])
	}
	if ext.applyCalls[0].Manifest.ArtifactID != "art-1" {
		t.Errorf("Manifest.ArtifactID = %q, want art-1", ext.applyCalls[0].Manifest.ArtifactID)
	}

	// Check the trace write went through (Apply wrote into store).
	v, ok, err := ext.captured.Get("trace", "art-1")
	if err != nil {
		t.Fatalf("Get trace: %v", err)
	}
	if !ok || string(v) != "seen" {
		t.Errorf("trace write missing: ok=%v v=%q", ok, v)
	}
}

func TestDispatch_NotSubscribed_NoApply(t *testing.T) {
	ctx := t.Context()
	idx := openMemIndex(t)
	defer idx.Close()

	// Subscribed only to Deleted, but we'll fire Indexed.
	ext := &fakeCustomIndex{
		name:      "test.notsub",
		version:   1,
		subscribe: []customindex.EventKind{customindex.EventKindManifestDeleted},
	}
	idx.CustomIndexes().Register(context.Background(), ext)

	m := makeBlobManifest("art-2")
	idx.IndexManifest(ctx, m, domain.PhysicalAddress{Path: "/blobs/y"})

	if len(ext.applyCalls) != 0 {
		t.Errorf("non-subscribed custom index's Apply called %d times", len(ext.applyCalls))
	}
}

func TestDispatch_ApplyError_RollsBack(t *testing.T) {
	ctx := t.Context()
	idx := openMemIndex(t)
	defer idx.Close()

	failure := errors.New("apply boom")
	ext := &fakeCustomIndex{
		name:      "test.applyfail",
		version:   1,
		subscribe: []customindex.EventKind{customindex.EventKindManifestIndexed},
		applyErr:  failure,
	}
	idx.CustomIndexes().Register(context.Background(), ext)

	m := makeBlobManifest("art-rollback")
	addr := domain.PhysicalAddress{Path: "/blobs/z"}
	err := idx.IndexManifest(ctx, m, addr)
	if !errors.Is(err, failure) {
		t.Errorf("expected apply error to propagate, got %v", err)
	}

	// The main index write must have rolled back too — manifest
	// should NOT be in the manifests table.
	_, exists, err := idx.ResolveManifestDigest(ctx, "art-rollback")
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if exists {
		t.Error("manifest committed despite custom index apply failure")
	}
}

// --- Dispatch on DeleteManifest ---

func TestDispatch_ManifestDeleted(t *testing.T) {
	ctx := t.Context()
	idx := openMemIndex(t)
	defer idx.Close()

	ext := &fakeCustomIndex{
		name:      "test.del",
		version:   1,
		subscribe: []customindex.EventKind{customindex.EventKindManifestDeleted},
	}
	idx.CustomIndexes().Register(context.Background(), ext)

	// Insert a manifest, then delete.
	m := makeBlobManifest("art-del")
	addr := domain.PhysicalAddress{Path: "/blobs/d"}
	if err := idx.IndexManifest(ctx, m, addr); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}

	if len(ext.applyCalls) != 1 {
		t.Fatalf("Apply called %d times, want 1", len(ext.applyCalls))
	}
	if ext.applyKinds[0] != customindex.EventKindManifestDeleted {
		t.Errorf("kind = %v, want ManifestDeleted", ext.applyKinds[0])
	}
	if ext.applyCalls[0].ArtifactID != "art-del" {
		t.Errorf("ArtifactID = %q, want art-del", ext.applyCalls[0].ArtifactID)
	}
}

// --- Close cleanup ---

func TestClose_CallsCustomIndexClose(t *testing.T) {
	idx := openMemIndex(t)

	ext := &fakeCustomIndex{name: "test.close", version: 1}
	idx.CustomIndexes().Register(context.Background(), ext)

	if err := idx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if ext.closeCalls != 1 {
		t.Errorf("Close called %d times, want 1", ext.closeCalls)
	}
}

// --- helpers ---

func openMemIndex(t *testing.T) *Index {
	t.Helper()
	idx, err := NewStore(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("NewStore(:memory:): %v", err)
	}
	return idx
}

// --- ListCustomIndexes ---

// TestIndex_ListCustomIndexes verifies (*Index).ListCustomIndexes returns
// every registered custom index's name and SchemaVersion. Introduced
// after P0.6, when the method's return type was lifted from a
// sqlite-local Info to customindex.Info (the contract
// type backends share). Without a direct test, a regression in
// either the return shape or the slice population would surface
// only via the projection layer's stats renderer — too far from
// the source.
//
// The test registers a small set of custom indexes with distinct
// names and schemas, then asserts the listing reproduces all of
// them. Order is unspecified per the Lister contract;
// we verify by membership, not by index position.
func TestIndex_ListCustomIndexes(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	cis := []*fakeCustomIndex{
		{name: "scrinium.alpha", version: 1},
		{name: "scrinium.beta", version: 3},
		{name: "scrinium.gamma", version: 7},
	}
	for _, e := range cis {
		if err := idx.CustomIndexes().Register(context.Background(), e); err != nil {
			t.Fatalf("Register %q: %v", e.name, err)
		}
	}

	got := idx.ListCustomIndexes()
	if len(got) != len(cis) {
		t.Fatalf("ListCustomIndexes: got %d entries, want %d", len(got), len(cis))
	}

	// Build a name→version lookup for membership assertion. Order
	// is unspecified per the Lister contract.
	byName := make(map[string]int, len(got))
	for _, info := range got {
		byName[info.Name] = info.SchemaVersion
	}
	for _, want := range cis {
		gotVer, ok := byName[want.name]
		if !ok {
			t.Errorf("ListCustomIndexes: missing %q", want.name)
			continue
		}
		if gotVer != want.version {
			t.Errorf("ListCustomIndexes[%q]: SchemaVersion = %d, want %d",
				want.name, gotVer, want.version)
		}
	}
}

// TestIndex_ListCustomIndexes_Empty: with no custom indexes registered
// the listing must be a non-nil empty slice. Callers iterate
// with range; nil and empty are equivalent there but the
// Lister contract specifies non-nil to avoid surprising
// reflective consumers (e.g. JSON encoders that distinguish
// `null` from `[]`).
func TestIndex_ListCustomIndexes_Empty(t *testing.T) {
	idx := openMemIndex(t)
	defer idx.Close()

	got := idx.ListCustomIndexes()
	if got == nil {
		t.Error("ListCustomIndexes on fresh Index: returned nil, want empty non-nil slice")
	}
	if len(got) != 0 {
		t.Errorf("ListCustomIndexes on fresh Index: got %d entries, want 0", len(got))
	}
}

func makeBlobManifest(id domain.ArtifactID) domain.Manifest {
	return domain.Manifest{
		ArtifactID: id,
		// manifest_digest is the primary key and never empty in production;
		// derive one from the id so two fixtures never collide.
		Digest:       domain.ManifestDigest(hex.EncodeToString([]byte(id))),
		BlobRefs:     []domain.BlobRef{"aaaa"},
		ContentHash:  "aaaa",
		OriginalSize: 100,
		CreatedAt:    time.Now().UTC(),
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
	}
}
