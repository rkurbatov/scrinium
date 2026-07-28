package fspath_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/index/sqlite"
	fsExt "scrinium.dev/x/fspath"
)

// fspathindex is populated through the Indexer capability (Index/Unindex),
// which the core runs inside the index-write and delete transactions — NOT
// through the Subscribe/Apply event path (09 §9.2, §9.13). These tests drive
// the public surface end-to-end via the sqlite index (IndexManifest dispatches
// Index; DeleteManifest dispatches Unindex) and directly exercise the Accessor
// family (KeyLookup/PrefixScan) and the ViewProvider seam.

// --- helpers ---

// newRegisteredFSPathIndex returns an in-memory sqlite Index plus a
// freshly-registered fspathindex. Cleanup closes both.
func newRegisteredFSPathIndex(t *testing.T) (*sqlite.Index, *fsExt.CustomIndex) {
	t.Helper()
	idx, err := sqlite.NewStore(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	ci := fsExt.NewIndex()
	if err := idx.CustomIndexes().Register(context.Background(), ci); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return idx, ci
}

// makeFSManifest returns a Manifest with vfsmeta-shaped Ext at the given
// path. The filesystem schema lives in the ext pocket (09 §9.13; the
// ingester writes vfsmeta into Ext, see examples/ingest).
func makeFSManifest(t *testing.T, id domain.ArtifactID, path string) domain.Manifest {
	t.Helper()
	raw, err := vfsmeta.Embed(nil, vfsmeta.FileSystem{
		Path: path,
		Mode: 0o644,
	})
	if err != nil {
		t.Fatalf("vfsmeta.Embed: %v", err)
	}
	return domain.Manifest{
		ArtifactID:   id,
		Digest:       domain.ManifestDigest(fxKey(id)),
		BlobRefs:     []domain.BlobRef{domain.BlobRef(fxKey(id))},
		ContentHash:  domain.ContentHash(fxKey(id)),
		OriginalSize: 100,
		CreatedAt:    time.Now().UTC(),
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
		Ext:          raw,
	}
}

// makeForeignManifest returns a Manifest whose metadata does NOT use the
// vfsmeta schema. fspathindex must skip it.
func makeForeignManifest(t *testing.T, id domain.ArtifactID) domain.Manifest {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"email": map[string]string{"subject": "hi"}})
	return domain.Manifest{
		ArtifactID:   id,
		Digest:       domain.ManifestDigest(fxKey(id)),
		BlobRefs:     []domain.BlobRef{domain.BlobRef(fxKey(id))},
		ContentHash:  domain.ContentHash(fxKey(id)),
		OriginalSize: 50,
		CreatedAt:    time.Now().UTC(),
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
		Ext:          raw,
	}
}

func physAddr() domain.PhysicalAddress {
	return domain.PhysicalAddress{
		Path: "/blobs/x",
	}
}

// --- registration ---

func TestRegister_Succeeds(t *testing.T) {
	idx, ext := newRegisteredFSPathIndex(t)
	if ext == nil {
		t.Fatal("nil custom index")
	}
	if idx == nil {
		t.Fatal("nil index")
	}
}

func TestRegister_DoubleRegister_Rejects(t *testing.T) {
	idx, _ := newRegisteredFSPathIndex(t)
	err := idx.CustomIndexes().Register(context.Background(), fsExt.NewIndex())
	if err == nil {
		t.Error("expected error on second register, got nil")
	}
}

// --- Index (write-side capability) ---

func TestIndex_VFSMetadata_Stored(t *testing.T) {
	ctx := t.Context()
	idx, ext := newRegisteredFSPathIndex(t)

	m := makeFSManifest(t, "art-1", "photos/2024/sunset.jpg")
	if err := idx.IndexManifest(ctx, m, physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	raw, ok, err := ext.GetByID("art-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if !ok {
		t.Fatal("GetByID: ok=false; expected to find indexed manifest")
	}
	fs, ok, err := vfsmeta.Decode(raw)
	if err != nil || !ok {
		t.Fatalf("Decode persisted bytes: ok=%v err=%v", ok, err)
	}
	if fs.Path != "photos/2024/sunset.jpg" {
		t.Errorf("Path = %q, want photos/2024/sunset.jpg", fs.Path)
	}
}

func TestIndex_ForeignSchema_Skipped(t *testing.T) {
	ctx := t.Context()
	idx, ext := newRegisteredFSPathIndex(t)

	m := makeForeignManifest(t, "email-1")
	if err := idx.IndexManifest(ctx, m, physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	_, ok, err := ext.GetByID("email-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if ok {
		t.Error("foreign-schema artifact was indexed by fspathindex; expected skip")
	}
}

func TestIndex_NoMetadata_Skipped(t *testing.T) {
	ctx := t.Context()
	idx, ext := newRegisteredFSPathIndex(t)

	m := domain.Manifest{
		ArtifactID:   "bare-1",
		Digest:       domain.ManifestDigest(fxKey("bare-1")),
		BlobRefs:     []domain.BlobRef{"ba4e0000"},
		ContentHash:  "ba4e0000",
		OriginalSize: 10,
		CreatedAt:    time.Now().UTC(),
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
		// Ext is nil
	}
	if err := idx.IndexManifest(ctx, m, physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	_, ok, _ := ext.GetByID("bare-1")
	if ok {
		t.Error("manifest with nil Ext indexed; expected skip")
	}
}

// Index errors propagate out of the write transaction and roll the whole
// IndexManifest back (strict consistency, ADR-49).
func TestIndex_BrokenVFSMeta_RollsBackMainWrite(t *testing.T) {
	ctx := t.Context()
	idx, _ := newRegisteredFSPathIndex(t)

	// Right key, wrong type for Path: Decode returns an error.
	bad := json.RawMessage(`{"vfsmeta":{"version":1,"path":12345}}`)
	m := domain.Manifest{
		ArtifactID:   "art-bad",
		Digest:       domain.ManifestDigest(fxKey("art-bad")),
		BlobRefs:     []domain.BlobRef{"bad00000"},
		ContentHash:  "bad00000",
		OriginalSize: 10,
		CreatedAt:    time.Now().UTC(),
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
		Ext:          bad,
	}
	err := idx.IndexManifest(ctx, m, physAddr())
	if err == nil {
		t.Fatal("expected error from broken fsmeta, got nil")
	}

	_, exists, err := idx.ResolveManifestDigest(ctx, "art-bad")
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if exists {
		t.Error("manifest committed despite fspathindex failure (atomicity broken)")
	}
}

// --- Unindex (delete-side capability) ---

func TestUnindex_RemovesEntries(t *testing.T) {
	ctx := t.Context()
	idx, ext := newRegisteredFSPathIndex(t)

	m := makeFSManifest(t, "art-del", "tmp/file.txt")
	if err := idx.IndexManifest(ctx, m, physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	if _, ok, _ := ext.GetByID("art-del"); !ok {
		t.Fatal("pre-delete: not indexed")
	}

	// DeleteManifest dispatches Unindex inside the delete transaction.
	if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}

	if _, ok, _ := ext.GetByID("art-del"); ok {
		t.Error("post-delete: still in byID")
	}
	if _, ok, _ := ext.LookupByPath("tmp/file.txt"); ok {
		t.Error("post-delete: still findable by path")
	}
}

func TestUnindex_NotIndexed_NoOp(t *testing.T) {
	ctx := t.Context()
	idx, _ := newRegisteredFSPathIndex(t)

	// A non-vfsmeta manifest is never indexed; deleting it must be a clean
	// no-op (Unindex finds no own byID entry).
	m := makeForeignManifest(t, "email-2")
	if err := idx.IndexManifest(ctx, m, physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
		t.Errorf("DeleteManifest of un-indexed artifact failed: %v", err)
	}
}

// --- KeyLookup (read-side: exact path) ---

func TestLookup_Single(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	if err := idx.IndexManifest(ctx, makeFSManifest(t, "a-1", "photos/sunset.jpg"), physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	ids, err := ci.Lookup("photos/sunset.jpg")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(ids) != 1 || ids[0] != "a-1" {
		t.Fatalf("Lookup = %v, want [a-1]", ids)
	}
}

func TestLookup_Miss(t *testing.T) {
	_, ci := newRegisteredFSPathIndex(t)
	ids, err := ci.Lookup("nonexistent/path.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("Lookup miss returned %v, want empty", ids)
	}
}

// KeyLookup is many-to-many: two artifacts may briefly share a path during
// reindex, and both must come back.
func TestLookup_ManyAtSamePath(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	for _, id := range []domain.ArtifactID{"dup-1", "dup-2"} {
		if err := idx.IndexManifest(ctx, makeFSManifest(t, id, "shared/path"), physAddr()); err != nil {
			t.Fatalf("IndexManifest %q: %v", id, err)
		}
	}
	ids, err := ci.Lookup("shared/path")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("Lookup = %v, want 2 ids at the shared path", ids)
	}
}

// The \x00 pin makes the match exact: "a/b" must not match a sibling "a/bc"
// that shares it only as a byte-prefix.
func TestLookup_ExactNotBytePrefix(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	if err := idx.IndexManifest(ctx, makeFSManifest(t, "bc", "a/bc"), physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	ids, err := ci.Lookup("a/b")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf(`Lookup("a/b") matched %v; must not match sibling "a/bc"`, ids)
	}
}

// --- PrefixScan (read-side: subtree / directory listing) ---

func TestScanPrefix_Subtree(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	paths := map[domain.ArtifactID]string{
		"p1": "photos/2024/a.jpg",
		"p2": "photos/2024/b.jpg",
		"p3": "photos/2025/c.jpg",
		"d1": "docs/readme.md", // outside the prefix
	}
	for id, p := range paths {
		if err := idx.IndexManifest(ctx, makeFSManifest(t, id, p), physAddr()); err != nil {
			t.Fatalf("IndexManifest %q: %v", id, err)
		}
	}

	got := map[string]int{}
	err := ci.ScanPrefix("photos/", func(k customindex.Key, ids []domain.ArtifactID) error {
		got[string(k)] = len(ids)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ScanPrefix yielded %d paths, want 3 (photos/* only): %v", len(got), got)
	}
	if _, leaked := got["docs/readme.md"]; leaked {
		t.Error("ScanPrefix leaked a path outside the prefix")
	}
}

// Two artifacts at one path arrive in a SINGLE callback with both ids
// (the scan coalesces same-path entries on the group boundary).
func TestScanPrefix_CoalescesSamePath(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	for _, id := range []domain.ArtifactID{"x1", "x2"} {
		if err := idx.IndexManifest(ctx, makeFSManifest(t, id, "dir/file"), physAddr()); err != nil {
			t.Fatalf("IndexManifest %q: %v", id, err)
		}
	}
	calls, batch := 0, 0
	err := ci.ScanPrefix("dir/", func(_ customindex.Key, ids []domain.ArtifactID) error {
		calls++
		batch = len(ids)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	if calls != 1 || batch != 2 {
		t.Errorf("ScanPrefix calls=%d batch=%d, want 1 call carrying 2 ids", calls, batch)
	}
}

func TestScanPrefix_EmptyIndex(t *testing.T) {
	_, ci := newRegisteredFSPathIndex(t)
	calls := 0
	err := ci.ScanPrefix("", func(customindex.Key, []domain.ArtifactID) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	if calls != 0 {
		t.Errorf("ScanPrefix on empty index made %d callbacks, want 0", calls)
	}
}

// --- WalkAll (host-facing bulk read) ---

func TestWalkAll_VisitsAll(t *testing.T) {
	ctx := t.Context()
	idx, ext := newRegisteredFSPathIndex(t)

	pairs := map[domain.ArtifactID]string{
		"a-1": "alpha/file1",
		"a-2": "alpha/file2",
		"b-1": "beta/file1",
	}
	for id, path := range pairs {
		if err := idx.IndexManifest(ctx, makeFSManifest(t, id, path), physAddr()); err != nil {
			t.Fatalf("IndexManifest %q: %v", id, err)
		}
	}

	visited := map[domain.ArtifactID]string{}
	err := ext.WalkAll(func(id domain.ArtifactID, raw json.RawMessage) error {
		fs, _, err := vfsmeta.Decode(raw)
		if err != nil {
			return err
		}
		visited[id] = fs.Path
		return nil
	})
	if err != nil {
		t.Fatalf("WalkAll: %v", err)
	}
	if len(visited) != 3 {
		t.Errorf("WalkAll visited %d, want 3: %v", len(visited), visited)
	}
	for id, want := range pairs {
		if got := visited[id]; got != want {
			t.Errorf("WalkAll: %q got %q, want %q", id, got, want)
		}
	}
}

// --- ViewProvider (ADR-98) ---

func TestProvidedViews_Shape(t *testing.T) {
	views := fsExt.NewIndex().ProvidedViews()
	if len(views) != 1 {
		t.Fatalf("ProvidedViews: got %d, want 1", len(views))
	}
	pv := views[0]
	if pv.Root != "by-path" {
		t.Errorf("Root = %q, want by-path", pv.Root)
	}
	if pv.Path == nil {
		t.Error("Path is nil; the by-path view needs a placement function")
	}
	if pv.Metadata == nil {
		t.Error("Metadata is nil; backfill loses its bulk ext source")
	}
	if !pv.Collide {
		t.Error("by-path must be collidable (logical paths are not artifact-unique)")
	}
	if !pv.Orphans {
		t.Error("by-path must orphan manifests it cannot place")
	}
}

func TestProvidedViews_Path(t *testing.T) {
	pv := fsExt.NewIndex().ProvidedViews()[0]

	path, ok := pv.Path(makeFSManifest(t, "a-1", "photos/sunset.jpg"))
	if !ok || path != "photos/sunset.jpg" {
		t.Errorf("Path = (%q,%v), want (photos/sunset.jpg,true)", path, ok)
	}
	if _, ok := pv.Path(makeForeignManifest(t, "e-1")); ok {
		t.Error("Path admitted a foreign-schema manifest; want ok=false")
	}
}

func TestProvidedViews_MetadataDelegatesToIndex(t *testing.T) {
	ctx := t.Context()
	idx, ci := newRegisteredFSPathIndex(t)
	if err := idx.IndexManifest(ctx, makeFSManifest(t, "a-1", "docs/readme.md"), physAddr()); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	// The view's Metadata source must surface exactly what the index
	// persisted — the backfill relies on it.
	pv := ci.ProvidedViews()[0]
	raw, ok, err := pv.Metadata.Metadata("a-1")
	if err != nil {
		t.Fatalf("pv.Metadata.Metadata: %v", err)
	}
	if !ok {
		t.Fatal("pv.Metadata.Metadata: ok=false for an indexed artifact")
	}
	fs, ok, err := vfsmeta.Decode(raw)
	if err != nil || !ok {
		t.Fatalf("Decode metadata bytes: ok=%v err=%v", ok, err)
	}
	if fs.Path != "docs/readme.md" {
		t.Errorf("metadata path = %q, want docs/readme.md", fs.Path)
	}
}

// --- Capability assertions (run-time mirror of the compile-time _ vars) ---

func TestCapabilities_Assertable(t *testing.T) {
	var ci customindex.CustomIndex = fsExt.NewIndex()
	if _, ok := ci.(customindex.Indexer); !ok {
		t.Error("fspath no longer satisfies customindex.Indexer")
	}
	if _, ok := ci.(customindex.KeyLookup); !ok {
		t.Error("fspath no longer satisfies customindex.KeyLookup")
	}
	if _, ok := ci.(customindex.PrefixScan); !ok {
		t.Error("fspath no longer satisfies customindex.PrefixScan")
	}
	if _, ok := ci.(customindex.ViewProvider); !ok {
		t.Error("fspath no longer satisfies customindex.ViewProvider")
	}
}

// --- Read API on an un-registered index ---

func TestReadAPI_NotRegistered_Errs(t *testing.T) {
	ci := fsExt.NewIndex()
	if _, _, err := ci.GetByID("anything"); err == nil {
		t.Error("GetByID on un-registered index returned nil; want error")
	}
	if _, _, err := ci.LookupByPath("anything"); err == nil {
		t.Error("LookupByPath on un-registered index returned nil; want error")
	}
	if _, err := ci.Lookup("anything"); err == nil {
		t.Error("Lookup on un-registered index returned nil; want error")
	}
	if err := ci.ScanPrefix("", func(customindex.Key, []domain.ArtifactID) error { return nil }); err == nil {
		t.Error("ScanPrefix on un-registered index returned nil; want error")
	}
	if err := ci.WalkAll(func(domain.ArtifactID, json.RawMessage) error { return nil }); err == nil {
		t.Error("WalkAll on un-registered index returned nil; want error")
	}
}

// fxKey derives a bare-hex key from a fixture id (ADR-93: refs and digests
// carry no algorithm prefix, and the index stores them as raw bytes).
func fxKey(id domain.ArtifactID) string { return hex.EncodeToString([]byte(id)) }
