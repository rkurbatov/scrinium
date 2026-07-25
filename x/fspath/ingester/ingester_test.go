package ingester_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/driver/localfs"
	"scrinium.dev/engine/index/sqlite"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
	"scrinium.dev/extension"
	"scrinium.dev/testutil/storefx"
	"scrinium.dev/x/fspath"
	"scrinium.dev/x/fspath/ingester"
)

// The ingester is driven end to end: a real localfs source, a real store over a
// real sqlite index with the filesystem-path extension registered, and the
// extension's scoped system store holding the watermark. Nothing here is faked,
// because every interesting property of this agent — the already-captured probe,
// the watermark, directory capture — only exists against real ones.

type harness struct {
	ing    ingester.Ingester
	source string // the directory being ingested from
	store  store.Store
	paths  *fspath.CustomIndex

	// deps kept so a test can build another ingester over the same store —
	// New's argument checks fire before its mode and capability checks, so
	// exercising those needs real dependencies rather than nils.
	src    driver.Driver
	cursor *extension.ScopedSystemStore
	bus    event.Publisher
}

func newHarness(t *testing.T, cfg ingester.Config) *harness {
	t.Helper()
	ctx := context.Background()

	// A FILE-backed index, not ":memory:". A plain in-memory sqlite database is
	// private to each connection, and the index pool opens up to eight: the
	// migrations run on one connection, and a later read can land on another
	// that has no tables at all ("no such table: ext_data"). A file is what a
	// real store uses anyway, and it makes the substrate reads deterministic.
	idx, err := sqlite.NewStore(ctx, filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatalf("sqlite.NewStore: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	paths := fspath.NewIndex()
	if err := idx.CustomIndexes().Register(ctx, paths); err != nil {
		t.Fatalf("register fspath: %v", err)
	}

	st := storefx.Init(t, store.WithStoreIndex(idx))

	scoped, err := extension.NewScopedSystemStore("ingester", st.System())
	if err != nil {
		t.Fatalf("scoped system store: %v", err)
	}

	source := keepOnFail(t, "source")
	src, err := localfs.New(source)
	if err != nil {
		t.Fatalf("source driver: %v", err)
	}

	ing, err := ingester.New(src, st, paths, scoped, event.NewEventBus(), cfg)
	if err != nil {
		t.Fatalf("ingester.New: %v", err)
	}
	return &harness{
		ing: ing, source: source, store: st, paths: paths,
		src: src, cursor: scoped, bus: event.NewEventBus(),
	}
}

// build constructs another ingester over the same store and source, so a test
// can assert on New's own refusals.
func (h *harness) build(cfg ingester.Config) (ingester.Ingester, error) {
	return ingester.New(h.src, h.store, h.paths, h.cursor, h.bus, cfg)
}

// keepOnFail makes a directory that is removed only when the test passes: the
// source tree is the evidence of a capture failure, and t.TempDir would delete
// it regardless of the verdict.
func keepOnFail(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "scrinium-"+name+"-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("%s tree kept for inspection: %s", name, dir)
			return
		}
		_ = os.RemoveAll(dir)
	})
	return dir
}

// writeAged writes a file and backdates it, so the settle window (which is
// about wall-clock freshness) does not defer it.
func writeAged(t *testing.T, root, rel, body string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(full, old, old); err != nil {
		t.Fatal(err)
	}
}

func mkdirAged(t *testing.T, root, rel string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(full, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(full, old, old); err != nil {
		t.Fatal(err)
	}
}

// captured returns the vfsmeta of everything captured, keyed by path.
//
// It reads through the filesystem-path index rather than Store.Walk: a Walk row
// is built from index columns only and does not hydrate Ext, so the pocket is
// not visible there. The index is where the path lives anyway — and reading it
// this way also proves the extension indexed what the agent wrote.
func captured(t *testing.T, paths *fspath.CustomIndex) map[string]vfsmeta.FileSystem {
	t.Helper()
	out := map[string]vfsmeta.FileSystem{}
	err := paths.ScanPrefix("", func(key customindex.Key, ids []domain.ArtifactID) error {
		if len(ids) == 0 {
			return nil
		}
		raw, ok, err := paths.GetByID(ids[0])
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		fs, ok, err := vfsmeta.Decode(raw)
		if err != nil || !ok {
			return nil
		}
		out[fs.Path] = fs
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	return out
}

// --- one sweep, then idempotence ---

func TestSweep_CapturesFilesThenStopsRepeating(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	writeAged(t, h.source, "a.txt", "one")
	writeAged(t, h.source, "sub/b.txt", "two")

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	if stats.Captured != 2 {
		t.Fatalf("captured = %d, want 2 (%+v)", stats.Captured, stats)
	}
	got := captured(t, h.paths)
	if _, ok := got["a.txt"]; !ok {
		t.Errorf("a.txt not captured: %v", got)
	}
	if _, ok := got["sub/b.txt"]; !ok {
		t.Errorf("sub/b.txt not captured: %v", got)
	}

	// The second sweep must write nothing: identity is unique per Put, so
	// without the already-captured probe this is where manifests would double.
	stats, err = h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatalf("second RunNow: %v", err)
	}
	if stats.Captured != 0 {
		t.Fatalf("a re-sweep captured %d artifacts again (%+v)", stats.Captured, stats)
	}
	if n := len(captured(t, h.paths)); n != 2 {
		t.Fatalf("store holds %d path-carrying artifacts, want 2", n)
	}
}

// A new file appears: found on the next sweep even though the watermark has
// already moved past the older ones.
func TestSweep_PicksUpNewFileAfterWatermark(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	writeAged(t, h.source, "first.txt", "1")
	if _, err := h.ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}

	writeAged(t, h.source, "second.txt", "2")
	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Captured != 1 {
		t.Fatalf("captured = %d, want the one new file (%+v)", stats.Captured, stats)
	}
}

// The same path with a newer modification time is a NEW appearance, not a
// duplicate: WORM keeps both, and which one is current is not the ingester's
// call.
func TestSweep_ChangedFileIsANewAppearance(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	writeAged(t, h.source, "doc.txt", "v1")
	if _, err := h.ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Rewrite with a different (but still aged) timestamp.
	writeAged(t, h.source, "doc.txt", "v2 longer")
	newer := time.Now().Add(-30 * time.Minute)
	if err := os.Chtimes(filepath.Join(h.source, "doc.txt"), newer, newer); err != nil {
		t.Fatal(err)
	}

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Captured != 1 {
		t.Fatalf("captured = %d, want a second appearance (%+v)", stats.Captured, stats)
	}
	ids, err := h.paths.Lookup("doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("path holds %d artifacts, want both appearances", len(ids))
	}
}

// --- settle window ---

// A file written this very second is postponed, not captured and not failed:
// reading it could catch half of it.
func TestSweep_FreshFileIsDeferred(t *testing.T) {
	h := newHarness(t, ingester.Config{SettleWindow: time.Hour})
	writeAged(t, h.source, "aged.txt", "old enough")
	full := filepath.Join(h.source, "fresh.txt")
	if err := os.WriteFile(full, []byte("just now"), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Deferred != 1 {
		t.Fatalf("deferred = %d, want the fresh file (%+v)", stats.Deferred, stats)
	}
	if _, ok := captured(t, h.paths)["fresh.txt"]; ok {
		t.Error("a file inside the settle window was captured")
	}
	// And the aged one is not held back with it.
	if _, ok := captured(t, h.paths)["aged.txt"]; !ok {
		t.Error("an aged file was deferred along with the fresh one")
	}
}

// A deferred element must stay reachable: the watermark may not advance past it,
// or the next sweep would never see it again.
func TestSweep_DeferredElementSurvivesTheWatermark(t *testing.T) {
	h := newHarness(t, ingester.Config{SettleWindow: 50 * time.Millisecond})
	full := filepath.Join(h.source, "settling.txt")
	if err := os.WriteFile(full, []byte("in flight"), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Deferred != 1 || stats.Captured != 0 {
		t.Fatalf("first sweep: %+v", stats)
	}

	time.Sleep(100 * time.Millisecond)
	stats, err = h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Captured != 1 {
		t.Fatalf("a settled element was lost behind the watermark: %+v", stats)
	}
}

// --- directories ---

func TestSweep_CapturesDirectoriesIncludingEmpty(t *testing.T) {
	h := newHarness(t, ingester.Config{CaptureDirs: true})
	writeAged(t, h.source, "books/one.txt", "x")
	mkdirAged(t, h.source, "books/empty")

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	if stats.Failed != 0 {
		t.Fatalf("elements failed during a directory sweep: %+v", stats)
	}
	got := captured(t, h.paths)

	// The empty directory is the point: no file path leads to it.
	empty, ok := got["books/empty"]
	if !ok {
		t.Fatalf("empty directory not captured: %v (stats %+v)", got, stats)
	}
	if empty.Mode&syscall.S_IFDIR == 0 {
		t.Errorf("directory artifact carries no type bits: mode %#o", empty.Mode)
	}
	if _, ok := got["books"]; !ok {
		t.Errorf("parent directory not captured: %v", got)
	}
	if _, ok := got["books/one.txt"]; !ok {
		t.Errorf("file not captured alongside directories: %v", got)
	}
}

// Directory capture over a driver that reports objects only is refused at
// construction: dropping directories silently would yield a tree that looks
// complete and is not.
func TestNew_CaptureDirsNeedsTheCapability(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	h.src = objectOnlyDriver{} // declares no CapDirEntries

	_, err := h.build(ingester.Config{CaptureDirs: true})
	if err == nil {
		t.Fatal("CaptureDirs accepted over a driver without CapDirEntries")
	}
	if !strings.Contains(err.Error(), "CapDirEntries") {
		t.Errorf("refusal does not name the missing capability: %v", err)
	}
}

func TestNew_WatchModeIsNotImplemented(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	if _, err := h.build(ingester.Config{Mode: ingester.IngestModeWatch}); !errors.Is(err, errs.ErrNotImplemented) {
		t.Fatalf("want ErrNotImplemented, got %v", err)
	}
}

// objectOnlyDriver declares no directory entries — the shape of an object store.
// Only Capabilities is reached before New refuses.
type objectOnlyDriver struct{ driver.Driver }

func (objectOnlyDriver) Capabilities() driver.CapabilityMask { return 0 }

// --- policy ---

// The policy decides and enriches; the agent supplies only the path block, and
// the two must coexist in one Ext object.
func TestSweep_PolicyDecidesAndEnriches(t *testing.T) {
	h := newHarness(t, ingester.Config{Policy: booksOnly{}})
	writeAged(t, h.source, "keep.txt", "wanted")
	writeAged(t, h.source, "skip.tmp", "junk")

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Captured != 1 || stats.Skipped != 1 {
		t.Fatalf("stats = %+v", stats)
	}

	// Both schemas survive in the pocket. The manifest is fetched by id, because
	// a Walk row carries no Ext.
	ids, err := h.paths.Lookup("keep.txt")
	if err != nil || len(ids) != 1 {
		t.Fatalf("Lookup: %v (%v)", ids, err)
	}
	rh, err := h.store.Get(t.Context(), ids[0])
	if err != nil {
		t.Fatal(err)
	}
	ext := rh.Manifest().Ext
	_ = rh.Close()

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(ext, &obj); err != nil {
		t.Fatalf("ext is not an object: %s", ext)
	}
	if _, ok := obj[vfsmeta.Key]; !ok {
		t.Errorf("agent's own block missing: %s", ext)
	}
	if _, ok := obj["librarium"]; !ok {
		t.Errorf("policy's block missing: %s", ext)
	}
}

type booksOnly struct{}

func (booksOnly) Decide(el ingester.Element) ingester.Decision {
	if filepath.Ext(el.Path) == ".tmp" {
		return ingester.Skip
	}
	return ingester.Take
}

func (booksOnly) Enrich(ingester.Element) (json.RawMessage, json.RawMessage, []domain.PutOption, error) {
	return json.RawMessage(`{"librarium":{"role":"origin"}}`), nil, nil, nil
}

// --- disposal ---

func TestSweep_RemoveDisposalDeletesSourceAfterCapture(t *testing.T) {
	h := newHarness(t, ingester.Config{Disposal: ingester.Remove})
	writeAged(t, h.source, "inbox/one.txt", "content")

	if _, err := h.ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := captured(t, h.paths)["inbox/one.txt"]; !ok {
		t.Fatal("element was not captured")
	}
	if _, err := os.Stat(filepath.Join(h.source, "inbox/one.txt")); !os.IsNotExist(err) {
		t.Errorf("source element still present after Remove disposal: %v", err)
	}
}

// Keep is the default: nothing in the source is touched.
func TestSweep_KeepDisposalLeavesSourceAlone(t *testing.T) {
	h := newHarness(t, ingester.Config{})
	writeAged(t, h.source, "keep.txt", "content")
	if _, err := h.ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(h.source, "keep.txt")); err != nil {
		t.Errorf("source element disturbed under the default disposal: %v", err)
	}
}

// --- zero-length artifacts (ADR-114 acceptance) ---

// A directory artifact has no content, and so does an empty file. Both must be
// writable; content addressing means every empty artifact shares one blob.
func TestSweep_ZeroLengthArtifacts(t *testing.T) {
	h := newHarness(t, ingester.Config{CaptureDirs: true})
	writeAged(t, h.source, "empty.txt", "")
	mkdirAged(t, h.source, "hollow")

	stats, err := h.ing.RunNow(t.Context())
	if err != nil {
		t.Fatalf("zero-length capture failed: %v", err)
	}
	// A per-element failure is only logged, so assert it explicitly: a
	// zero-length artifact that cannot be written would otherwise hide here.
	if stats.Failed != 0 {
		t.Fatalf("zero-length elements failed to write: %+v", stats)
	}
	got := captured(t, h.paths)
	for _, p := range []string{"empty.txt", "hollow"} {
		if _, ok := got[p]; !ok {
			t.Errorf("%q not captured: %v", p, got)
		}
	}
}
