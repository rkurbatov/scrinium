package extractor_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver/localfs"
	"scrinium.dev/engine/index/sqlite"
	"scrinium.dev/engine/store"
	"scrinium.dev/event"
	"scrinium.dev/extension"
	"scrinium.dev/projection"
	"scrinium.dev/testutil/storefx"
	"scrinium.dev/x/fspath"
	"scrinium.dev/x/fspath/extractor"
	"scrinium.dev/x/fspath/ingester"
)

// The round trip is the acceptance criterion of ADR-114 and of the
// Ingester↔Extractor pair (ADR-115): capture a tree with empty directories and
// meaningful modes, lay it back out, compare. Nothing else proves that both
// halves agree about what a captured tree is.

type roundTrip struct {
	source string
	target string
	ing    ingester.Ingester
	ext    *extractor.Extractor
	store  store.Store
}

// buildExtractor builds the projection AFTER the capture: the view materialises
// what the store holds, so it has to be built once there is something to see.
func (r *roundTrip) buildExtractor(t *testing.T, paths *fspath.CustomIndex) {
	t.Helper()
	// Every field of the contribution is forwarded. A field-by-field mapping in
	// a test is exactly where a newly added one goes missing silently — the
	// directory declaration (ADR-116) did, and the round trip then restored an
	// empty directory as a file.
	provided := paths.ProvidedViews()
	views := make([]projection.ProvidedView, 0, len(provided))
	for _, p := range provided {
		views = append(views, projection.ProvidedView{
			Root:     p.Root,
			Path:     p.Path,
			Collide:  p.Collide,
			Orphans:  p.Orphans,
			CountKey: p.CountKey,
			IsDir:    p.IsDir,
		})
	}
	proj, err := projection.Build(context.Background(), r.store, paths, projection.Config{
		RootView:      "by-path",
		ProvidedViews: views,
	})
	if err != nil {
		t.Fatalf("projection.Build: %v", err)
	}
	t.Cleanup(func() { _ = proj.Close() })

	ext, err := extractor.New(proj.View, nil)
	if err != nil {
		t.Fatalf("extractor.New: %v", err)
	}
	r.ext = ext
}

// keepOnFail makes a directory that is removed only when the test passes. A
// round-trip failure IS a pair of directory trees; t.TempDir would delete the
// evidence before anyone could look at it, because its cleanup runs regardless
// of the verdict.
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

func writeAged(t *testing.T, root, rel, body string, mode os.FileMode) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(full, mode); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(full, old, old); err != nil {
		t.Fatal(err)
	}
}

func mkdirAged(t *testing.T, root, rel string, mode os.FileMode) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(full, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(full, mode); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(full, old, old); err != nil {
		t.Fatal(err)
	}
}

// tree walks a real directory and returns a comparable description of it.
type entry struct {
	path  string
	isDir bool
	mode  os.FileMode
	body  string
}

func describe(t *testing.T, root string) []entry {
	t.Helper()
	var out []entry
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		e := entry{path: filepath.ToSlash(rel), isDir: d.IsDir(), mode: info.Mode().Perm()}
		if !d.IsDir() {
			body, berr := os.ReadFile(p)
			if berr != nil {
				return berr
			}
			e.body = string(body)
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		t.Fatalf("describe %q: %v", root, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

func TestRoundTrip_TreeSurvivesCaptureAndRestore(t *testing.T) {
	var paths *fspath.CustomIndex
	r := newRoundTrip(t, &paths)

	// A tree with the things a naive capture loses: an empty directory, a
	// directory with a non-default mode, and a file that is not 0644.
	writeAged(t, r.source, "books/one.txt", "first", 0o600)
	writeAged(t, r.source, "books/deep/two.txt", "second", 0o644)
	mkdirAged(t, r.source, "books/empty", 0o750)

	if _, err := r.ing.RunNow(t.Context()); err != nil {
		t.Fatalf("capture: %v", err)
	}
	r.buildExtractor(t, paths)

	stats, err := r.ext.Extract(t.Context(), extractor.Config{Root: r.target})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if stats.Files == 0 || stats.Dirs == 0 {
		t.Fatalf("restore did nothing useful: %+v", stats)
	}

	want := describe(t, r.source)
	got := describe(t, r.target)
	if len(want) != len(got) {
		t.Fatalf("tree differs in size:\nsource %+v\ntarget %+v", want, got)
	}
	for i := range want {
		if want[i].path != got[i].path || want[i].isDir != got[i].isDir {
			t.Fatalf("entry %d differs: source %+v target %+v", i, want[i], got[i])
		}
		if want[i].body != got[i].body {
			t.Errorf("%q content: source %q target %q", want[i].path, want[i].body, got[i].body)
		}
		if want[i].mode != got[i].mode {
			t.Errorf("%q mode: source %#o target %#o", want[i].path, want[i].mode, got[i].mode)
		}
	}

	// The empty directory is the point: it carries no file, so only a captured
	// directory artifact can bring it back.
	if info, err := os.Stat(filepath.Join(r.target, "books/empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty directory not restored: %v", err)
	}
}

// A captured path is data and may say "..": containment is enforced.
func TestExtract_RefusesToEscapeTheRoot(t *testing.T) {
	var paths *fspath.CustomIndex
	r := newRoundTrip(t, &paths)
	e, err := extractor.New(escapingTree{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Extract(t.Context(), extractor.Config{Root: r.target}); err == nil ||
		!strings.Contains(err.Error(), "escapes") {
		t.Fatalf("want an escape refusal, got %v", err)
	}
}

// The default collision policy never touches a stranger's file.
func TestExtract_FailsOnOccupiedPath(t *testing.T) {
	var paths *fspath.CustomIndex
	r := newRoundTrip(t, &paths)
	writeAged(t, r.source, "doc.txt", "captured", 0o644)
	if _, err := r.ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}
	r.buildExtractor(t, paths)

	// Someone else's file is already there.
	if err := os.WriteFile(filepath.Join(r.target, "doc.txt"), []byte("not ours"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := r.ext.Extract(t.Context(), extractor.Config{Root: r.target})
	if err == nil {
		t.Fatal("restore overwrote an occupied path under the default policy")
	}
	body, _ := os.ReadFile(filepath.Join(r.target, "doc.txt"))
	if string(body) != "not ours" {
		t.Errorf("the existing file was modified: %q", body)
	}

	// Skip leaves it alone and reports it.
	stats, err := r.ext.Extract(t.Context(), extractor.Config{Root: r.target, Collision: extractor.Skip})
	if err != nil {
		t.Fatalf("Skip policy: %v", err)
	}
	if stats.Skipped == 0 {
		t.Errorf("skipped nothing: %+v", stats)
	}
}

// A directory the tree derived from paths, with no captured metadata, gets the
// configured default mode rather than a guess from its children.
func TestExtract_SyntheticDirectoryGetsTheDefaultMode(t *testing.T) {
	var paths *fspath.CustomIndex
	r := newRoundTrip(t, &paths)
	// Capture WITHOUT directories: the tree will synthesise "sub".
	ing, err := ingester.New(mustDriver(t, r.source), r.store, paths,
		mustScoped(t, r.store), event.NewEventBus(), ingester.Config{})
	if err != nil {
		t.Fatal(err)
	}
	writeAged(t, r.source, "sub/file.txt", "x", 0o644)
	if _, err := ing.RunNow(t.Context()); err != nil {
		t.Fatal(err)
	}
	r.buildExtractor(t, paths)

	if _, err := r.ext.Extract(t.Context(), extractor.Config{
		Root: r.target, DirMode: 0o700,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	info, err := os.Stat(filepath.Join(r.target, "sub"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("synthetic directory mode = %#o, want 0700", info.Mode().Perm())
	}
}

// --- helpers ---

// newRoundTrip builds source and target directories, a store whose index hosts
// the path extension, and an ingester over the source. The registered index is
// handed back, because the projection must be built over the SAME instance the
// store writes into.
func newRoundTrip(t *testing.T, out **fspath.CustomIndex) *roundTrip {
	t.Helper()
	ctx := context.Background()

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

	source := keepOnFail(t, "source")
	ing, err := ingester.New(mustDriver(t, source), st, paths, mustScoped(t, st),
		event.NewEventBus(), ingester.Config{CaptureDirs: true})
	if err != nil {
		t.Fatalf("ingester.New: %v", err)
	}
	*out = paths
	return &roundTrip{source: source, target: keepOnFail(t, "target"), ing: ing, store: st}
}

func mustDriver(t *testing.T, root string) *localfs.Driver {
	t.Helper()
	d, err := localfs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustScoped(t *testing.T, st store.Store) *extension.ScopedSystemStore {
	t.Helper()
	s, err := extension.NewScopedSystemStore("ingester", st.System())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// escapingTree is a view whose single node claims a path outside the root.
type escapingTree struct{}

func (escapingTree) WalkIn(_ projection.RootView, _ string) projection.Seq {
	return func(yield func(projection.Node, error) bool) {
		yield(projection.Node{FS: projection.FilesystemFacet{Path: "../outside.txt"}}, nil)
	}
}

func (escapingTree) OpenIn(context.Context, projection.RootView, string, ...domain.GetOption) (domain.ReadHandle, error) {
	return nil, os.ErrNotExist
}
