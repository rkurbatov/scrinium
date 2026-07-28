package view_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"scrinium.dev/projection/internal/source"
	vw "scrinium.dev/projection/internal/view"
	"scrinium.dev/testutil/manifestfx"
	"scrinium.dev/testutil/projectionfx"

	"scrinium.dev/domain"
	"scrinium.dev/errs"
)

// makeManifest is a thin wrapper around manifestfx.Blob that
// overrides the fields a typical projection test cares about.
// Local to this file because the override pattern is small and
// every other call-site has its own preferences.
func makeManifest(id string, sid domain.SessionID, size int64, createdAt time.Time) domain.Manifest {
	m := manifestfx.Blob(id, "sha256-"+repeatChar('b', 64))
	m.SessionID = sid
	m.OriginalSize = size
	m.CreatedAt = createdAt
	return m
}

// repeatChar is a tiny helper because strings.Repeat is overkill
// for a single use.
func repeatChar(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

// makeShortID creates a fake but unique ArtifactID for tests
// that need many distinct IDs without caring about content.
func makeShortID(i int) string {
	const hex = "0123456789abcdef"
	const algo = "sha256-"
	b := make([]byte, 64)
	for j := range b {
		b[j] = hex[(i+j)%16]
	}
	return algo + string(b)
}

// --- NewView ---

func TestNewView_Empty(t *testing.T) {
	src := projectionfx.New()
	v, err := vw.New(context.Background(), src)
	if err != nil {
		t.Fatalf("NewView: %v", err)
	}
	defer v.Close()
	if v.Source != source.KindStore {
		t.Errorf("Source: got %q, want store", v.Source)
	}
	if v.Stats.TotalNodes != 0 {
		t.Errorf("TotalNodes: got %d, want 0", v.Stats.TotalNodes)
	}
}

func TestNewView_NilSource(t *testing.T) {
	_, err := vw.New(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil source")
	}
}

func TestNewView_SourceWalkError(t *testing.T) {
	src := projectionfx.New()
	src.SetWalkErr(errors.New("source kaboom"))
	_, err := vw.New(context.Background(), src)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, errs.ErrSourceUnavailable) {
		t.Errorf("expected ErrSourceUnavailable, got %v", err)
	}
}

// --- Backfill ---

func TestNewView_PopulatesByArtifact(t *testing.T) {
	src := projectionfx.New()
	now := time.Now().UTC()
	src.Add(makeManifest("sha256-aabbccdd", "sess1", 100, now), []byte("hello"))
	src.Add(makeManifest("sha256-eeffgghh", "sess1", 200, now), []byte("world"))

	v, err := vw.New(context.Background(), src)
	if err != nil {
		t.Fatalf("NewView: %v", err)
	}
	defer v.Close()

	if got := v.Stats.TotalNodes; got != 2 {
		t.Errorf("TotalNodes: got %d, want 2", got)
	}
	if got := v.Stats.TotalBytes; got != 300 {
		t.Errorf("TotalBytes: got %d, want 300", got)
	}
}

// --- Get ---

func TestGetByArtifact_File(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "sess1", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	node, err := v.GetIn(vw.RootByArtifact, "aa/bb/sha256-aabbccdd")
	if err != nil {
		t.Fatalf("GetByArtifact: %v", err)
	}
	if node.FS.IsDir {
		t.Errorf("expected file, got dir")
	}
	if node.Artifact == nil {
		t.Fatal("expected Artifact to be populated")
	}
	if node.Artifact.ArtifactID != domain.ArtifactID("sha256-aabbccdd") {
		t.Errorf("ArtifactID: got %q", node.Artifact.ArtifactID)
	}
}

func TestGetByArtifact_VirtualDirectory(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "sess1", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	node, err := v.GetIn(vw.RootByArtifact, "aa")
	if err != nil {
		t.Fatalf("GetByArtifact: %v", err)
	}
	if !node.FS.IsDir {
		t.Errorf("expected dir, got file")
	}
	if node.Artifact != nil {
		t.Errorf("virtual dir should not have Artifact")
	}
}

func TestGetByArtifact_Root(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "sess1", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	node, err := v.GetIn(vw.RootByArtifact, "")
	if err != nil {
		t.Fatalf("GetByArtifact root: %v", err)
	}
	if !node.FS.IsDir {
		t.Errorf("root should be dir")
	}
}

func TestGetByArtifact_NotFound(t *testing.T) {
	src := projectionfx.New()
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	_, err := v.GetIn(vw.RootByArtifact, "0e0e0e0e")
	if !errors.Is(err, errs.ErrPathNotFound) {
		t.Errorf("expected ErrPathNotFound, got %v", err)
	}
}

func TestGetByArtifact_Closed(t *testing.T) {
	src := projectionfx.New()
	v, _ := vw.New(context.Background(), src)
	v.Close()

	_, err := v.GetIn(vw.RootByArtifact, "")
	if !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}
}

// --- List ---

func TestListByArtifact_RootListsShards(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	src.Add(makeManifest("sha256-ccddeeff", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	var names []string
	for n, err := range v.ListIn(vw.RootByArtifact, "") {
		if err != nil {
			t.Fatalf("iter error: %v", err)
		}
		names = append(names, n.FS.Name)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 children at root, got %d (%v)", len(names), names)
	}
	if names[0] != "aa" || names[1] != "cc" {
		t.Errorf("expected [aa, cc] sorted, got %v", names)
	}
}

func TestListByArtifact_FileReturnsErrNotADirectory(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	for _, err := range v.ListIn(vw.RootByArtifact, "aa/bb/sha256-aabbccdd") {
		if !errors.Is(err, errs.ErrNotADirectory) {
			t.Errorf("expected ErrNotADirectory, got %v", err)
		}
		return
	}
	t.Fatal("expected at least one yield with error")
}

func TestListByArtifact_NonexistentReturnsErrPathNotFound(t *testing.T) {
	src := projectionfx.New()
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	for _, err := range v.ListIn(vw.RootByArtifact, "nope/path") {
		if !errors.Is(err, errs.ErrPathNotFound) {
			t.Errorf("expected ErrPathNotFound, got %v", err)
		}
		return
	}
	t.Fatal("expected at least one yield with error")
}

// --- Walk ---

func TestWalkByArtifact_AllNodes(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	src.Add(makeManifest("sha256-aaccddee", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	var paths []string
	for n, err := range v.WalkIn(vw.RootByArtifact, "") {
		if err != nil {
			t.Fatalf("iter error: %v", err)
		}
		paths = append(paths, n.FS.Path)
	}
	// Expected: root "", "aa", "aa/bb", "aa/bb/sha256-aabbccdd",
	// "aa/cc", "aa/cc/sha256-aaccddee".
	if len(paths) != 6 {
		t.Errorf("expected 6 paths, got %d: %v", len(paths), paths)
	}
}

func TestWalkByArtifact_StopEarly(t *testing.T) {
	src := projectionfx.New()
	for i := 0; i < 10; i++ {
		id := makeShortID(i)
		src.Add(makeManifest(id, "s", 100, time.Now().UTC()), nil)
	}
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	count := 0
	for _, err := range v.WalkIn(vw.RootByArtifact, "") {
		if err != nil {
			t.Fatalf("iter error: %v", err)
		}
		count++
		if count >= 3 {
			break
		}
	}
	if count != 3 {
		t.Errorf("expected exactly 3, got %d", count)
	}
}

// --- Open ---

func TestOpenByArtifact_File(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 5, time.Now().UTC()), []byte("hello"))
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	rh, err := v.OpenIn(context.Background(), vw.RootByArtifact, "aa/bb/sha256-aabbccdd")
	if err != nil {
		t.Fatalf("OpenByArtifact: %v", err)
	}
	defer rh.Close()

	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want hello", got)
	}
}

func TestOpenByArtifact_Directory(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	_, err := v.OpenIn(context.Background(), vw.RootByArtifact, "aa")
	if !errors.Is(err, errs.ErrIsADirectory) {
		t.Errorf("expected ErrIsADirectory, got %v", err)
	}
}

func TestOpenByArtifact_NotFound(t *testing.T) {
	src := projectionfx.New()
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	_, err := v.OpenIn(context.Background(), vw.RootByArtifact, "nope")
	if !errors.Is(err, errs.ErrPathNotFound) {
		t.Errorf("expected ErrPathNotFound, got %v", err)
	}
}

func TestOpenByArtifact_SourceArtifactNotFound(t *testing.T) {
	// View has the path, but source returns ErrArtifactNotFound —
	// race with concurrent deletion. Mapping yields
	// ErrPathNotFound on the projection side.
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	src.SetGetErr(errs.ErrArtifactNotFound)

	_, err := v.OpenIn(context.Background(), vw.RootByArtifact, "aa/bb/sha256-aabbccdd")
	if !errors.Is(err, errs.ErrPathNotFound) {
		t.Errorf("expected ErrPathNotFound, got %v", err)
	}
}

func TestOpenByArtifact_SourceLocked(t *testing.T) {
	src := projectionfx.New()
	src.Add(makeManifest("sha256-aabbccdd", "s", 100, time.Now().UTC()), nil)
	v, _ := vw.New(context.Background(), src)
	defer v.Close()

	src.SetGetErr(errs.ErrLocked)

	_, err := v.OpenIn(context.Background(), vw.RootByArtifact, "aa/bb/sha256-aabbccdd")
	if !errors.Is(err, errs.ErrArtifactUnreadable) {
		t.Errorf("expected ErrArtifactUnreadable, got %v", err)
	}
	if !errors.Is(err, errs.ErrLocked) {
		t.Errorf("expected ErrLocked to be wrapped; got %v", err)
	}
}

// testRoot is the root of the neutral fake provided view the projection
// tests use to exercise the generic provided-view rail (ADR-98). It is
// deliberately NOT by-path or by-namespace — those are extension views,
// tested with their extensions (x/fspath, x/namespace). The projection
// attaches no meaning to it.
const testRoot vw.RootView = "by-test"

// testProvided is a neutral collidable, orphaning provided view standing
// in for "some extension's view". Its path is a logical path the test
// stamps into the manifest Ext under "_p"; a manifest with no stamp misses
// (orphaned, or synthetic under FallbackSynthetic). The projection never
// knows what computes it.
func testProvided() vw.ProvidedView {
	return vw.ProvidedView{
		Root:    testRoot,
		Path:    testPath,
		Collide: true,
		Orphans: true,
	}
}

func testPath(m domain.Manifest) (string, bool) {
	if len(m.Ext) == 0 {
		return "", false
	}
	var x struct {
		Path string `json:"_p"`
	}
	if err := json.Unmarshal(m.Ext, &x); err != nil || x.Path == "" {
		return "", false
	}
	return x.Path, true
}

// testManifest builds a neutral blob manifest carrying a logical path in
// Ext under "_p" (empty path ⇒ no stamp, so the view misses it). The other
// fields a test cares about (namespace, session, time) are set by the
// caller afterwards.
func testManifest(id, path string, createdAt time.Time) domain.Manifest {
	m := manifestfx.Blob(id, "sha256-"+repeatChar('b', 64))
	m.CreatedAt = createdAt
	if path != "" {
		m.Ext, _ = json.Marshal(map[string]string{"_p": path})
	}
	return m
}

// pathManifest is testManifest at the current time, for tests that do not
// care about CreatedAt.
func pathManifest(id, path string) domain.Manifest {
	return testManifest(id, path, time.Now().UTC())
}
