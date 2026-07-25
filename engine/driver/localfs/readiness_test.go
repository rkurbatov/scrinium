package localfs_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/driver/localfs"
)

// The two things an ingester leans on (ADR-115). They are tested here rather
// than in the conformance suite because they are deliberately NOT part of the
// mandatory Driver contract: an object store has nothing to answer.

func newDriver(t *testing.T) (*localfs.Driver, string) {
	t.Helper()
	root := t.TempDir()
	d, err := localfs.New(root)
	if err != nil {
		t.Fatalf("localfs.New: %v", err)
	}
	return d, root
}

func write(t *testing.T, root, rel, body string) string {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return full
}

func TestCapabilities_DirEntriesDeclaredProbeNotInMask(t *testing.T) {
	d, _ := newDriver(t)

	// The mask carries what a configuration's validity depends on.
	if !d.Capabilities().Has(driver.CapDirEntries) {
		t.Error("a filesystem driver should declare CapDirEntries")
	}

	// The readiness probe is taken by type, not by bit: nothing plans around
	// it, so it earns no promise in the mask.
	if _, ok := interface{}(d).(driver.ReadinessProber); !ok {
		t.Error("a POSIX driver should implement ReadinessProber")
	}
	if _, ok := interface{}(d).(driver.TreeLister); !ok {
		t.Error("a driver declaring CapDirEntries must implement TreeLister")
	}
}

// --- ReadyToRead ---

func TestReadyToRead(t *testing.T) {
	d, root := newDriver(t)
	ctx := t.Context()

	write(t, root, "ready.txt", "complete content")
	if ok, err := d.ReadyToRead(ctx, "ready.txt"); err != nil || !ok {
		t.Fatalf("a finished file should be ready: (%v, %v)", ok, err)
	}

	// Absent: not ready, not an error — it may appear on the next pass.
	if ok, err := d.ReadyToRead(ctx, "missing.txt"); err != nil || ok {
		t.Fatalf("absent path: (%v, %v)", ok, err)
	}

	// A zero-length file is a legitimate file: emptiness is not evidence that
	// somebody is still writing it. Deferring it here would defer it forever —
	// freshness is the settle window's business, not the probe's.
	write(t, root, "empty.txt", "")
	if ok, err := d.ReadyToRead(ctx, "empty.txt"); err != nil || !ok {
		t.Fatalf("zero-length file reported not ready: (%v, %v)", ok, err)
	}

	// A directory has nothing to read; answering true would let a caller
	// open it as a file.
	if err := os.MkdirAll(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.ReadyToRead(ctx, "dir"); err != nil || ok {
		t.Fatalf("directory reported ready: (%v, %v)", ok, err)
	}
}

// A file held under an exclusive advisory lock is a file still being written:
// the probe must say "not yet" and must not block waiting for the writer.
func TestReadyToRead_LockedFileIsNotReady(t *testing.T) {
	d, root := newDriver(t)
	full := write(t, root, "busy.txt", "partial")

	fh, err := os.OpenFile(full, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	if err := syscall.Flock(int(fh.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Skipf("advisory locks unavailable on this filesystem: %v", err)
	}

	done := make(chan struct{})
	var ok bool
	var probeErr error
	go func() {
		ok, probeErr = d.ReadyToRead(t.Context(), "busy.txt")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReadyToRead blocked on a locked file")
	}
	if probeErr != nil {
		t.Fatalf("ReadyToRead: %v", probeErr)
	}
	if ok {
		t.Error("a locked file was reported ready")
	}

	// Released: ready again, and the probe left no lock of its own behind.
	_ = syscall.Flock(int(fh.Fd()), syscall.LOCK_UN)
	if ok, err := d.ReadyToRead(t.Context(), "busy.txt"); err != nil || !ok {
		t.Fatalf("after release: (%v, %v)", ok, err)
	}
}

// --- ListTree ---

func TestListTree_ReportsDirectoriesIncludingEmpty(t *testing.T) {
	d, root := newDriver(t)

	write(t, root, "a/b/file.txt", "x")
	if err := os.MkdirAll(filepath.Join(root, "a/empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	var files, dirs []string
	err := d.ListTree(t.Context(), "", time.Time{}, func(e driver.TreeEntry) error {
		if e.IsDir {
			dirs = append(dirs, e.Path)
			// The type bits travel in Mode, which is what the vfsmeta schema
			// carries for a directory artifact.
			if e.Mode&syscall.S_IFDIR == 0 {
				t.Errorf("directory %q carries no type bits: mode %#o", e.Path, e.Mode)
			}
		} else {
			files = append(files, e.Path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ListTree: %v", err)
	}
	sort.Strings(dirs)

	if len(files) != 1 || files[0] != "a/b/file.txt" {
		t.Errorf("files = %v", files)
	}
	// The empty directory is the whole point: no object path leads to it.
	want := []string{"a", "a/b", "a/empty"}
	if len(dirs) != len(want) {
		t.Fatalf("dirs = %v, want %v", dirs, want)
	}
	for i := range want {
		if dirs[i] != want[i] {
			t.Fatalf("dirs = %v, want %v", dirs, want)
		}
	}
}

// A parent must be reported before its children, so a consumer that captures
// or materialises in callback order never meets a child first.
func TestListTree_ParentBeforeChildren(t *testing.T) {
	d, root := newDriver(t)
	write(t, root, "x/y/z/deep.txt", "v")

	var seen []string
	if err := d.ListTree(t.Context(), "", time.Time{}, func(e driver.TreeEntry) error {
		seen = append(seen, e.Path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, p := range seen {
		pos[p] = i
	}
	for _, pair := range [][2]string{{"x", "x/y"}, {"x/y", "x/y/z"}, {"x/y/z", "x/y/z/deep.txt"}} {
		if pos[pair[0]] > pos[pair[1]] {
			t.Errorf("%q reported after %q: %v", pair[0], pair[1], seen)
		}
	}
	// The walk root is the boundary, not an entry.
	for _, p := range seen {
		if p == "." || p == "" {
			t.Errorf("the root was reported as an entry: %v", seen)
		}
	}
}

// The `since` filter applies to files only: a directory's own timestamp says
// nothing about whether anything below it changed, so pruning by it would hide
// new files under an old directory.
func TestListTree_SinceFiltersFilesNotDirectories(t *testing.T) {
	d, root := newDriver(t)
	write(t, root, "old/stale.txt", "old")
	past := time.Now().Add(-2 * time.Hour)
	for _, rel := range []string{"old", "old/stale.txt"} {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(rel)), past, past); err != nil {
			t.Fatal(err)
		}
	}
	write(t, root, "old/fresh.txt", "new")

	var files, dirs []string
	if err := d.ListTree(t.Context(), "", time.Now().Add(-time.Hour), func(e driver.TreeEntry) error {
		if e.IsDir {
			dirs = append(dirs, e.Path)
		} else {
			files = append(files, e.Path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != "old/fresh.txt" {
		t.Errorf("files = %v, want only the fresh one", files)
	}
	if len(dirs) != 1 || dirs[0] != "old" {
		t.Errorf("dirs = %v — an old directory must still be reported", dirs)
	}
}

func TestListTree_SkipAllStopsWithoutError(t *testing.T) {
	d, root := newDriver(t)
	for _, rel := range []string{"a.txt", "b.txt", "c.txt"} {
		write(t, root, rel, "x")
	}
	n := 0
	if err := d.ListTree(t.Context(), "", time.Time{}, func(driver.TreeEntry) error {
		n++
		return fs.SkipAll
	}); err != nil {
		t.Fatalf("SkipAll surfaced as an error: %v", err)
	}
	if n != 1 {
		t.Errorf("walk continued after SkipAll: %d entries", n)
	}
}

func TestListTree_MissingPrefixIsEmptyWalk(t *testing.T) {
	d, _ := newDriver(t)
	n := 0
	if err := d.ListTree(t.Context(), "nowhere", time.Time{}, func(driver.TreeEntry) error {
		n++
		return nil
	}); err != nil {
		t.Fatalf("missing prefix should be an empty walk: %v", err)
	}
	if n != 0 {
		t.Errorf("entries reported for a missing prefix: %d", n)
	}
}

// A callback error propagates unchanged — that is how a consumer aborts.
func TestListTree_PropagatesCallbackError(t *testing.T) {
	d, root := newDriver(t)
	write(t, root, "a.txt", "x")
	stop := errors.New("enough")
	if err := d.ListTree(t.Context(), "", time.Time{}, func(driver.TreeEntry) error {
		return stop
	}); !errors.Is(err, stop) {
		t.Fatalf("callback error swallowed: %v", err)
	}
}
