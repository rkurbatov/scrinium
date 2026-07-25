package materialize_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"scrinium.dev/engine/internal/materialize"
)

func body(s string) materialize.Fill {
	return func(w io.Writer) error {
		_, err := io.WriteString(w, s)
		return err
	}
}

func TestFile_WritesAtomicallyAndHashes(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "out.txt")

	sum, err := materialize.File(final, "", body("hello"))
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Errorf("content = %q", got)
	}
	// The reported hash is of what landed, so a later reuse can be verified
	// without the caller reading the file a second time.
	again, err := materialize.Hash(final)
	if err != nil {
		t.Fatal(err)
	}
	if sum != again {
		t.Errorf("hash mismatch: reported %s, on disk %s", sum, again)
	}

	// Nothing is left staging behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("staging file left behind: %s", e.Name())
		}
	}
}

// A failing fill must leave no file at the destination and no staging file
// either: a half-written artifact is worse than a missing one.
func TestFile_FailedFillLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "out.txt")
	boom := errors.New("source went away")

	_, err := materialize.File(final, "", func(w io.Writer) error {
		_, _ = io.WriteString(w, "partial")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want the fill error, got %v", err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Errorf("destination exists after a failed fill: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("directory not clean: %v", entries)
	}
}

// File replaces an existing destination: whether that is allowed is the
// caller's policy, not this primitive's.
func TestFile_ReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(final, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := materialize.File(final, "", body("new")); err != nil {
		t.Fatalf("File: %v", err)
	}
	got, _ := os.ReadFile(final)
	if string(got) != "new" {
		t.Errorf("content = %q", got)
	}
}

func TestFile_CreatesParentOfStagingDir(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging", "deep")
	final := filepath.Join(dir, "out.txt")
	if _, err := materialize.File(final, staging, body("x")); err != nil {
		t.Fatalf("File with a nested staging dir: %v", err)
	}
}

// --- FileExclusive ---

func TestFileExclusive_ClaimsTheName(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "sub", "out.txt")

	if _, err := materialize.FileExclusive(final, body("first")); err != nil {
		t.Fatalf("FileExclusive: %v", err)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Errorf("content = %q", got)
	}

	// The second attempt loses, and loses recognisably — that is what a "fail"
	// collision policy is built on.
	_, err = materialize.FileExclusive(final, body("second"))
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("want os.ErrExist, got %v", err)
	}
	got, _ = os.ReadFile(final)
	if string(got) != "first" {
		t.Errorf("the loser overwrote the winner: %q", got)
	}
}

func TestFileExclusive_FailedFillRemovesTheClaim(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "out.txt")
	boom := errors.New("source went away")

	if _, err := materialize.FileExclusive(final, func(w io.Writer) error {
		_, _ = io.WriteString(w, "partial")
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("want the fill error, got %v", err)
	}
	// The claim is released, so a retry is possible.
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Errorf("claim not released: %v", err)
	}
	if _, err := materialize.FileExclusive(final, body("retry")); err != nil {
		t.Fatalf("retry after a failed fill: %v", err)
	}
}

// A symlink in the target tree must not redirect the write.
func TestFileExclusive_RefusesToFollowASymlink(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("do not touch"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if _, err := materialize.FileExclusive(link, body("hijacked")); err == nil {
		t.Fatal("wrote through a symlink")
	}
	got, _ := os.ReadFile(outside)
	if string(got) != "do not touch" {
		t.Errorf("target of the symlink was modified: %q", got)
	}
}

// An empty body is a legitimate artifact — a zero-length file, or the payload of
// a directory artifact.
func TestFile_EmptyBody(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "empty.txt")
	if _, err := materialize.File(final, "", func(io.Writer) error { return nil }); err != nil {
		t.Fatalf("File with an empty body: %v", err)
	}
	info, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("size = %d, want 0", info.Size())
	}
}
