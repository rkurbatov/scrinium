// Package materialize writes one artifact's bytes onto a real filesystem,
// atomically, and reports the hash of what landed.
//
// It exists because two agents need exactly this and must not diverge on it
// (ADR-97 INV-97-5). It sits at the module root rather than under engine/
// because one of those agents lives with the filesystem-path extension, outside
// the engine tree: an engine-internal package would be unreachable from there.
//
// The Ejector materialises a single artifact into a private scratch directory,
// named by content hash, for consumers that need a path rather than a stream.
// The Extractor lays a whole tree out at the paths the capture recorded, leaf by
// leaf. Different verbs, different lifetimes, different naming — the same write.
//
// What the primitive guarantees: a temporary file created exclusively (no
// following a symlink into somewhere else), the caller's bytes, an fsync, and a
// rename into place. A reader either sees no file or sees the whole of it; a
// crash leaves at most a temporary that the caller may remove. It hashes what it
// wrote as a side effect, so a later reuse can be verified without a second
// read.
//
// What it deliberately does NOT do: decide the destination name, apply
// permissions or timestamps, resolve collisions, or hold anything alive.
// Those differ between the two callers and are theirs.
package materialize

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Fill writes the artifact's bytes into w. It is the caller's business where
// those bytes come from — a whole read handle, a byte range, an empty body.
type Fill func(w io.Writer) error

// Staged writes fill's bytes into a temporary file in tempDir and returns that
// file's path and the hex sha256 of its contents. Nothing is committed: the
// caller either Commits it under a final name or Discards it.
//
// This is the shape a caller needs when the destination name is DERIVED from the
// content — an ejector naming files by content hash cannot know the name until
// the bytes have been hashed. A caller that knows the name up front uses File.
func Staged(tempDir string, fill Fill) (tmpPath, hash string, err error) {
	if tempDir == "" {
		return "", "", fmt.Errorf("materialize: empty staging dir")
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return "", "", fmt.Errorf("materialize: staging dir: %w", err)
	}
	tmp, err := os.CreateTemp(tempDir, ".tmp-materialize-")
	if err != nil {
		return "", "", fmt.Errorf("materialize: staging file: %w", err)
	}
	name := tmp.Name()

	h := sha256.New()
	if ferr := fill(io.MultiWriter(tmp, h)); ferr != nil {
		_ = tmp.Close()
		Discard(name)
		return "", "", ferr
	}
	if serr := tmp.Sync(); serr != nil {
		_ = tmp.Close()
		Discard(name)
		return "", "", serr
	}
	if cerr := tmp.Close(); cerr != nil {
		Discard(name)
		return "", "", cerr
	}
	return name, hex.EncodeToString(h.Sum(nil)), nil
}

// Commit renames a staged file into its final place. The rename is atomic, so a
// reader sees either no file or the whole of it.
func Commit(tmpPath, final string) error {
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return fmt.Errorf("materialize: parent dir for %q: %w", final, err)
	}
	if err := os.Rename(tmpPath, final); err != nil {
		Discard(tmpPath)
		return err
	}
	return nil
}

// Discard removes a staged file. Safe on an already-removed path.
func Discard(tmpPath string) { _ = os.Remove(tmpPath) }

// File writes fill's bytes to final, atomically, and returns the hex sha256 of
// what was written.
//
// tempDir is where the staging file is created; it must be on the same
// filesystem as final, or the rename cannot be atomic. Passing an empty tempDir
// stages beside final, which is the safe default for a tree restore.
//
// An existing final is replaced by the rename. Deciding whether that is allowed
// is the caller's policy — the Extractor asks its collision policy first, the
// Ejector names by content hash and so never overwrites anything meaningful.
func File(final, tempDir string, fill Fill) (string, error) {
	if final == "" {
		return "", fmt.Errorf("materialize: empty destination")
	}
	if tempDir == "" {
		tempDir = filepath.Dir(final)
	}
	tmp, hash, err := Staged(tempDir, fill)
	if err != nil {
		return "", fmt.Errorf("materialize: write %q: %w", final, err)
	}
	if err := Commit(tmp, final); err != nil {
		return "", fmt.Errorf("materialize: commit %q: %w", final, err)
	}
	return hash, nil
}

// FileExclusive writes to a destination that must not already exist, and fails
// with os.ErrExist if it does. It is the shape a restore wants when its
// collision policy is "fail".
//
// The exclusive create IS the write target here — no staging and no rename —
// because the point is that the name is claimed atomically by this call and by
// nobody else. The cost is that a crash mid-write leaves a short file under the
// final name; a restore that cares runs File with an explicit collision decision
// instead. O_NOFOLLOW refuses to be redirected through a symlink someone left in
// the target tree.
func FileExclusive(final string, fill Fill) (string, error) {
	if final == "" {
		return "", fmt.Errorf("materialize: empty destination")
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return "", fmt.Errorf("materialize: parent dir for %q: %w", final, err)
	}
	f, err := os.OpenFile(final, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if ferr := fill(io.MultiWriter(f, h)); ferr != nil {
		_ = f.Close()
		_ = os.Remove(final)
		return "", fmt.Errorf("materialize: write %q: %w", final, ferr)
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(final)
		return "", fmt.Errorf("materialize: sync %q: %w", final, serr)
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(final)
		return "", fmt.Errorf("materialize: close %q: %w", final, cerr)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Hash reports the hex sha256 of an existing file, for verifying a reuse
// without a second full read by the caller.
func Hash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
