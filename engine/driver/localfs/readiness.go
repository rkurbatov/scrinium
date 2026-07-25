package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"scrinium.dev/engine/driver"
)

// The two things a POSIX backend can honestly offer an ingester (ADR-115):
// telling a finished file from one still being written, and walking a tree with
// its directories. The first is an optional interface with no mask bit — it is
// used, not planned around; the second is behind CapDirEntries, because a
// configuration's validity depends on it.

// ReadyToRead reports whether path can be read in full right now.
//
// The signal is a non-blocking exclusive advisory lock: if another process
// holds the file open for writing with a lock, the acquire fails and the
// answer is "not yet". The lock is released immediately — it is asked as a
// question, not taken as a claim, so this method never blocks a writer.
//
// A path that does not exist answers false rather than erroring: it may appear
// on the next pass, and a caller postpones such an element instead of failing.
//
// Emptiness is NOT taken as evidence of anything. A zero-length file is a
// legitimate file, and treating it as "still being created" would defer it
// forever; freshness is the settle window's job, and the window has already had
// its say by the time this is asked.
//
// Advisory locks are exactly that — advisory. A writer that takes no lock is
// invisible to this probe, and most writers take none (the common uploader
// writes to a temporary name and renames). That is why the consumer's guard is
// the settle window, unconditionally, and this probe only ever vetoes on top of
// it — it may postpone an element earlier than the window, never release one the
// window still holds.
func (d *Driver) ReadyToRead(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	full, err := d.resolve(path)
	if err != nil {
		return false, err
	}

	fh, err := os.OpenFile(full, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = fh.Close() }()

	if err := syscall.Flock(int(fh.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		// Held by a writer: not an error, just not ready.
		return false, nil
	}
	_ = syscall.Flock(int(fh.Fd()), syscall.LOCK_UN)

	info, err := fh.Stat()
	if err != nil {
		return false, err
	}
	if info.IsDir() {
		// A directory has nothing to read; the question does not apply, and
		// answering true would let a caller open it as a file.
		return false, nil
	}
	return true, nil
}

// ListTree walks the tree under prefix and reports every entry modified at or
// after since — directories included, unlike ListObjectsWithModTime.
//
// A directory is reported BEFORE its contents, so a consumer that materialises
// or captures in callback order never sees a child before its parent. Hidden
// entries (tombstones, in-flight temp files) are filtered as elsewhere in this
// driver, and a hidden directory prunes its whole subtree.
//
// The prefix root itself is not reported: it is the boundary of the walk, not
// part of the captured tree.
//
// Early termination is fs.SkipAll, matching ListObjectsWithModTime.
func (d *Driver) ListTree(
	ctx context.Context,
	prefix string,
	since time.Time,
	cb func(driver.TreeEntry) error,
) error {
	full, err := d.resolveDir(prefix)
	if err != nil {
		// A missing prefix is an empty walk, not an error — the same
		// convention ListObjectsWithModTime follows.
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if _, err := os.Stat(full); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	walkErr := filepath.WalkDir(full, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == full {
			return nil // the root is the boundary, not an entry
		}
		if isHidden(entry.Name()) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(since) && !entry.IsDir() {
			return nil
		}
		// A directory is never skipped by mtime: its own timestamp says
		// nothing about whether the subtree below it changed.

		rel, err := d.toLogical(path)
		if err != nil {
			return err
		}
		return cb(driver.TreeEntry{
			Path:         rel,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			Mode:         uint32(info.Mode().Perm()) | typeBits(info.Mode()),
			IsDir:        entry.IsDir(),
		})
	})

	if errors.Is(walkErr, fs.SkipAll) {
		return nil
	}
	return walkErr
}

// typeBits maps Go's portable file-mode type bits onto the POSIX ones the
// vfsmeta schema carries. Only the kinds a tree capture can meaningfully
// restore are mapped; anything else contributes no type bits and is reported
// by its permissions alone.
func typeBits(m fs.FileMode) uint32 {
	switch {
	case m.IsDir():
		return syscall.S_IFDIR
	case m&fs.ModeSymlink != 0:
		return syscall.S_IFLNK
	case m.IsRegular():
		return syscall.S_IFREG
	default:
		return 0
	}
}

// Compile-time conformance: the driver honours the capability it declares and
// the optional interface it implements, so dropping a method (or the mask bit)
// breaks the build here rather than at an assertion site.
var (
	_ driver.ReadinessProber = (*Driver)(nil)
	_ driver.TreeLister      = (*Driver)(nil)
)
