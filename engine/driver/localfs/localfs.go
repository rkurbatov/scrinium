package localfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"scrinium.dev/engine/driver"
)

// Driver is a localfs implementation of driver.Driver. Methods are
// safe for concurrent use; concurrent operations on overlapping
// paths follow the underlying POSIX semantics (atomic rename,
// last-writer-wins for Put through the temp+rename pattern).
type Driver struct {
	root string
	opts options
}

// Compile-time interface conformance check.
var _ driver.Driver = (*Driver)(nil)

// options is the resolved configuration. Not exported; populated
// from variadic Options at construction time.
type options struct {
	fsyncOnWrite bool
	dirMode      fs.FileMode
	fileMode     fs.FileMode
}

func defaultOptions() options {
	return options{
		fsyncOnWrite: true,
		dirMode:      0o755,
		fileMode:     0o644,
	}
}

// Option configures a Driver at construction time.
type Option func(*options)

// WithFsync controls whether Put and Clone fsync the data file
// before rename and the parent directory after rename. Default is
// true. Disabling improves throughput drastically but loses crash
// safety; tests are the legitimate use case.
func WithFsync(enabled bool) Option {
	return func(o *options) { o.fsyncOnWrite = enabled }
}

// WithDirMode sets the permission used for newly created
// directories. Default 0o755.
func WithDirMode(mode fs.FileMode) Option {
	return func(o *options) { o.dirMode = mode }
}

// WithFileMode sets the permission used for newly created files.
// Default 0o644.
func WithFileMode(mode fs.FileMode) Option {
	return func(o *options) { o.fileMode = mode }
}

// New creates a Driver rooted at the given directory. The directory
// is created if it does not exist. An existing path that is not a
// directory returns an error.
//
// The root is resolved to an absolute path so subsequent calls are
// independent of the process's working directory.
func New(root string, opts ...Option) (*Driver, error) {
	if root == "" {
		return nil, fmt.Errorf("localfs: empty root")
	}

	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("localfs: resolve root: %w", err)
	}

	info, err := os.Stat(abs)
	switch {
	case err == nil:
		if !info.IsDir() {
			return nil, fmt.Errorf("localfs: %q is not a directory", abs)
		}
	case os.IsNotExist(err):
		if err := os.MkdirAll(abs, o.dirMode); err != nil {
			return nil, fmt.Errorf("localfs: create root: %w", err)
		}
	default:
		return nil, fmt.Errorf("localfs: stat root: %w", err)
	}

	return &Driver{root: abs, opts: o}, nil
}

// Root returns the absolute root directory of this driver. Useful
// for diagnostics; not part of the driver.Driver contract.
func (d *Driver) Root() string {
	return d.root
}

// resolve translates a logical (relative, forward-slash) path into
// an absolute filesystem path under d.root. It rejects absolute
// paths, empty paths, and paths attempting to escape the root via
// "..".
func (d *Driver) resolve(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("localfs: empty path")
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("localfs: absolute paths are not allowed: %q", p)
	}
	// Normalise: convert forward slashes to OS-native, clean up "."
	// and "..", then verify the result still lives under root.
	osPath := filepath.FromSlash(p)
	cleaned := filepath.Clean(osPath)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("localfs: path traversal not allowed: %q", p)
	}
	full := filepath.Join(d.root, cleaned)
	// Defensive check: filepath.Join cleans, but a malformed input
	// (lots of ".." or symlinks pointing outside) could still escape
	// in theory. Verify the absolute prefix.
	if !strings.HasPrefix(full, d.root+string(os.PathSeparator)) && full != d.root {
		return "", fmt.Errorf("localfs: path escapes root: %q", p)
	}
	return full, nil
}

// resolveDir is like resolve but accepts the empty string and "."
// as meaning the driver root. Used by read-side directory
// operations (List, ListObjectsWithModTime, CountObjects,
// PruneEmptyDirs) where "list everything" is a legitimate request.
//
// Write-side operations keep using resolve, which rejects empty
// paths — there is no sensible way to Put without a destination.
func (d *Driver) resolveDir(p string) (string, error) {
	if p == "" || p == "." || p == "/" {
		return d.root, nil
	}
	return d.resolve(p)
}

// Capabilities reports the static capability mask. See doc.go for
// the rationale of each declared flag.
func (d *Driver) Capabilities() driver.CapabilityMask {
	return driver.CapBlockAlign4096 | driver.CapWatch | driver.CapDirEntries
}
