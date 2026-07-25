// Package extractor lays a captured tree back onto a real filesystem — the
// reverse half of the Ingester↔Extractor pair (ADR-97).
//
// It restores exactly what the ingester captured in vfsmeta and nothing more:
// what was not captured is not restored. The tree itself is not rebuilt here —
// it is the materialisation of a projection view (ADR-89), which supplies
// resolved {artifact, path} pairs and has already settled internal and
// structural collisions. This package adds the two things a real filesystem
// brings: an external collision (the target path is occupied by something that
// is not ours) and the POSIX attributes of what was captured.
//
// The result is durable and owned by the user — unlike an ejection, nothing
// keeps it alive and nothing reclaims it.
//
// Like the ingester, it lives with the filesystem-path extension rather than in
// the engine's agent tree: it depends on that extension's view of paths, and a
// core agent may not depend on an extension. The single-file write is the
// primitive shared with the Ejector (ADR-97 INV-97-5).
package extractor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/internal/materialize"
	"scrinium.dev/projection"
)

// CollisionPolicy decides what happens when the target path is already taken by
// something this restore did not put there. Default Fail: never touch someone
// else's file silently.
type CollisionPolicy uint8

const (
	// Fail — stop with ErrPathOccupied.
	Fail CollisionPolicy = iota
	// Skip — leave the existing file and move on.
	Skip
	// Overwrite — replace the existing file.
	Overwrite
	// Suffix — write beside it as "name (1)", "name (2)", …
	Suffix
)

// Errors of a restore.
var (
	// ErrPathOccupied — the target path exists and the policy is Fail.
	ErrPathOccupied = errors.New("extractor: target path occupied")

	// ErrEscapesRoot — a captured path resolves outside the target root.
	// Captured paths are DATA: they come from whatever was ingested, so they
	// are checked, never trusted.
	ErrEscapesRoot = errors.New("extractor: path escapes the target root")
)

// Tree is the resolved view a restore walks. *projection.Projection's View
// satisfies it; declaring the port here keeps the extractor free of the
// projection's construction.
type Tree interface {
	WalkIn(rv projection.RootView, prefix string) projection.Seq
	OpenIn(ctx context.Context, rv projection.RootView, path string, opts ...domain.GetOption) (domain.ReadHandle, error)
}

// Config is one restore.
type Config struct {
	// Root is the directory the tree is laid out under. Created if missing.
	Root string

	// View selects which materialised tree to restore. Empty means the
	// by-path tree the filesystem-path extension provides.
	View projection.RootView

	// Prefix restores a subtree only. Empty restores everything in the view.
	Prefix string

	// Collision is the external-collision policy. Zero value is Fail.
	Collision CollisionPolicy

	// TempDir stages file writes. Empty stages beside each destination, which
	// keeps the rename atomic (same filesystem) — the safe default.
	TempDir string

	// RestoreOwner attempts chown. It needs privileges, so failure is a
	// warning, never a failure of the restore.
	RestoreOwner bool

	// DirMode is the mode for a directory the tree derived from paths, with no
	// captured metadata of its own. Zero means 0o755.
	DirMode os.FileMode
}

const defaultView = projection.RootView("by-path")

// Stats is the outcome of a restore.
type Stats struct {
	Files    int64
	Dirs     int64
	Skipped  int64 // external collisions the policy skipped
	Warnings int64 // best-effort steps that did not take (ownership, times)
}

// Extractor restores trees. It holds no state between calls.
type Extractor struct {
	tree Tree
	log  Warner
}

// Warner receives best-effort failures — an ownership change without
// privileges, a timestamp a filesystem refused. A restore continues past them,
// so they must be visible somewhere; the host decides where.
type Warner interface {
	Warn(msg string, args ...any)
}

// New binds an extractor to a resolved view. warn may be nil (warnings dropped).
func New(tree Tree, warn Warner) (*Extractor, error) {
	if tree == nil {
		return nil, fmt.Errorf("extractor: a resolved view is required")
	}
	return &Extractor{tree: tree, log: warn}, nil
}

// Extract lays the selected tree out under cfg.Root.
//
// Directories are created before their contents: the walk yields a parent
// before its children, and a directory carrying captured metadata gets that
// metadata rather than the default mode.
//
// Every destination is checked for containment inside the root before anything
// is written: a captured path is data, and data can say "../..".
func (e *Extractor) Extract(ctx context.Context, cfg Config) (Stats, error) {
	var stats Stats

	if cfg.Root == "" {
		return stats, fmt.Errorf("extractor: empty target root")
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return stats, fmt.Errorf("extractor: resolve root: %w", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return stats, fmt.Errorf("extractor: create root: %w", err)
	}
	if cfg.View == "" {
		cfg.View = defaultView
	}
	if cfg.DirMode == 0 {
		cfg.DirMode = 0o755
	}

	for node, walkErr := range e.tree.WalkIn(cfg.View, cfg.Prefix) {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if walkErr != nil {
			return stats, fmt.Errorf("extractor: walk: %w", walkErr)
		}

		target, err := contain(root, node.FS.Path)
		if err != nil {
			return stats, err
		}

		captured, hasMeta := capturedMeta(node)

		if node.FS.IsDir {
			if err := e.restoreDir(target, captured, hasMeta, cfg); err != nil {
				return stats, err
			}
			stats.Dirs++
			continue
		}

		written, err := e.restoreFile(ctx, cfg, node, target, captured, hasMeta)
		if err != nil {
			return stats, err
		}
		if !written {
			stats.Skipped++
			continue
		}
		stats.Files++
	}

	// Directory times are applied last: writing a file into a directory updates
	// that directory's mtime, so restoring it before the contents would lose it.
	if err := e.reapplyDirTimes(ctx, cfg, root, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

// restoreDir creates a directory and gives it the mode it was captured with, or
// the configured default when the tree derived it from paths and it carries no
// metadata of its own (ADR-97 развилка D, ADR-114).
func (e *Extractor) restoreDir(target string, captured vfsmeta.FileSystem, hasMeta bool, cfg Config) error {
	mode := cfg.DirMode
	if hasMeta && captured.Mode != 0 {
		mode = os.FileMode(captured.Mode & 0o7777)
	}
	if err := os.MkdirAll(target, mode); err != nil {
		return fmt.Errorf("extractor: create %q: %w", target, err)
	}
	// MkdirAll honours the mode only when it creates; an existing directory
	// keeps whatever it had, so set it explicitly.
	if err := os.Chmod(target, mode); err != nil {
		e.warn("directory mode not applied", "path", target, "err", err)
	}
	if hasMeta && cfg.RestoreOwner {
		e.chown(target, captured)
	}
	return nil
}

// restoreFile materialises one artifact at its captured path. It reports whether
// anything was written — false means an external collision the policy skipped.
func (e *Extractor) restoreFile(
	ctx context.Context,
	cfg Config,
	node projection.Node,
	target string,
	captured vfsmeta.FileSystem,
	hasMeta bool,
) (bool, error) {
	final, proceed, err := e.resolveCollision(target, cfg.Collision)
	if err != nil || !proceed {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return false, fmt.Errorf("extractor: parent of %q: %w", final, err)
	}

	fill := func(w io.Writer) error {
		rh, err := e.tree.OpenIn(ctx, cfg.View, node.FS.Path)
		if err != nil {
			return err
		}
		defer func() { _ = rh.Close() }()
		_, cerr := io.Copy(w, rh)
		return cerr
	}

	if cfg.Collision == Fail {
		// The exclusive create IS the claim: two restores into one root cannot
		// both believe they won.
		if _, err := materialize.FileExclusive(final, fill); err != nil {
			if errors.Is(err, os.ErrExist) {
				return false, fmt.Errorf("%q: %w", final, ErrPathOccupied)
			}
			return false, fmt.Errorf("extractor: write %q: %w", final, err)
		}
	} else if _, err := materialize.File(final, cfg.TempDir, fill); err != nil {
		return false, fmt.Errorf("extractor: write %q: %w", final, err)
	}

	e.applyAttrs(final, captured, hasMeta, cfg)
	return true, nil
}

// resolveCollision applies the external-collision policy. It reports the path to
// write to and whether to write at all.
func (e *Extractor) resolveCollision(target string, policy CollisionPolicy) (string, bool, error) {
	if _, err := os.Lstat(target); err != nil {
		if os.IsNotExist(err) {
			return target, true, nil
		}
		return "", false, fmt.Errorf("extractor: stat %q: %w", target, err)
	}
	switch policy {
	case Skip:
		return "", false, nil
	case Overwrite:
		return target, true, nil
	case Suffix:
		free, err := freeName(target)
		if err != nil {
			return "", false, err
		}
		return free, true, nil
	default: // Fail
		return "", false, fmt.Errorf("%q: %w", target, ErrPathOccupied)
	}
}

// applyAttrs round-trips what was captured: mode and modification time always,
// ownership only when asked and only best-effort.
func (e *Extractor) applyAttrs(path string, captured vfsmeta.FileSystem, hasMeta bool, cfg Config) {
	if !hasMeta {
		return
	}
	if captured.Mode != 0 {
		if err := os.Chmod(path, os.FileMode(captured.Mode&0o7777)); err != nil {
			e.warn("mode not applied", "path", path, "err", err)
		}
	}
	if !captured.ModTime.IsZero() {
		if err := os.Chtimes(path, captured.ModTime, captured.ModTime); err != nil {
			e.warn("times not applied", "path", path, "err", err)
		}
	}
	if cfg.RestoreOwner {
		e.chown(path, captured)
	}
}

// chown is best-effort by contract: it needs privileges, and a restore run by an
// ordinary user is still a successful restore (ADR-97 развилка C).
func (e *Extractor) chown(path string, captured vfsmeta.FileSystem) {
	if captured.UID == 0 && captured.GID == 0 {
		return
	}
	if err := os.Chown(path, int(captured.UID), int(captured.GID)); err != nil {
		e.warn("ownership not applied", "path", path, "err", err)
	}
}

// reapplyDirTimes restores directory modification times after the contents are
// in place, because writing into a directory bumps its mtime.
func (e *Extractor) reapplyDirTimes(ctx context.Context, cfg Config, root string, stats *Stats) error {
	type dirTime struct {
		path string
		when time.Time
	}
	var dirs []dirTime

	for node, walkErr := range e.tree.WalkIn(cfg.View, cfg.Prefix) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("extractor: walk: %w", walkErr)
		}
		if !node.FS.IsDir {
			continue
		}
		captured, hasMeta := capturedMeta(node)
		if !hasMeta || captured.ModTime.IsZero() {
			continue
		}
		target, err := contain(root, node.FS.Path)
		if err != nil {
			return err
		}
		dirs = append(dirs, dirTime{path: target, when: captured.ModTime})
	}

	// Deepest first, so setting a parent's time is not undone by touching a
	// child afterwards.
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chtimes(dirs[i].path, dirs[i].when, dirs[i].when); err != nil {
			e.warn("directory times not applied", "path", dirs[i].path, "err", err)
			stats.Warnings++
		}
	}
	return nil
}

func (e *Extractor) warn(msg string, args ...any) {
	if e.log != nil {
		e.log.Warn(msg, args...)
	}
}

// capturedMeta reads the filesystem schema of the artifact behind a node. A
// directory the tree derived from paths has no artifact and no metadata; that is
// the ordinary case, not an error.
func capturedMeta(node projection.Node) (vfsmeta.FileSystem, bool) {
	if node.Artifact == nil {
		return vfsmeta.FileSystem{}, false
	}
	fs, ok, err := vfsmeta.Decode(node.Artifact.Ext)
	if err != nil || !ok {
		return vfsmeta.FileSystem{}, false
	}
	return fs, true
}

// contain resolves a captured path against the root and refuses anything that
// lands outside it. Captured paths are data — they came from whatever was
// ingested — so containment is enforced, not assumed.
func contain(root, capturedPath string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(capturedPath))
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("%q: %w", capturedPath, ErrEscapesRoot)
	}
	target := filepath.Join(root, clean)
	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("%q: %w", capturedPath, ErrEscapesRoot)
	}
	return target, nil
}

// freeName finds an unused "name (n)" beside an occupied path.
func freeName(target string) (string, error) {
	ext := filepath.Ext(target)
	stem := strings.TrimSuffix(target, ext)
	for n := 1; n < 10000; n++ {
		candidate := fmt.Sprintf("%s (%d)%s", stem, n, ext)
		if _, err := os.Lstat(candidate); os.IsNotExist(err) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("extractor: no free name beside %q", target)
}
