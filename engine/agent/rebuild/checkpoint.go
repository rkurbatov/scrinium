package rebuild

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/checkpointfmt"
)

// tryCheckpointFastPath restores the newest checkpoint into the index and
// replays the tail of manifests written since. It returns used=false (nil
// error) when the index cannot restore checkpoints or none exists, leaving
// the caller to fall back. The checkpoint is fetched from the Store's own
// System() namespace, so it is by construction a checkpoint of this Store.
func (a *rebuildAgent) tryCheckpointFastPath(ctx context.Context, keys domain.KeyProvider) (used bool, err error) {
	restorer, ok := a.idx.(index.CheckpointRestorer)
	if !ok {
		return false, nil
	}
	name, createdAt, ok, err := checkpointfmt.Latest(ctx, a.store.System())
	if err != nil {
		return false, fmt.Errorf("find latest checkpoint: %w", err)
	}
	if !ok {
		return false, nil
	}

	// RestoreCheckpoint needs an on-disk path; stream the artifact to a temp.
	tmpPath, cleanup, err := a.fetchCheckpoint(ctx, name)
	if err != nil {
		return false, fmt.Errorf("fetch checkpoint %q: %w", name, err)
	}
	defer cleanup()

	// Identity is enforced upstream: fetchCheckpoint's System().Get opens the
	// checkpoint pointer envelope, which rejects a foreign store_id
	// (ADR-104/105). A foreign checkpoint never reaches the restore below.

	if err := restorer.RestoreCheckpoint(ctx, tmpPath); err != nil {
		return false, fmt.Errorf("restore checkpoint %q: %w", name, err)
	}
	a.setSource(RebuildSourceCheckpoint)
	a.setCheckpointUsed(name)

	// Replay the tail: manifests modified since (checkpoint time − overlap).
	// IndexManifest is idempotent, so any overlap re-reads are harmless.
	since := createdAt.Add(-a.cfg.RecoveryOverlap)
	return true, a.scanManifests(ctx, keys, since)
}

// fetchCheckpoint streams the named checkpoint artifact from System() to a
// fresh temp file, returning its path and a cleanup that removes the temp
// directory. The caller must invoke cleanup.
func (a *rebuildAgent) fetchCheckpoint(ctx context.Context, name string) (path string, cleanup func(), err error) {
	noop := func() {}
	rh, err := a.store.System().Get(ctx, name)
	if err != nil {
		return "", noop, fmt.Errorf("get: %w", err)
	}
	defer rh.Close()

	tmpDir, err := os.MkdirTemp("", "scrinium-restore-")
	if err != nil {
		return "", noop, fmt.Errorf("temp dir: %w", err)
	}
	cleanup = func() { _ = os.RemoveAll(tmpDir) }

	tmpPath := filepath.Join(tmpDir, "checkpoint.db")
	f, err := os.Create(tmpPath)
	if err != nil {
		cleanup()
		return "", noop, fmt.Errorf("create temp: %w", err)
	}
	if _, err := io.Copy(f, rh); err != nil {
		_ = f.Close()
		cleanup()
		return "", noop, fmt.Errorf("copy: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("close temp: %w", err)
	}
	return tmpPath, cleanup, nil
}
