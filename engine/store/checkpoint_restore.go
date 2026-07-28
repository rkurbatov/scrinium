package store

// Restoring the index from a checkpoint at open — the fast half of the
// recovery procedure (ADR-118). The slow half (reading every manifest back
// in) is the reconciliation that runs afterwards regardless; a restored
// checkpoint just means it finds almost everything already known.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/checkpointfmt"
)

// restoreIndexFromCheckpoint loads the newest checkpoint into the index.
// Reports whether one was used; a false without an error means "no checkpoint
// to work with", which is not a failure — the reconciliation then does the
// whole job by itself.
//
// Nothing here consults the index. The pointer is a system artifact addressed
// by name, and the payload's physical address is derived from the layout
// (cas.OpenHandleWithoutIndex): the index is what we are repairing, so it
// cannot be the source of its own repair.
func restoreIndexFromCheckpoint(ctx context.Context, s *store) (bool, error) {
	restorer, ok := s.index.(index.CheckpointRestorer)
	if !ok {
		return false, nil
	}
	name, _, ok, err := checkpointfmt.Latest(ctx, s.system)
	if err != nil {
		// A checkpoint we cannot enumerate is a lost accelerator, not a
		// fault: say so and let the full pass carry the load.
		return false, fmt.Errorf("find latest checkpoint: %w", err)
	}
	if !ok {
		return false, nil
	}

	tmpPath, cleanup, err := stageCheckpoint(ctx, s, name)
	if err != nil {
		return false, err
	}
	defer cleanup()

	if err := restorer.RestoreCheckpoint(ctx, tmpPath); err != nil {
		return false, fmt.Errorf("restore checkpoint %q: %w", name, err)
	}
	return true, nil
}

// stageCheckpoint streams the checkpoint payload to a file the index backend
// can open, and returns a cleanup that removes it.
//
// Where that file lives depends on the crypto mode (ADR-118, Р2). Plain
// stores get an ordinary temp file. Sealed and Paranoid must not leave
// plaintext on disk, so the staging directory has to be memory-backed; when
// there is none, we refuse to stage rather than leak, and the caller falls
// back to reading manifests — slower, and confidential.
func stageCheckpoint(ctx context.Context, s *store, name string) (path string, cleanup func(), err error) {
	noop := func() {}

	dir, err := checkpointStagingDir(s.snapshotConfig().ManifestCrypto)
	if err != nil {
		return "", noop, err
	}
	tmpDir, err := os.MkdirTemp(dir, "scrinium-restore-")
	if err != nil {
		return "", noop, fmt.Errorf("checkpoint staging dir: %w", err)
	}
	cleanup = func() { _ = os.RemoveAll(tmpDir) }

	rh, err := openCheckpointPayload(ctx, s, name)
	if err != nil {
		cleanup()
		return "", noop, err
	}
	defer rh.Close()

	tmpPath := filepath.Join(tmpDir, "checkpoint.db")
	f, err := os.Create(tmpPath)
	if err != nil {
		cleanup()
		return "", noop, fmt.Errorf("create checkpoint staging file: %w", err)
	}
	if _, err := io.Copy(f, rh); err != nil {
		_ = f.Close()
		cleanup()
		return "", noop, fmt.Errorf("stage checkpoint %q: %w", name, err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("close checkpoint staging file: %w", err)
	}
	return tmpPath, cleanup, nil
}

// openCheckpointPayload resolves the checkpoint pointer to its payload
// without touching the index (see restoreIndexFromCheckpoint).
func openCheckpointPayload(ctx context.Context, s *store, name string) (domain.ReadHandle, error) {
	ref, ok, err := s.system.PointerRef(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint pointer %q: %w", name, err)
	}
	if !ok {
		return nil, fmt.Errorf("checkpoint %q carries no external payload", name)
	}
	cfg := s.snapshotConfig()
	m, err := s.contentIO().LoadByDigest(ctx, ref, s.crypto.KeyProvider(), string(cfg.ContentHasher))
	if err != nil {
		return nil, fmt.Errorf("read checkpoint manifest %q: %w", name, err)
	}
	return s.contentIO().OpenHandleWithoutIndex(ctx, m, cfg.PathTopology)
}

// checkpointStagingDir picks where the checkpoint may be unpacked. "" means
// the OS default temp directory.
func checkpointStagingDir(crypto config.ManifestCrypto) (string, error) {
	if crypto == config.ManifestCryptoPlain {
		return "", nil
	}
	// Memory-backed staging: the checkpoint holds the same fields the
	// manifests hide, so writing it to a disk-backed temp would undo the
	// mode for as long as the file exists.
	const shm = "/dev/shm"
	if fi, err := os.Stat(shm); err == nil && fi.IsDir() {
		return shm, nil
	}
	return "", fmt.Errorf(
		"no memory-backed directory for checkpoint staging: refusing to write index contents " +
			"to disk under an encrypting ManifestCrypto")
}
