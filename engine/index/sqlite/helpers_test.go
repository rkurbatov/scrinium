package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/internal/timefmt"
)

// Glass-box helpers shared between the per-package tests of the
// sqlite implementation. The conformance suite at
// internal/testutil/indexsuite does not use these — it asserts
// through the public StoreIndex API only. These helpers exist
// because the remaining sqlite-only tests (vacuum content checks,
// blob row with packed metadata) need direct SQL access that the
// public API does not
// expose.

// countRows returns the number of rows in `table`. Used by
// vacuum-checkpoint tests to verify the checkpoint copied data
// across; the public API does not surface raw row counts.
func countRows(t *testing.T, idx *Index, table string) int {
	t.Helper()
	var n int
	q := `SELECT COUNT(*) FROM ` + table
	if err := idx.db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// insertBlob inserts a blob row directly into the blobs table,
// bypassing IndexManifest's blob-side bookkeeping (manifest_blobs
// links, ref_count from incoming manifest, etc.). Used by tests that
// need to stage a specific blob-row shape directly.
func insertBlob(t *testing.T, idx *Index, ref, contentHash string, size int64, addr domain.PhysicalAddress, refCount int) {
	t.Helper()
	_, err := idx.db.ExecContext(context.Background(),
		`INSERT INTO blobs (
			blob_ref, content_hash, original_size,
			crypto_identity, physical_path,
			ref_count, last_verified_at, created_at
		) VALUES (?, ?, ?, '', ?, ?, NULL, ?)`,
		hexKey(ref), hexKey(contentHash), size, addr.Path,
		refCount, timefmt.Format(time.Now()),
	)
	if err != nil {
		t.Fatalf("insertBlob: %v", err)
	}
}

// insertManifest inserts a manifest row directly via SQL,
// bypassing IndexManifest. Used by WriteCheckpoint_CreatesCheckpoint to
// stage data so the checkpoint is a meaningful copy without
// dragging blob-side bookkeeping into the test setup.
func insertManifest(t *testing.T, idx *Index, m domain.Manifest) {
	t.Helper()
	createdAt := m.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	// blob_ref is NULL for Inline manifests (§9.1.2). The list
	// helpers in tests rarely set LayoutHeader, so the common
	// path is non-NULL — but we honour the invariant either way.
	var blobRefArg hexKey // empty binds as NULL
	if m.LayoutHeader.BlobStorage != domain.LayoutInline {
		blobRefArg = hexKey(m.PrimaryBlobRef())
	}
	var retentionArg any
	if !m.RetentionUntil.IsZero() {
		retentionArg = timefmt.Format(m.RetentionUntil)
	}
	// manifest_digest is the PK after the identity axis (ADR-83/92).
	// Honour a fixture-supplied digest; synthesize a distinct one from
	// the row's identifying fields otherwise so two staged manifests
	// never collide on an empty PK.
	digest := string(m.Digest)
	if digest == "" {
		// A digest is bare hex (ADR-93) — no algorithm prefix, or the
		// binary key column could not hold it.
		sum := sha256.Sum256([]byte(string(m.ArtifactID) + "|" + string(m.SessionID) + "|" + timefmt.Format(createdAt)))
		digest = hex.EncodeToString(sum[:])
	}
	_, err := idx.db.ExecContext(context.Background(),
		`INSERT INTO manifests (
			manifest_digest, artifact_id, session_id,
			blob_ref, created_at, retention_until
		) VALUES (?, ?, ?, ?, ?, ?)`,
		hexKey(digest), string(m.ArtifactID),
		m.SessionID, blobRefArg,
		timefmt.Format(createdAt), retentionArg,
	)
	if err != nil {
		t.Fatalf("insertManifest: %v", err)
	}
}
