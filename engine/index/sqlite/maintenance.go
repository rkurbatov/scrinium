package sqlite

import (
	"context"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/internal/timefmt"
)

// MarkVerified records that a Scrub Agent has just finished a
// successful checksum verification of blobRef. The timestamp is
// the moment the verification completed; future scrubs use it
// to prioritise the oldest-verified blobs first.
//
// A missing blob is a no-op rather than an error: by the time the
// Scrub Agent reaches a blob, the GC may have already removed it
// in a parallel cycle. Failing here would create useless noise in
// scrub logs without helping anything.
func (i *Index) MarkVerified(ctx context.Context, blobRef string, timestamp time.Time) error {
	return i.observe("MarkVerified", func() error {
		const stmt = `UPDATE blobs SET last_verified_at = ? WHERE blob_ref = ?`
		_, err := i.db.ExecContext(ctx, stmt, timefmt.Format(timestamp), hexKey(blobRef))
		return err
	})
}

// MarkManifestVerified records that the Scrub Agent has fully verified
// the artifact: its manifest re-hashed and (for blob-backed artifacts)
// its blobs confirmed fresh. The manifest-level stamp
// complements MarkVerified, which stamps physical blobs — it is the
// only place an Inline artifact's verification can be recorded, since
// Inline carries no blobs row.
//
// A missing artifact is a no-op rather than an error, mirroring
// MarkVerified: a parallel Delete may have removed the manifest between
// the scrub list and the stamp, and failing here would only add noise.
func (i *Index) MarkManifestVerified(ctx context.Context, artifactID domain.ArtifactID, timestamp time.Time) error {
	return i.observe("MarkManifestVerified", func() error {
		const stmt = `UPDATE manifests SET last_verified_at = ? WHERE artifact_id = ?`
		_, err := i.db.ExecContext(ctx, stmt, timefmt.Format(timestamp), string(artifactID))
		return err
	})
}

// DeleteOrphanBlob removes the blobs row for blobRef only while it is
// still an orphan (ref_count = 0). The guard lives in the WHERE clause
// so the check and the delete are one atomic statement: a concurrent
// Revive that bumps ref_count between the GC Sweep and this call leaves
// the row in place. removed reports whether a row was actually deleted.
func (i *Index) DeleteOrphanBlob(ctx context.Context, blobRef string) (bool, error) {
	var removed bool
	err := i.observe("DeleteOrphanBlob", func() error {
		const stmt = `DELETE FROM blobs WHERE blob_ref = ? AND ref_count = 0`
		res, err := i.db.ExecContext(ctx, stmt, hexKey(blobRef))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		removed = n > 0
		return nil
	})
	return removed, err
}
