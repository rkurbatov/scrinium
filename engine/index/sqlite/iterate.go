package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/internal/timefmt"
)

// manifestProjection is the shared SELECT list feeding scanManifestRow.
// Column order MUST match scanManifestRow's Scan order. The blobs JOIN
// recovers content_hash/original_size (the dedup key lives on the blob
// row, not the manifest row); LEFT because Inline manifests have no
// blobs partner.
const manifestProjection = `m.manifest_digest, m.artifact_id, m.session_id,
	       m.blob_ref, m.created_at, m.retention_until,
	       b.content_hash, b.original_size`

// IterateManifests iterates over every user manifest, namespace-agnostic —
// the core attaches no namespace meaning. The callback is invoked once per
// manifest in created_at order; cancelling via fs.SkipAll or any other
// callback error stops the iteration.
//
// Handleless artifacts are NEVER included: the predicate
// `artifact_id IS NOT NULL` (ADR-83) excludes pack containers (empty
// slot). System artifacts are not indexed at all (ADR-85), so they
// never appear here.
func (i *Index) IterateManifests(
	ctx context.Context,
	cb func(domain.Manifest) error,
) error {
	const query = `
			SELECT ` + manifestProjection + `
			FROM manifests m
			LEFT JOIN blobs b ON b.blob_ref = m.blob_ref
			WHERE m.artifact_id IS NOT NULL
			ORDER BY m.created_at`

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()

	return iterateManifestRows(ctx, rows, cb)
}

// GetBySession returns every ArtifactID with the given SessionID.
// Used by RollbackSession; the result set is small in practice
// (one user session, dozens to hundreds of artifacts), so we
// materialise it into a slice rather than streaming via callback.
//
// An empty SessionID guarded at the engine level (errs.ErrEmptySessionID).
// The index itself does NOT enforce that — it would be a useful
// last-line check, but consistency demands the index honour any
// query the caller passes. The engine's RollbackSession is the
// place where mass-delete safety lives.
//
// Rows with a NULL artifact_id (system artifacts under the name slot,
// pack containers) are skipped — rollback operates on user handles.
func (i *Index) GetBySession(ctx context.Context, sessionID domain.SessionID) ([]domain.ArtifactID, error) {
	const stmt = `SELECT artifact_id FROM manifests WHERE session_id = ? AND artifact_id IS NOT NULL`
	rows, err := i.db.QueryContext(ctx, stmt, string(sessionID))
	if err != nil {
		return nil, classifyError(err)
	}
	defer rows.Close()

	var out []domain.ArtifactID
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			out = append(out, domain.ArtifactID(id.String))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, classifyError(err)
	}
	return out, nil
}

// ListOrphanBlobs iterates over blobs with ref_count = 0. Used by
// the GC Agent's Mark phase — every entry is a deletion candidate.
//
// We rely on the partial index `blobs_orphan`
// (ON blobs(ref_count) WHERE ref_count = 0) so the scan
// is cheap even on very large blob tables. SQLite uses the
// partial index automatically when the query predicate matches.
func (i *Index) ListOrphanBlobs(
	ctx context.Context,
	cb func(blobRef string) error,
) error {
	const stmt = `SELECT blob_ref FROM blobs WHERE ref_count = 0 ORDER BY blob_ref`
	rows, err := i.db.QueryContext(ctx, stmt)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateBlobRefRows(ctx, rows, cb)
}

// ListUnverifiedBlobs iterates over blobs whose last_verified_at is
// strictly older than `before`, plus blobs that have never been
// scrubbed (last_verified_at IS NULL). Used by the Scrub Agent;
// the `before` cutoff is computed by the agent as
// now() - StoreConfig.MaxAge, possibly shifted upward for blobs
// on a CapNativeChecksum medium.
//
// NULL last_verified_at means "never verified" — those rows take
// priority and always come first under the ORDER BY (SQLite sorts
// NULLs first ASC by default).
//
// Order is by last_verified_at ascending: oldest first, which is
// what the scrub schedule wants. RFC 3339 second-precision strings
// (UTC) sort lexicographically the same as chronologically.
func (i *Index) ListUnverifiedBlobs(ctx context.Context, before time.Time, cb func(blobRef string) error) error {
	cutoff := timefmt.Format(before)
	const stmt = `
		SELECT blob_ref FROM blobs
		WHERE last_verified_at IS NULL OR last_verified_at < ?
		ORDER BY last_verified_at`
	rows, err := i.db.QueryContext(ctx, stmt, cutoff)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateBlobRefRows(ctx, rows, cb)
}

// iterateManifestRows is the shared cursor loop for callbacks that
// take domain.Manifest. Centralised because the iteration sites
// (IterateManifests, ManifestsByBlobRef, ListUnverifiedManifests) want
// the same context check / fs.SkipAll / scan pattern.
func iterateManifestRows(
	ctx context.Context,
	rows *sql.Rows,
	cb func(domain.Manifest) error,
) error {
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := scanManifestRow(rows)
		if err != nil {
			return fmt.Errorf("sqlite: scan manifest: %w", err)
		}
		if cbErr := cb(m); cbErr != nil {
			if errors.Is(cbErr, fs.SkipAll) {
				return nil
			}
			return cbErr
		}
	}
	return classifyError(rows.Err())
}

// ManifestsByBlobRef iterates over every manifest that references
// blobRef, joining through the manifest_blobs edge table (keyed by
// manifest digest). The Scrub Agent uses it to cascade from a verified
// physical blob to its consuming artifacts. The same projection as
// IterateManifests feeds scanManifestRow; the join to blobs recovers
// content_hash/original_size, and manifest_blobs supplies the edge.
func (i *Index) ManifestsByBlobRef(
	ctx context.Context,
	blobRef string,
	cb func(domain.Manifest) error,
) error {
	const query = `
		SELECT ` + manifestProjection + `
		FROM manifest_blobs mb
		JOIN manifests m ON m.manifest_digest = mb.manifest_digest
		LEFT JOIN blobs b ON b.blob_ref = m.blob_ref
		WHERE mb.blob_ref = ?
		ORDER BY m.manifest_digest`
	rows, err := i.db.QueryContext(ctx, query, hexKey(blobRef))
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateManifestRows(ctx, rows, cb)
}

// ListUnverifiedManifests iterates over manifests whose
// last_verified_at is older than `before` (NULL = never verified,
// always eligible), oldest first. The Scrub Agent's manifest pass uses
// it to reach Inline artifacts, which have no blobs row and so never
// surface through ListUnverifiedBlobs. Handleless artifacts are
// excluded (artifact_id IS NOT NULL, ADR-83): pack containers verify
// through their own pack blob, and system artifacts are not indexed
// (ADR-85).
func (i *Index) ListUnverifiedManifests(
	ctx context.Context,
	before time.Time,
	cb func(domain.Manifest) error,
) error {
	cutoff := timefmt.Format(before)
	const query = `
		SELECT ` + manifestProjection + `
		FROM manifests m
		LEFT JOIN blobs b ON b.blob_ref = m.blob_ref
		WHERE m.artifact_id IS NOT NULL
		  AND (m.last_verified_at IS NULL OR m.last_verified_at < ?)
		ORDER BY m.last_verified_at`
	rows, err := i.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateManifestRows(ctx, rows, cb)
}

// iterateBlobRefRows is the shared cursor loop for callbacks that
// take a single blob_ref string. ListOrphanBlobs and ListUnverifiedBlobs
// share this; M3.4 (RebuildIndexAgent) and any future "list blobs
// by predicate" method drop in here too — the differentiation is
// in the SELECT, not in the iteration.
func iterateBlobRefRows(
	ctx context.Context,
	rows *sql.Rows,
	cb func(blobRef string) error,
) error {
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var ref string
		if err := rows.Scan(hexOut{dst: &ref}); err != nil {
			return err
		}
		if cbErr := cb(ref); cbErr != nil {
			if errors.Is(cbErr, fs.SkipAll) {
				return nil
			}
			return cbErr
		}
	}
	return classifyError(rows.Err())
}
