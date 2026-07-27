package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"

	"scrinium.dev/domain"
)

// QueryByExtField streams ArtifactIDs whose projected ext field extName.field
// equals value (§9.6, read-side of the Indexer projection). proj_ext holds the
// manifest digest, so the query joins manifests for the floating ArtifactID and
// drops handle-less rows (artifact_id IS NULL → system artifacts, pack
// containers): only user-visible artifacts surface, invisibility by
// construction. v1 is equality; a richer query language is M7. The callback may
// return fs.SkipAll to stop early without an error.
func (i *Index) QueryByExtField(ctx context.Context, extName, field, value string, cb func(domain.ArtifactID) error) error {
	const stmt = `
		SELECT DISTINCT m.artifact_id
		FROM proj_ext p
		JOIN manifests m ON m.manifest_digest = p.manifest_digest
		WHERE p.ext_name = ? AND p.field = ? AND p.value = ?
		  AND m.artifact_id IS NOT NULL
		ORDER BY m.artifact_id`
	rows, err := i.db.QueryContext(ctx, stmt, extName, field, value)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateArtifactIDRows(ctx, rows, cb)
}

// ListByExtField iterates over manifests whose projected ext field
// extName.field equals value, read from proj_ext (read-side of the Indexer
// projection, §9.6). It is the manifest-yielding sibling of QueryByExtField:
// where that streams bare ArtifactIDs (membership / search), this hydrates
// the index-resident Manifest — no manifest-file I/O, columns only, exactly
// as IterateManifests does — so it is the proj_ext-backed form of a listing.
// Handle-less rows (system artifacts, pack containers) are excluded by
// artifact_id IS NOT NULL. v1 is equality (a richer language is M7). The
// callback may return fs.SkipAll to stop early without an error.
func (i *Index) ListByExtField(ctx context.Context, extName, field, value string, cb func(domain.Manifest) error) error {
	const stmt = `
		SELECT ` + manifestProjection + `
		FROM manifests m
		JOIN proj_ext p ON p.manifest_digest = m.manifest_digest
		LEFT JOIN blobs b ON b.blob_ref = m.blob_ref
		WHERE p.ext_name = ? AND p.field = ? AND p.value = ?
		  AND m.artifact_id IS NOT NULL
		ORDER BY m.created_at`
	rows, err := i.db.QueryContext(ctx, stmt, extName, field, value)
	if err != nil {
		return classifyError(err)
	}
	defer rows.Close()
	return iterateManifestRows(ctx, rows, cb)
}

// iterateArtifactIDRows streams a single-column artifact_id result set through
// cb, honouring ctx cancellation and fs.SkipAll (mirrors iterateBlobRefRows).
func iterateArtifactIDRows(ctx context.Context, rows *sql.Rows, cb func(domain.ArtifactID) error) error {
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if cbErr := cb(domain.ArtifactID(id)); cbErr != nil {
			if errors.Is(cbErr, fs.SkipAll) {
				return nil
			}
			return cbErr
		}
	}
	return classifyError(rows.Err())
}
