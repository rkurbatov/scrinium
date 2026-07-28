package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/internal/timefmt"
	"scrinium.dev/errs"
)

// Resolve returns the physical address of a blob. It is the
// hot-path call on every Get; performance matters but correctness
// matters more — a stale address means reading a different file
// or a deleted one.
//
// A missing blob_ref returns errs.ErrArtifactNotFound. The choice
// of sentinel deserves a note: ErrArtifactNotFound is the
// engine-level "this thing is not here" error; from the StoreIndex
// perspective there is no separate "blob not found" — the index
// either knows where to find a blob or it does not.
//
// Resolves loose blobs only — blobs.physical_path. Packed
// placement is owned by custom-index Resolvers (ADR-86), not a column
// here; the core never branches on pack state.
func (i *Index) Resolve(ctx context.Context, blobRef string) (domain.PhysicalAddress, error) {
	const stmt = `SELECT physical_path FROM blobs WHERE blob_ref = ?`
	var addr domain.PhysicalAddress
	err := i.db.QueryRowContext(ctx, stmt, hexKey(blobRef)).
		Scan(&addr.Path)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return domain.PhysicalAddress{}, errs.ErrArtifactNotFound
	case err != nil:
		return domain.PhysicalAddress{}, classifyError(err)
	}
	return addr, nil
}

// ExistsByContent is the deduplication primitive. It looks up a
// blob by the composite key (content_hash, original_size,
// crypto_identity) — ADR-58. The size guards against pathological
// hash-prefix collisions across files of different lengths; the
// crypto-identity guards against collapsing two physically distinct
// encrypted blobs (different key, or Plain vs encrypted) that happen
// to share a plaintext ContentHash. For Plain blobs crypto is empty
// and the key degrades to the (ContentHash, OriginalSize) pair.
//
// Returns (blobRef, true, nil) when found; ("", false, nil) when
// absent; and ("", false, err) for unexpected failures.
func (i *Index) ExistsByContent(ctx context.Context, hash domain.ContentHash, originalSize int64, crypto domain.CryptoIdentity) (string, bool, error) {
	const stmt = `
		SELECT blob_ref FROM blobs
		WHERE content_hash = ? AND original_size = ? AND crypto_identity = ?
		LIMIT 1`
	var ref string
	err := i.db.QueryRowContext(ctx, stmt, hexKey(hash), originalSize, string(crypto)).Scan(hexOut{dst: &ref})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, classifyError(err)
	}
	return ref, true, nil
}

// ExistsByHash is the chunk-deduplication primitive used by
// chunker.Wrapper. It keys on the full dedup triple (content_hash,
// original_size, crypto_identity) — ADR-58. A chunk is anonymous in
// name but not in length (the chunker knows the plaintext size) nor
// in crypto-identity (an encrypted chunk under Disabled has a random
// IV and must not collapse onto another). For a Plain chunk crypto is
// empty and the key degrades to (content_hash, original_size). It
// also distinguishes a normal blob from a tombstoned one.
//
// At the index level we don't currently track tombstones — the
// driver does (see localfs.MarkTombstone). The StoreIndex contract
// returns BlobIsTombstone when the index has a record marked as
// such; for now there is no schema field for it, so we always
// return BlobNotFound or BlobExists. The future schema migration
// that adds a tombstone column will make this method richer
// without changing its signature.
//
// This is a deliberate gap, not a bug. The current architecture
// uses the index for liveness (ref_count > 0) and the driver for
// physical state. Until M3.2 (GC) ties them together, BlobIsTombstone
// returns are not produced.
func (i *Index) ExistsByHash(ctx context.Context, hash domain.ContentHash, originalSize int64, crypto domain.CryptoIdentity) (domain.BlobExistStatus, error) {
	const stmt = `
		SELECT 1 FROM blobs
		WHERE content_hash = ? AND original_size = ? AND crypto_identity = ?
		LIMIT 1`
	var one int
	err := i.db.QueryRowContext(ctx, stmt, hexKey(hash), originalSize, string(crypto)).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return domain.BlobNotFound, nil
	case err != nil:
		return domain.BlobNotFound, classifyError(err)
	}
	return domain.BlobExists, nil
}

// GetRefCount returns the current reference count of a blob. A
// missing blob returns errs.ErrArtifactNotFound — same rationale
// as Resolve: the index either has the blob or it does not.
//
// Returning 0 on a missing blob (instead of an error) was tempting
// for "it's just a number, callers can treat it as no references"
// — but it would hide the difference between "blob is dead, GC can
// reap" and "blob never existed". Two very different conditions.
func (i *Index) GetRefCount(ctx context.Context, blobRef string) (int, error) {
	const stmt = `SELECT ref_count FROM blobs WHERE blob_ref = ?`
	var n int
	err := i.db.QueryRowContext(ctx, stmt, hexKey(blobRef)).Scan(&n)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, errs.ErrArtifactNotFound
	case err != nil:
		return 0, classifyError(err)
	}
	return n, nil
}

// scanManifestRow scans one row produced by the projection
// `manifests m LEFT JOIN blobs b ON b.blob_ref = m.blob_ref`. The
// blobs side supplies content_hash and original_size; nullable here
// because Inline manifests have no blobs row (bytes live in the
// manifest). Returns a partial domain.Manifest with the persisted
// routing fields; the rest (Pipeline, LayoutHeader, InlineBlob, Ext,
// Usr, the full BlobRefs/HandleRefs arrays) are absent — the caller
// reconstructs them from the manifest file on disk if needed. This is
// intentional: the index is the cheap routing layer, not the source of
// truth for manifest content.
//
// The identity slot is the nullable artifact_id (ADR-83/84/92): a user
// artifact's handle, or NULL for a pack container (empty slot). System
// artifacts are not indexed (ADR-85), so no name column exists.
func scanManifestRow(rows *sql.Rows) (domain.Manifest, error) {
	var (
		manifestDigest string
		artifactID     sql.NullString
		sessionID      domain.SessionID
		createdAt      string
		retention      sql.NullString
		originalSize   sql.NullInt64

		// Key columns come back as raw bytes and are re-hexed on the way
		// out (keys.go); validity of the nullable ones is tracked here
		// because a hex string alone cannot express NULL.
		blobRef          string
		blobRefValid     bool
		contentHash      string
		contentHashValid bool
	)
	if err := rows.Scan(
		hexOut{dst: &manifestDigest}, &artifactID, &sessionID,
		hexOutNull{dst: &blobRef, valid: &blobRefValid}, &createdAt, &retention,
		hexOutNull{dst: &contentHash, valid: &contentHashValid}, &originalSize,
	); err != nil {
		return domain.Manifest{}, err
	}
	m := domain.Manifest{
		Digest:    domain.ManifestDigest(manifestDigest),
		SessionID: sessionID,
	}
	if artifactID.Valid {
		m.ArtifactID = domain.ArtifactID(artifactID.String)
	}
	if blobRefValid {
		// NULL when LayoutHeader.BlobStorage == "Inline" (§9.1.2);
		// stays as the zero BlobRef. Callers that need to know whether
		// this is Inline must read the file. Transitional single-blob
		// cache; the authoritative list is manifest_blobs (ADR-92).
		m.BlobRefs = []domain.BlobRef{domain.BlobRef(blobRef)}
	}
	if contentHashValid {
		m.ContentHash = domain.ContentHash(contentHash)
	}
	if originalSize.Valid {
		m.OriginalSize = originalSize.Int64
	}
	t, err := timefmt.Parse(createdAt)
	if err != nil {
		return domain.Manifest{}, fmt.Errorf("scan created_at: %w", err)
	}
	m.CreatedAt = t
	if retention.Valid {
		t, err := timefmt.Parse(retention.String)
		if err != nil {
			return domain.Manifest{}, fmt.Errorf("scan retention_until: %w", err)
		}
		m.RetentionUntil = t
	}
	return m, nil
}
