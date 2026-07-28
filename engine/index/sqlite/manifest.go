package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/internal/timefmt"
)

// IndexManifest registers an artifact in the index. The strategy is
// chosen by the identity slot / structure (ADR-83/92), not a type field:
//   - plain blob (filled handle slot, single blob): upserts the blob
//     row, increments ref_count, inserts the manifest row, links
//     manifest -> blob.
//   - composite (chunker "composite" flag in Ext): same as blob plus a
//     ref_count bump per chunkRef and positional manifest -> chunk links.
//   - headless pack container (empty slot): indexed exactly like a plain
//     blob (its body blob_ref flows through manifest_blobs). The pack
//     PLACEMENT map is owned by the bundler's custom-index Resolver
//     (ADR-86), not the core — the core holds no pack table and does not
//     branch on pack-ness.
//
// Regardless of strategy, the manifest's HandleRefs (artifact→artifact
// edges, ADR-92) are linked into manifest_handles after the per-strategy
// step. Loose and composite carry none; pack containers carry their
// members. The manifests table is keyed by ManifestDigest (the file
// hash); the floating handle and the system name are nullable slot
// columns (ADR-83/85/92).
//
// All work happens inside a single transaction; partial registration
// is impossible.
func (i *Index) IndexManifest(
	ctx context.Context,
	m domain.Manifest,
	addr domain.PhysicalAddress,
) error {
	return i.observe("IndexManifest", func() error {
		return i.indexManifestTx(ctx, m, addr)
	})
}

// indexManifestTx runs the actual transactional work. Split out
// for readability — IndexManifest stays a thin observability
// wrapper, while the transactional flow lives here at one level
// of nesting.
func (i *Index) indexManifestTx(
	ctx context.Context,
	m domain.Manifest,
	addr domain.PhysicalAddress,
) error {
	return i.inTx(ctx, func(tx *sql.Tx) error {
		// Registration is uniform across kinds (ADR-83/87/92), driven by
		// structure, not a type field: every blob in blob_refs is
		// registered + ref-counted + linked positionally — a composite's
		// chunk list lives in blob_refs (the core keeps its ref_count), a
		// plain blob has one, a headless pack container [toc, pack]. Pack
		// PLACEMENT (the per-member slice map) is owned by the bundler's
		// custom-index Resolver (ADR-86), recorded out-of-band via
		// RecordPack — the core holds no pack table (closure, ADR-83).
		if err := indexBlobManifest(ctx, tx, m, addr); err != nil {
			return err
		}

		// Link the artifact→artifact edges (HandleRefs, ADR-92). Empty
		// for loose and composite; populated for pack containers
		// (placement of members). Current fixtures set none, so the loop
		// is a no-op until the bundler populates HandleRefs.
		for pos, hr := range m.HandleRefs {
			if err := linkManifestToHandle(ctx, tx, m.Digest, hr, pos); err != nil {
				return err
			}
		}

		// Dispatch to subscribed custom indexes BEFORE commit. An error
		// from any custom index rolls back the entire transaction —
		// strict-consistency guarantee per ADR-49. A headless pack
		// container has no handle and is not surfaced through Walk, so
		// we skip dispatch for it; custom indexes index user-visible
		// artifacts only.
		if !m.IsContainer() {
			// Indexers: project ext/usr into proj_* and write any own
			// tables, in this transaction (§9.2.1). Before the generic
			// dispatch so projections land first; a failure rolls back the
			// whole write (strict consistency).
			if err := i.applyIndexers(ctx, tx, m); err != nil {
				return err
			}
			args := customindex.EventArgs{Manifest: m, ArtifactID: m.ArtifactID}
			if err := i.dispatchCustomIndexes(ctx, tx, customindex.EventKindManifestIndexed, args); err != nil {
				return err
			}
		}

		// Stamp the change-sequence on the row in this transaction (ADR-106):
		// Token advances and Since(cursor) will surface this digest. Applies to
		// every kind, container included — the manifests row exists either way.
		c, err := nextCSN(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE manifests SET csn = ? WHERE manifest_digest = ?`,
			c, hexKey(m.Digest),
		); err != nil {
			return fmt.Errorf("sqlite: stamp csn: %w", err)
		}
		return nil
	})
}

// insertManifestRow writes a row into the manifests table.
// Idempotency: ON CONFLICT(manifest_digest) DO NOTHING. The same
// manifest file registered twice (after a crash, for instance) is a
// no-op rather than an error — re-indexing identical content lands the
// identical row.
//
// Primary key: ManifestDigest (the hash of the file bytes, always
// present). Identity lives in the single nullable slot column
// artifact_id (ADR-83/84/92):
//   - a user artifact's floating handle (m.ArtifactID);
//   - NULL for a pack container — the empty slot; the container is
//     addressed by its volume hash, not a handle.
//
// System artifacts are NOT indexed (ADR-85: identity is a name,
// addressed name→path bypassing the index), so they never reach this
// row and the table has no name column.
func insertManifestRow(ctx context.Context, tx *sql.Tx, m domain.Manifest) error {
	const stmt = `
		INSERT INTO manifests (
			manifest_digest, artifact_id, session_id,
			blob_ref, created_at, retention_until
		) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(manifest_digest) DO NOTHING`

	// Identity slot. A user artifact fills artifact_id with its floating
	// handle; a pack container has an empty slot (artifact_id NULL — it is
	// addressed by its volume hash). System artifacts are NOT indexed
	// (ADR-85: name→path bypasses the index), so they never reach this
	// row and the table has no name column.
	var artifactIDArg any
	if m.ArtifactID != "" {
		artifactIDArg = string(m.ArtifactID)
	}

	// blob_ref is NULL for Inline manifests per §9.1.2 — Inline blobs do
	// not have a row in `blobs`, and the routing layer uses the absence
	// of a JOIN partner as the "this is inline, read the file" signal.
	// It is a transitional single-blob cache; the authoritative blob
	// list is manifest_blobs (ADR-92).
	// hexKey("") binds as NULL, which is exactly the Inline case.
	var blobRefArg hexKey
	if m.LayoutHeader.BlobStorage != domain.LayoutInline && len(m.BlobRefs) > 0 {
		blobRefArg = hexKey(m.BlobRefs[0])
	}

	// retention_until is NULL when no retention was set. Stored alongside
	// the manifest row (rather than read from the file at Delete-time) so
	// RollbackSession can do its atomic retention check in one indexed
	// query instead of N file reads.
	var retentionArg any
	if !m.RetentionUntil.IsZero() {
		retentionArg = timefmt.Format(m.RetentionUntil)
	}

	createdAt := m.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	_, err := tx.ExecContext(ctx, stmt,
		hexKey(m.Digest),
		artifactIDArg,
		m.SessionID,
		blobRefArg,
		timefmt.Format(createdAt),
		retentionArg,
	)
	return err
}

// linkManifestToHandle inserts an edge row into manifest_handles, keyed
// by the manifest digest (ADR-92). Position preserves array order.
// Idempotency is provided by the PRIMARY KEY (manifest_digest, position)
// and ON CONFLICT DO NOTHING.
//
// NOTE: this stores the artifact→artifact edge only. Reference-count
// accounting for HandleRefs (the filled-slot consumption direction,
// ADR-92) is deferred until a producer exists — no loose or composite
// carries handle edges, and pack containers arrive with the rebuilt
// bundler.
func linkManifestToHandle(
	ctx context.Context,
	tx *sql.Tx,
	digest domain.ManifestDigest,
	handleRef domain.HandleRef,
	position int,
) error {
	const stmt = `
		INSERT INTO manifest_handles (manifest_digest, handle_ref, position)
		VALUES (?, ?, ?)
		ON CONFLICT(manifest_digest, position) DO NOTHING`
	// handle_ref stays text: a handle is "<algo>-<hex>", not bare hex.
	_, err := tx.ExecContext(ctx, stmt,
		hexKey(digest), string(handleRef), position,
	)
	return err
}

// --- Registration path ---

func indexBlobManifest(
	ctx context.Context,
	tx *sql.Tx,
	m domain.Manifest,
	addr domain.PhysicalAddress,
) error {
	// Inline manifests carry their bytes inside the manifest
	// itself and do not have a separate blob record. The manifest
	// is still indexed so Walk and GetBySession find it; the
	// blobs table is left alone (deduplication is disabled for
	// inline blobs by design — docs §… ).
	//
	// We dispatch on LayoutHeader.BlobStorage rather than on
	// emptiness of BlobRefs because §7.2 mandates the array be
	// populated even on inline manifests (it carries the hash of
	// the embedded bytes).
	if m.LayoutHeader.BlobStorage == domain.LayoutInline {
		return insertManifestRow(ctx, tx, m)
	}
	if len(m.BlobRefs) == 0 {
		return fmt.Errorf("sqlite: manifest %q has no BlobRefs", m.ArtifactID)
	}
	// Every blob in blob_refs is registered + ref-counted (ADR-87/92):
	// a loose blob has one, a composite the ordered chunk list, a pack
	// container [toc_blob, pack_blob]. The upsert (ON CONFLICT DO NOTHING)
	// keeps a blob the chunker/bundler already wrote and only adds a ref.
	//
	// Each statement runs once per BlobRef, so for a composite with many
	// chunks prepare each once and reuse it across the loop instead of
	// recompiling per ref.
	insBlob, err := tx.PrepareContext(ctx, `
		INSERT INTO blobs (
			blob_ref, content_hash, original_size,
			crypto_identity, physical_path,
			ref_count, last_verified_at, created_at
		) VALUES (?, ?, ?, ?, ?, 0, NULL, ?)
		ON CONFLICT(blob_ref) DO NOTHING`)
	if err != nil {
		return err
	}
	defer insBlob.Close()
	bumpRC, err := tx.PrepareContext(ctx,
		`UPDATE blobs SET ref_count = ref_count + 1 WHERE blob_ref = ?`)
	if err != nil {
		return err
	}
	defer bumpRC.Close()

	now := timefmt.Format(time.Now())
	crypto := string(domain.CryptoIdentityOf(m.Pipeline))
	for _, ref := range m.BlobRefs {
		blobRef := string(ref)
		if _, err := insBlob.ExecContext(ctx,
			hexKey(blobRef), hexKey(m.ContentHash), m.OriginalSize,
			crypto, addr.Path, now,
		); err != nil {
			return err
		}
		res, err := bumpRC.ExecContext(ctx, hexKey(blobRef))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("sqlite: register blob %q: not present after upsert", blobRef)
		}
	}

	if err := insertManifestRow(ctx, tx, m); err != nil {
		return err
	}

	linkBlob, err := tx.PrepareContext(ctx, `
		INSERT INTO manifest_blobs (manifest_digest, blob_ref, position)
		VALUES (?, ?, ?)
		ON CONFLICT(manifest_digest, position) DO NOTHING`)
	if err != nil {
		return err
	}
	defer linkBlob.Close()
	dg := hexKey(m.Digest)
	for pos, ref := range m.BlobRefs {
		if _, err := linkBlob.ExecContext(ctx, dg, hexKey(ref), pos); err != nil {
			return err
		}
	}
	return nil
}

// DeleteManifest performs the logical deletion of a manifest, keyed
// by its digest (the manifests PK). Single transaction:
//  1. Confirm the manifest row exists — source of truth for "already
//     deleted". A missing row is a no-op. (manifest_blobs is NOT the
//     existence oracle: Inline manifests have no edges there by design,
//     §9.2.1.)
//  2. Read the (digest, blob_ref) edges from manifest_blobs — the
//     authoritative set of blobs to decrement.
//  3. Decrement ref_count for each.
//  4. Delete the manifest_blobs and manifest_handles edges.
//  5. Delete the manifests row.
//
// The decremented set is DERIVED from manifest_blobs, not supplied by
// the caller: the index stored those edges at IndexManifest time, so it
// is the authoritative source. RebuildIndex is the recovery tool if the
// index is suspected corrupt.
//
// Idempotency: deleting an already-deleted digest returns nil.
func (i *Index) DeleteManifest(ctx context.Context, digest domain.ManifestDigest) error {
	return i.observe("DeleteManifest", func() error {
		return i.deleteManifestTx(ctx, digest)
	})
}

func (i *Index) deleteManifestTx(ctx context.Context, digest domain.ManifestDigest) error {
	return i.inTx(ctx, func(tx *sql.Tx) error {
		// Existence + artifact_id (for the deletion event). A missing
		// row means the manifest is already gone: no-op. artifact_id is
		// NULL for headless containers — COALESCE to "".
		dg := hexKey(digest)
		var artifactID string
		err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(artifact_id, '') FROM manifests WHERE manifest_digest = ? LIMIT 1`,
			dg,
		).Scan(&artifactID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		// Read the blob edges from the index — the authoritative set.
		rows, err := tx.QueryContext(ctx,
			`SELECT blob_ref FROM manifest_blobs WHERE manifest_digest = ?`,
			dg,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		var actual []string
		for rows.Next() {
			var ref string
			if err := rows.Scan(hexOut{dst: &ref}); err != nil {
				return err
			}
			actual = append(actual, ref)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		decRC, err := tx.PrepareContext(ctx,
			`UPDATE blobs SET ref_count = ref_count - 1 WHERE blob_ref = ? AND ref_count > 0`)
		if err != nil {
			return err
		}
		defer decRC.Close()
		for _, ref := range actual {
			if _, err := decRC.ExecContext(ctx, hexKey(ref)); err != nil {
				return err
			}
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM manifest_blobs WHERE manifest_digest = ?`,
			dg,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM manifest_handles WHERE manifest_digest = ?`,
			dg,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM manifests WHERE manifest_digest = ?`,
			dg,
		); err != nil {
			return err
		}

		// Remove the manifest's built-in projections (core-owned, by digest).
		if err := deleteProjections(ctx, tx, dg); err != nil {
			return err
		}

		// Own-table cleanup: every Indexer's Unindex removes the rows it
		// wrote to its own tables (§9.2.1). The delete path has no manifest
		// body, so we hand Unindex the indexed identity; an Indexer that
		// needs the payload recovers it from its own tables (fspathindex).
		// Skipped for headless containers (empty artifact_id) — custom
		// indexes index user-visible artifacts only, symmetric with the
		// write-side !IsContainer() guard.
		if artifactID != "" {
			if err := i.applyUnindexers(ctx, tx, domain.Manifest{
				ArtifactID: domain.ArtifactID(artifactID),
				Digest:     digest,
			}); err != nil {
				return err
			}
		}

		// Advance the change-sequence and record the prune watermark in this
		// transaction (ADR-106): the row is gone, so Since cannot surface the
		// deleted digest by csn — prune_csn drives Gapped (a consumer behind it
		// does a full Walk). Token still moves, so a convergent consumer
		// (projection, system-artifact cache) re-derives.
		c, err := nextCSN(ctx, tx)
		if err != nil {
			return err
		}
		if err := markPrune(ctx, tx, c); err != nil {
			return err
		}

		// Dispatch to custom indexes before commit. EventArgs carries the
		// actual blob refs read from manifest_blobs (authoritative set)
		// and the artifact id of the deleted row.
		args := customindex.EventArgs{
			ArtifactID: domain.ArtifactID(artifactID),
			BlobRefs:   actual,
		}
		return i.dispatchCustomIndexes(ctx, tx, customindex.EventKindManifestDeleted, args)
	})
}

// ResolveManifestDigest returns the current on-disk digest for a handle.
//
// The manifests_artifact index is deliberately NON-UNIQUE (decision R6):
// a form migration (re-key, rebundling) may safely hold two rows for one
// handle in transit — build the new form first, tear down the old one
// after. During that window the resolve is made deterministic by csn:
// the freshest form (max csn) wins. Duplicate-handle detection outside
// an active migration is the Scrub Agent's invariant check, not a
// schema constraint.
func (i *Index) ResolveManifestDigest(ctx context.Context, id domain.ArtifactID) (domain.ManifestDigest, bool, error) {
	const stmt = `SELECT manifest_digest FROM manifests WHERE artifact_id = ? ORDER BY csn DESC LIMIT 1`
	var d string
	err := i.db.QueryRowContext(ctx, stmt, string(id)).Scan(hexOut{dst: &d})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, classifyError(err)
	}
	return domain.ManifestDigest(d), true, nil
}

// ListDuplicateHandles returns every handle carrying more than one live
// manifest row (index.DuplicateHandleAuditor, decision R6). A duplicate
// is legal only inside an active form migration; the Scrub Agent calls
// this once per cycle and reports anything it finds. Cheap: a single
// GROUP BY over the manifests_artifact index.
func (i *Index) ListDuplicateHandles(ctx context.Context) ([]domain.ArtifactID, error) {
	const stmt = `SELECT artifact_id FROM manifests
		WHERE artifact_id IS NOT NULL AND artifact_id != ''
		GROUP BY artifact_id HAVING COUNT(*) > 1
		ORDER BY artifact_id`
	rows, err := i.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, classifyError(err)
	}
	defer rows.Close()
	var dups []domain.ArtifactID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, classifyError(err)
		}
		dups = append(dups, domain.ArtifactID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, classifyError(err)
	}
	return dups, nil
}

// ManifestExistsByDigest reports whether a manifest row carries digest.
// The digest is the primary key, so this is a direct point-lookup.
func (i *Index) ManifestExistsByDigest(ctx context.Context, digest domain.ManifestDigest) (bool, error) {
	const stmt = `SELECT 1 FROM manifests WHERE manifest_digest = ? LIMIT 1`
	var one int
	err := i.db.QueryRowContext(ctx, stmt, hexKey(digest)).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, classifyError(err)
	}
	return true, nil
}
