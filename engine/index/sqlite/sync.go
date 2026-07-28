package sqlite

import (
	"context"
	"fmt"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
)

// waitPollInterval is how often Wait re-reads Token while blocking. Pull is
// the source of truth for the synchronization capability (ADR-106); a
// serverless SQLite backend has no LISTEN/NOTIFY, so Wait polls. The idle
// probe is a single-row read, so a tight-ish interval is cheap.
const waitPollInterval = 50 * time.Millisecond

// Token reports the current change-sequence high-water mark — the last csn
// the index issued (ADR-106). It is the cheap "did anything change?" probe.
func (i *Index) Token(ctx context.Context) (index.Token, error) {
	csn, err := readToken(ctx, i.db)
	if err != nil {
		return 0, err
	}
	return index.Token(csn), nil
}

// Since returns the manifests whose csn is greater than cursor, in csn order,
// the cursor to resume from, and whether a hard deletion pruned history at or
// after cursor (ADR-106).
//
// Hard deletes are not enumerable: the row is gone, so a deleted digest never
// appears in Changes. A deletion is reported through Gapped (cursor below the
// prune watermark), which sends the consumer to a full Walk. Next is the
// highest csn actually returned, not the live Token — so a write racing this
// read is picked up by the next call instead of being skipped.
func (i *Index) Since(ctx context.Context, cursor index.Token) (index.Delta, error) {
	d := index.Delta{Next: cursor}

	rows, err := i.db.QueryContext(ctx,
		`SELECT manifest_digest, csn FROM manifests WHERE csn > ? ORDER BY csn`,
		uint64(cursor),
	)
	if err != nil {
		return index.Delta{}, fmt.Errorf("sqlite: Since: query changes: %w", err)
	}
	for rows.Next() {
		var (
			digest string
			csn    uint64
		)
		if err := rows.Scan(hexOut{dst: &digest}, &csn); err != nil {
			_ = rows.Close()
			return index.Delta{}, fmt.Errorf("sqlite: Since: scan: %w", err)
		}
		d.Changes = append(d.Changes, index.Change{
			Digest: domain.ManifestDigest(digest),
			CSN:    index.Token(csn),
		})
		d.Next = index.Token(csn)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return index.Delta{}, fmt.Errorf("sqlite: Since: iterate: %w", err)
	}
	// Close the result set before the watermark read: an open *sql.Rows keeps
	// the connection checked out, and a :memory: pool would hand the next
	// query a fresh, empty database. Reading prune_csn after the rows also
	// keeps Gapped a safe over-approximation — a delete racing this read can
	// only raise the watermark, never hide one.
	if err := rows.Close(); err != nil {
		return index.Delta{}, fmt.Errorf("sqlite: Since: close: %w", err)
	}

	var prune uint64
	if err := i.db.QueryRowContext(ctx,
		`SELECT prune_csn FROM index_seq WHERE id = 0`,
	).Scan(&prune); err != nil {
		return index.Delta{}, fmt.Errorf("sqlite: Since: read prune: %w", err)
	}
	d.Gapped = uint64(cursor) < prune
	return d, nil
}

// Wait blocks until Token moves past `after`, returning the new Token, or
// returns ctx.Err() on cancellation (ADR-106). It polls — push is not the
// source of truth on SQLite; a consumer that misses a wake catches up via
// Since on the next pull.
func (i *Index) Wait(ctx context.Context, after index.Token) (index.Token, error) {
	// Fast path: already moved past `after`, no sleep.
	if cur, err := i.Token(ctx); err != nil || cur > after {
		return cur, err
	}
	t := time.NewTicker(waitPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-t.C:
			cur, err := i.Token(ctx)
			if err != nil {
				return 0, err
			}
			if cur > after {
				return cur, nil
			}
		}
	}
}

var (
	_ index.SyncSource = (*Index)(nil)
	_ index.SyncWaiter = (*Index)(nil)
)

// ManifestByDigest reconstructs the full manifest carrying digest, reusing the
// exact projection and row scan IterateManifests walks with — so a resolved
// manifest is byte-identical to a walked one (ADR-107 ManifestResolver). ok is
// false when no user manifest carries the digest: pruned between a Since read
// and this resolve, which the caller treats as nothing to apply.
func (i *Index) ManifestByDigest(ctx context.Context, digest domain.ManifestDigest) (domain.Manifest, bool, error) {
	const query = `
			SELECT ` + manifestProjection + `
			FROM manifests m
			LEFT JOIN blobs b ON b.blob_ref = m.blob_ref
			WHERE m.manifest_digest = ? AND m.artifact_id IS NOT NULL
			LIMIT 1`

	rows, err := i.db.QueryContext(ctx, query, hexKey(digest))
	if err != nil {
		return domain.Manifest{}, false, classifyError(err)
	}
	defer rows.Close()

	var (
		found domain.Manifest
		ok    bool
	)
	if err := iterateManifestRows(ctx, rows, func(m domain.Manifest) error {
		found, ok = m, true
		return nil
	}); err != nil {
		return domain.Manifest{}, false, err
	}
	return found, ok, nil
}

var _ index.ManifestResolver = (*Index)(nil)
