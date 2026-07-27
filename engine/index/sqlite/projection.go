package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
)

// snapshotIndexers returns the registered custom indexes that implement the
// Indexer capability, copied under the lock so dispatch iterates a stable list
// even if a concurrent Close nils the map (mirrors snapshotSubscribers).
func (i *Index) snapshotIndexers() []customindex.CustomIndex {
	i.ciMu.Lock()
	defer i.ciMu.Unlock()
	if len(i.ciByName) == 0 {
		return nil
	}
	out := make([]customindex.CustomIndex, 0, len(i.ciByName))
	for _, ci := range i.ciByName {
		if _, ok := ci.(customindex.Indexer); ok {
			out = append(out, ci)
		}
	}
	return out
}

// applyIndexers runs every registered Indexer over m in the index-write
// transaction (§9.2.1): each index writes its OWN tables through its Substrate
// and RETURNS Projections the core writes into proj_ext. The core stamps digest
// and ext_name (= Name()) — an index cannot project under another's name
// (Principle 9). Idempotent (INSERT OR REPLACE) so a crash-replay of
// IndexManifest overwrites identically (§9.10).
func (i *Index) applyIndexers(ctx context.Context, tx *sql.Tx, m domain.Manifest) error {
	idxs := i.snapshotIndexers()
	if len(idxs) == 0 {
		return nil
	}
	digest := string(m.Digest)
	for _, ci := range idxs {
		name := ci.Name()
		sub := newSqliteSubstrate(name)
		sub.useTx(tx)
		projs, err := ci.(customindex.Indexer).Index(ctx, sub, m)
		if err != nil {
			return fmt.Errorf("indexer %q index: %w", name, err)
		}
		for _, p := range projs {
			if err := upsertProjExt(ctx, tx, digest, name, p.Field, p.Value); err != nil {
				return fmt.Errorf("indexer %q ext field %q: %w", name, p.Field, err)
			}
		}
	}
	return nil
}

// applyUnindexers runs every registered Indexer's Unindex over the manifest
// being deleted, in the delete transaction (§9.2.1) — the symmetric inverse of
// applyIndexers. Each index removes the rows it wrote to its OWN tables through
// its Substrate. The core has already removed the built-in proj_* rows by digest
// (deleteProjections); this covers the own-table side a digest alone cannot
// reach. The manifest passed carries the indexed identity (ArtifactID/Digest);
// its body (Ext) is not available at delete time, so an Unindex that needs the
// payload recovers it from its own tables (as fspathindex does).
func (i *Index) applyUnindexers(ctx context.Context, tx *sql.Tx, m domain.Manifest) error {
	idxs := i.snapshotIndexers()
	if len(idxs) == 0 {
		return nil
	}
	for _, ci := range idxs {
		name := ci.Name()
		sub := newSqliteSubstrate(name)
		sub.useTx(tx)
		if err := ci.(customindex.Indexer).Unindex(ctx, sub, m); err != nil {
			return fmt.Errorf("indexer %q unindex: %w", name, err)
		}
	}
	return nil
}

// deleteProjections removes every built-in projection row for digest, in the
// delete transaction. The core owns proj_*, so it removes them by digest — the
// symmetric inverse of having written them, and robust to an index toggled off
// since the write (no orphan rows). An index's OWN tables (Substrate, §9.7) are
// cleaned by Unindex (applyUnindexers, wired in deleteManifestTx). proj_ext delete
// needs only the digest, so it is handled here.
func deleteProjections(ctx context.Context, tx *sql.Tx, digest string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM proj_ext WHERE manifest_digest = ?`, digest); err != nil {
		return fmt.Errorf("delete proj_ext: %w", err)
	}
	return nil
}

// --- proj_* row writers ---

func upsertProjExt(ctx context.Context, tx *sql.Tx, digest, extName, field, value string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO proj_ext (manifest_digest, ext_name, field, value) VALUES (?, ?, ?, ?)`,
		digest, extName, field, value)
	return err
}
