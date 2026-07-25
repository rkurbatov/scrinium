package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"scrinium.dev/engine/index"
)

// Compile-time conformance for the optional capability. Without it a signature
// drift against index.Resetter would surface only as a failed type assertion
// inside InitStore — at which point a forced re-init refuses for a reason that
// looks like configuration and is actually a build problem.
var _ index.Resetter = (*Index)(nil)

// resetStatements is the order in which the tables are emptied: edges and
// projections first, then the rows they hang off. SQLite declares no foreign
// keys here, so the order is not load-bearing for the database — it is
// load-bearing for the reader, who should be able to see that nothing is left
// pointing at nothing halfway through.
//
// schema_version is deliberately absent. This empties content; the schema is
// structure, it is already at CurrentSchemaVersion, and dropping its row would
// send the next Open through the migrations again for no reason.
var resetStatements = []string{
	`DELETE FROM proj_ext`,
	`DELETE FROM proj_usr`,
	`DELETE FROM manifest_blobs`,
	`DELETE FROM manifest_handles`,
	`DELETE FROM manifests`,
	// ext_data is every extension's rows — provenance edges, path entries,
	// pack placement. ext_meta is the schema version each extension persisted
	// at Setup. Both go, and they go TOGETHER: an extension that finds its
	// recorded version without its data would decide it has nothing to migrate
	// and be wrong about an empty table. With neither, the next Setup starts
	// from scratch, which is the truth after a re-init.
	`DELETE FROM ext_data`,
	`DELETE FROM ext_meta`,
}

// ResetIndex implements index.Resetter: it empties every table this index owns
// inside one transaction, leaving the schema at its current version.
//
// The change-sequence counter goes back to zero along with the rows. A
// consumer holding a cursor from before the reset is not owed continuity here:
// the store it was following has a new StoreID and is a different store, so
// there is nothing for a monotonic counter to be monotonic about. Restarting at
// zero is the honest statement that this index has no history yet.
func (i *Index) ResetIndex(ctx context.Context, opts index.ResetOptions) error {
	return i.observe("ResetIndex", func() error {
		return i.inTx(ctx, func(tx *sql.Tx) error {
			for _, stmt := range resetStatements {
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("sqlite: ResetIndex: %s: %w", stmt, err)
				}
			}

			// The blobs table is the one thing a caller may keep. See
			// index.ResetOptions.KeepBlobs for why keeping the ROWS is what
			// makes keeping the FILES meaningful.
			blobs := `DELETE FROM blobs`
			if opts.KeepBlobs {
				blobs = `UPDATE blobs SET ref_count = 0`
			}
			if _, err := tx.ExecContext(ctx, blobs); err != nil {
				return fmt.Errorf("sqlite: ResetIndex: %s: %w", blobs, err)
			}

			if _, err := tx.ExecContext(ctx,
				`UPDATE index_seq SET csn = 0, prune_csn = 0 WHERE id = ?`, csnRowID,
			); err != nil {
				return fmt.Errorf("sqlite: ResetIndex: reset change-sequence: %w", err)
			}
			return nil
		})
	})
}
