package sqlite

// ResetIndex: the content of every owned table goes, the schema stays, and the
// blobs table is the one thing a caller may keep — with its ref_counts zeroed,
// which is the whole point of keeping it.

import (
	"context"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
)

// resetFixture fills every table ResetIndex touches, so a test asserting
// emptiness afterwards is asserting something. Rows go in through the glass-box
// helpers and direct SQL rather than IndexManifest: the point here is which
// tables get cleared, not how they were filled.
func resetFixture(t *testing.T, idx *Index) {
	t.Helper()
	ctx := context.Background()

	insertBlob(t, idx, "aabbccdd", "sha256-aabbccdd", 128,
		domain.PhysicalAddress{Path: "blobs/aa/bb/aabbccdd"}, 2)
	insertManifest(t, idx, domain.Manifest{
		Digest:       "sha256-1111111111",
		ArtifactID:   "artifact-1",
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutInline},
	})

	exec := func(stmt string, args ...any) {
		t.Helper()
		if _, err := idx.db.ExecContext(ctx, stmt, args...); err != nil {
			t.Fatalf("resetFixture: %s: %v", stmt, err)
		}
	}
	exec(`INSERT INTO manifest_blobs (manifest_digest, blob_ref, position) VALUES (?, ?, 0)`,
		"sha256-1111111111", "aabbccdd")
	exec(`INSERT INTO manifest_handles (manifest_digest, handle_ref, position) VALUES (?, ?, 0)`,
		"sha256-1111111111", "artifact-0")
	exec(`INSERT INTO proj_ext (manifest_digest, ext_name, field, value) VALUES (?, 'librarium', 'kind', 'origin')`,
		"sha256-1111111111")
	exec(`INSERT INTO proj_usr (manifest_digest, field, value_text) VALUES (?, 'title', 'a book')`,
		"sha256-1111111111")
	exec(`INSERT INTO ext_data (extension, table_name, key, value) VALUES ('provenance', 'edges', 'k', X'00')`)
	exec(`INSERT INTO ext_meta (extension, schema_version, registered_at) VALUES ('provenance', 1, '2026-07-25T00:00:00Z')`)
}

// emptiedTables are every table ResetIndex is expected to leave with no rows
// when it is not keeping blobs. index_seq and schema_version are excluded —
// they are counters and structure, checked separately below.
var emptiedTables = []string{
	"blobs", "manifests", "manifest_blobs", "manifest_handles",
	"proj_ext", "proj_usr", "ext_data", "ext_meta",
}

func TestResetIndex_EmptiesOwnedTables(t *testing.T) {
	ctx := context.Background()
	idx := newMemoryIndex(t)
	resetFixture(t, idx)

	// Guard the fixture itself: a reset that "empties" already-empty tables
	// would pass every assertion below and prove nothing.
	for _, table := range emptiedTables {
		if n := countRows(t, idx, table); n == 0 {
			t.Fatalf("fixture left %s empty — the assertions below would be vacuous", table)
		}
	}

	if err := idx.ResetIndex(ctx, index.ResetOptions{}); err != nil {
		t.Fatalf("ResetIndex: %v", err)
	}

	for _, table := range emptiedTables {
		if n := countRows(t, idx, table); n != 0 {
			t.Errorf("%s after reset: %d rows, want 0", table, n)
		}
	}

	// The schema is structure, not content: it must survive, or the next Open
	// re-runs migrations against a database that is already at this version.
	v, err := idx.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion after reset: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version after reset = %d, want %d", v, CurrentSchemaVersion)
	}

	// The change-sequence restarts: a reset store has no history to be
	// monotonic about.
	tok, err := idx.Token(ctx)
	if err != nil {
		t.Fatalf("Token after reset: %v", err)
	}
	if tok != 0 {
		t.Errorf("Token after reset = %d, want 0", tok)
	}
}

// A kept blobs table is the non-purging re-init's whole basis: the rows stay so
// the files stay resolvable, and the ref_counts drop to zero so GC can reclaim
// them.
func TestResetIndex_KeepBlobs_KeepsRowsAndZeroesRefCounts(t *testing.T) {
	ctx := context.Background()
	idx := newMemoryIndex(t)
	resetFixture(t, idx)

	if err := idx.ResetIndex(ctx, index.ResetOptions{KeepBlobs: true}); err != nil {
		t.Fatalf("ResetIndex: %v", err)
	}

	if n := countRows(t, idx, "blobs"); n != 1 {
		t.Fatalf("blobs after reset with KeepBlobs: %d rows, want 1", n)
	}
	rc, err := idx.GetRefCount(ctx, "aabbccdd")
	if err != nil {
		t.Fatalf("GetRefCount: %v", err)
	}
	if rc != 0 {
		t.Errorf("ref_count after reset = %d, want 0 (an unreclaimable survivor otherwise)", rc)
	}

	// A kept blob must still RESOLVE, or the Orphan Scan during the bootstrap
	// that follows a re-init deletes the file it was kept for.
	if _, err := idx.Resolve(ctx, "aabbccdd"); err != nil {
		t.Errorf("Resolve of a kept blob: %v", err)
	}

	// Everything else goes regardless.
	for _, table := range []string{"manifests", "manifest_blobs", "proj_ext", "ext_data"} {
		if n := countRows(t, idx, table); n != 0 {
			t.Errorf("%s after reset with KeepBlobs: %d rows, want 0", table, n)
		}
	}
}

// Reset is idempotent, and an empty index is a legitimate starting point: a
// crash between the reset and the rest of a re-init leaves exactly this state,
// and the retry must not care.
func TestResetIndex_OnEmptyIndex_IsNoOp(t *testing.T) {
	ctx := context.Background()
	idx := newMemoryIndex(t)

	for range 2 {
		if err := idx.ResetIndex(ctx, index.ResetOptions{}); err != nil {
			t.Fatalf("ResetIndex on an empty index: %v", err)
		}
	}
	for _, table := range emptiedTables {
		if n := countRows(t, idx, table); n != 0 {
			t.Errorf("%s: %d rows, want 0", table, n)
		}
	}
}
