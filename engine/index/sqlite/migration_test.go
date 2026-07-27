package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"scrinium.dev/engine/internal/timefmt"
	"scrinium.dev/errs"
)

// objectExists reports whether a table/index of the given name exists.
func objectExists(t *testing.T, idx *Index, typ, name string) bool {
	t.Helper()
	var n int
	err := idx.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type=? AND name=?`, typ, name).Scan(&n)
	if err != nil {
		t.Fatalf("probe %s %q: %v", typ, name, err)
	}
	return n > 0
}

// --- Conformance: the collapsed v1 baseline produces the full schema ---

func TestMigrate_BaselineTables(t *testing.T) {
	idx := newMemoryIndex(t)
	want := []string{
		"blobs", "manifests", "manifest_blobs",
		"ext_meta", "ext_data", "proj_ext", "schema_version",
	}
	for _, tbl := range want {
		if !objectExists(t, idx, "table", tbl) {
			t.Errorf("baseline missing table %q", tbl)
		}
	}
}

func TestMigrate_BaselineIndexes(t *testing.T) {
	idx := newMemoryIndex(t)
	want := []string{
		"blobs_content", "blobs_orphan", "blobs_scrub",
		"manifests_session", "manifests_scrub",
		"manifest_blobs_blob",
		"proj_ext_lookup",
	}
	for _, ix := range want {
		if !objectExists(t, idx, "index", ix) {
			t.Errorf("baseline missing index %q", ix)
		}
	}
}

func TestMigrate_BaselineRecordsSingleVersionRow(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	v, err := idx.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", v, CurrentSchemaVersion)
	}

	var rows int
	if err := idx.db.QueryRowContext(ctx, `SELECT count(*) FROM schema_version`).Scan(&rows); err != nil {
		t.Fatalf("count schema_version: %v", err)
	}
	if rows != 1 {
		t.Errorf("schema_version rows = %d, want 1 (baseline is one migration)", rows)
	}
}

// --- Mechanism: readSchemaVersion on a fresh, unmigrated database ---

func TestMigrate_ReadSchemaVersion_FreshIsZero(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()

	v, err := readSchemaVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("readSchemaVersion: %v", err)
	}
	if v != 0 {
		t.Errorf("fresh db version = %d, want 0", v)
	}
}

// --- Mechanism: a forward migration applies and records its version ---

func TestMigrate_ApplyForwardMigration(t *testing.T) {
	idx, _ := newDiskIndex(t)
	ctx := context.Background()

	m := migration{
		Version:     CurrentSchemaVersion + 1,
		Description: "test forward migration",
		Statements:  []string{"CREATE TABLE mig_probe (x INTEGER)"},
	}
	if err := applyMigration(ctx, idx.db, m); err != nil {
		t.Fatalf("applyMigration: %v", err)
	}

	if !objectExists(t, idx, "table", "mig_probe") {
		t.Error("forward migration did not create mig_probe")
	}
	v, err := readSchemaVersion(ctx, idx.db)
	if err != nil {
		t.Fatalf("readSchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion+1 {
		t.Errorf("version after forward migration = %d, want %d", v, CurrentSchemaVersion+1)
	}
}

// --- Mechanism: a failing migration rolls back atomically ---

func TestMigrate_FailedMigrationRollsBack(t *testing.T) {
	idx, _ := newDiskIndex(t)
	ctx := context.Background()

	// Statement 1 creates a table; statement 2 fails (duplicate
	// CREATE). The whole migration must roll back: neither the table
	// nor the version row survives.
	m := migration{
		Version:     CurrentSchemaVersion + 1,
		Description: "rollback probe",
		Statements: []string{
			"CREATE TABLE rb_probe (x INTEGER)",
			"CREATE TABLE rb_probe (x INTEGER)",
		},
	}
	if err := applyMigration(ctx, idx.db, m); err == nil {
		t.Fatal("applyMigration: expected error from duplicate CREATE")
	}
	if objectExists(t, idx, "table", "rb_probe") {
		t.Error("failed migration left rb_probe behind (no rollback)")
	}
	v, err := readSchemaVersion(ctx, idx.db)
	if err != nil {
		t.Fatalf("readSchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("version after failed migration = %d, want %d (unchanged)", v, CurrentSchemaVersion)
	}
}

// --- Mechanism: reopening an up-to-date database is a no-op ---

func TestMigrate_Reopen_Idempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reopen.db")

	idx1, err := NewStore(ctx, path)
	if err != nil {
		t.Fatalf("NewStore #1: %v", err)
	}
	v1, _ := idx1.SchemaVersion(ctx)
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	idx2, err := NewStore(ctx, path)
	if err != nil {
		t.Fatalf("NewStore #2 (reopen): %v", err)
	}
	defer idx2.Close()
	v2, _ := idx2.SchemaVersion(ctx)
	if v1 != v2 || v2 != CurrentSchemaVersion {
		t.Errorf("versions: first=%d reopen=%d, want both %d", v1, v2, CurrentSchemaVersion)
	}

	var rows int
	if err := idx2.db.QueryRowContext(ctx, `SELECT count(*) FROM schema_version`).Scan(&rows); err != nil {
		t.Fatalf("count schema_version: %v", err)
	}
	if rows != 1 {
		t.Errorf("schema_version rows after reopen = %d, want 1 (no re-apply)", rows)
	}
}

// --- Mechanism: a database newer than the binary is rejected ---

func TestMigrate_ForwardOnly_FutureVersionRejected(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "future.db")

	idx1, err := NewStore(ctx, path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := idx1.db.ExecContext(ctx,
		`INSERT INTO schema_version(version, applied_at) VALUES (?, ?)`,
		CurrentSchemaVersion+99, timefmt.Format(time.Now()),
	); err != nil {
		t.Fatalf("inject future version: %v", err)
	}
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = NewStore(ctx, path)
	if !errors.Is(err, errs.ErrIndexSchemaMismatch) {
		t.Errorf("reopen of future-version db: err = %v, want ErrIndexSchemaMismatch", err)
	}
}
