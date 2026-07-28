package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"scrinium.dev/errs"
	"scrinium.dev/testutil/manifestfx"
)

// TestClassifyError_BusyMaps in errors_test.go covers the unit
// shape of classifyError on a synthetic "database is locked" string.
// That alone is fragile — a SQLite driver upgrade that changes the
// error message text (e.g. "database is locked" → "SQLITE_BUSY:
// database is busy") would silently break the busy detection in
// production while keeping the unit test green.
//
// This integration test runs classifyError through the actual
// SQLite driver path: two concurrent connections to the same
// on-disk database, with one holding an exclusive transaction
// while the other tries to write. The driver-emitted error for
// that condition must round-trip through classifyError into
// errs.ErrLeaseHeld.
//
// Critical safety net before M3.1 (lease subsystem), where
// errs.ErrLeaseHeld is the canonical "back off and retry" signal
// that callers branch on. If this signal stops firing, every
// lease consumer goes blind without warning.
//
// File-backed (not :memory:) because each :memory: handle is a
// separate database — concurrent connections cannot collide there.
//
// Build-tag agnostic: works under both modernc.org/sqlite (default)
// and mattn/go-sqlite3 (sqlite_cgo). The driver-specific
// behaviour we test is "produces a busy-shaped error under contention",
// which both drivers satisfy.
func TestClassifyError_RealBusyContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "busy.db")

	// Connection 1: opens the database, will hold an exclusive
	// write transaction throughout the test.
	idx1, err := newStoreForTests(ctx, dbPath, nil)
	if err != nil {
		t.Fatalf("connection 1: NewStore: %v", err)
	}
	defer idx1.Close()

	// Acquire a dedicated *sql.Conn so we can pin a transaction
	// to one underlying SQLite connection. The default *sql.DB
	// pool may otherwise reuse connections across BeginTx calls.
	conn1, err := idx1.db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn 1: %v", err)
	}
	defer conn1.Close()

	// Begin an immediate transaction and write something. The
	// write promotes the implicit transaction to a RESERVED /
	// EXCLUSIVE state — concurrent writers from other handles
	// will hit SQLITE_BUSY once busy_timeout expires.
	tx, err := conn1.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx on conn 1: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO schema_version (version, applied_at) VALUES (9999, 'busy-test')",
	); err != nil {
		t.Fatalf("acquire write lock on conn 1: %v", err)
	}
	// Hold the lock until the end of the test by deferring the
	// rollback. Connection 2 must observe BUSY before connection
	// 1 releases the lock.
	defer func() { _ = tx.Rollback() }()

	// Connection 2: opens the same database file via a fresh
	// *sql.DB. Drop busy_timeout to 50ms so the test does not
	// stall on the SQLite default (varies by driver and could
	// be many seconds).
	idx2, err := newStoreForTests(ctx, dbPath, nil)
	if err != nil {
		t.Fatalf("connection 2: NewStore: %v", err)
	}
	defer idx2.Close()

	if _, err := idx2.db.ExecContext(ctx, "PRAGMA busy_timeout = 50"); err != nil {
		t.Fatalf("set busy_timeout on conn 2: %v", err)
	}

	// IndexManifest routes every write through observe → classifyError. With
	// connection 1 holding the write lock, this write on conn 2 must hit
	// SQLITE_BUSY and emerge as errs.ErrLeaseHeld.
	err = idx2.IndexManifest(ctx, manifestfx.Blob("busy-art", "b105ff01"), manifestfx.PhysAddr("blobs/bb/uy/busy-blob"))
	if err == nil {
		t.Fatal("IndexManifest on locked db: expected busy error, got nil")
	}
	if !errors.Is(err, errs.ErrLeaseHeld) {
		t.Errorf("IndexManifest busy error: errors.Is(err, errs.ErrLeaseHeld) = false; "+
			"got %v (type %T)", err, err)
	}
}

// TestClassifyError_NonBusyErrorPassesThrough is the negative
// counterpart: a non-busy real driver error (e.g. SQL syntax
// error) must NOT be misclassified as ErrLeaseHeld.
//
// Without this guard, classifyError could become greedy and
// over-match — turning every database error into "lease lost",
// which would then cause M3.1 callers to retry indefinitely on
// real, non-transient bugs.
func TestClassifyError_NonBusyErrorPassesThrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	idx, err := newStoreForTests(ctx, ":memory:", nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer idx.Close()

	// Trigger a syntax error directly on the underlying *sql.DB
	// so we observe the raw driver error before any wrapping.
	// The driver emits a "syntax error" / "near 'BANANA'" style
	// message; classifyError must NOT wrap that into ErrLeaseHeld.
	_, err = idx.db.ExecContext(ctx, "BANANA POTATO")
	if err == nil {
		t.Fatal("expected syntax error, got nil")
	}
	if errors.Is(err, errs.ErrLeaseHeld) {
		t.Errorf("syntax error misclassified as ErrLeaseHeld: %v", err)
	}
	// classifyError applied directly should also pass through.
	classified := classifyError(err)
	if errors.Is(classified, errs.ErrLeaseHeld) {
		t.Errorf("classifyError on syntax error returned ErrLeaseHeld: %v",
			classified)
	}
}
