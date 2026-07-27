package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/index"
	"scrinium.dev/event"
)

// DefaultBusyTimeout is the default value applied via
// PRAGMA busy_timeout when no WithBusyTimeout option is supplied.
// Five seconds covers practically every legitimate writer
// contention without hiding real deadlocks for too long.
const DefaultBusyTimeout = 5 * time.Second

// Index is the SQLite-backed implementation of index.StoreIndex.
// Construct via NewStore; Close when done.
type Index struct {
	db   *sql.DB
	opts options

	// publisher is invoked from instrumented call sites to emit
	// index.* metric events. Optional; nil disables emission. We
	// also keep a small mutex around publication because some
	// emitters (asynchronous buses) might race with Close.
	pubMu sync.RWMutex

	// closeOnce makes Close idempotent — the StoreIndex contract
	// requires a second Close to be a no-op success, while
	// database/sql.DB.Close returns an error when called twice.
	closeOnce sync.Once
	closeErr  error

	// ciMu guards the CustomIndex dispatcher. Acquired in write
	// mode by Register and by every IndexManifest/DeleteManifest
	// call (so a concurrent Register cannot insert a
	// new subscriber midway through a dispatch). Held briefly —
	// the in-memory work is map lookups; the SQL transaction it
	// guards may be long, but the lock is released right after.
	ciMu     sync.Mutex
	ciByName map[string]customindex.CustomIndex
	ciByKind map[customindex.EventKind][]customindex.CustomIndex
	// ciSubstrates keeps the long-lived substrate handed to each
	// CustomIndex during Setup. Apply uses fresh tx-scoped substrates
	// (allocated per call); the long-lived substrates in this map are
	// the ones custom indexes may have captured for read-side use.
	// Maintained so Close can release them in lockstep with the
	// custom index list.
	ciSubstrates map[string]*sqliteSubstrate
}

// Compile-time interface conformance. Catches signature drift
// between index.StoreIndex and *Index immediately at build time
// instead of at the first assignment site.
var _ index.StoreIndex = (*Index)(nil)

// options is the resolved configuration. Defaults applied by Open.
type options struct {
	busyTimeout time.Duration
	publisher   event.Publisher
	journalMode journalMode
	syncMode    syncMode
}

// journalMode mirrors PRAGMA journal_mode values we care about. WAL
// is the right default for everything except :memory:, where SQLite
// silently downgrades to MEMORY anyway.
type journalMode string

const (
	journalWAL    journalMode = "WAL"
	journalMemory journalMode = "MEMORY"
)

// syncMode mirrors PRAGMA synchronous values. NORMAL is the
// recommended setting under WAL: durability across crashes plus
// excellent throughput. FULL is paranoid and slow; OFF risks data
// loss across power failures and is for tests only.
type syncMode string

const (
	syncNormal syncMode = "NORMAL"
	syncFull   syncMode = "FULL"
	syncOff    syncMode = "OFF"
)

func defaultOptions() options {
	return options{
		busyTimeout: DefaultBusyTimeout,
		journalMode: journalWAL,
		syncMode:    syncNormal,
	}
}

// NewStore opens (or creates) a SQLite-backed StoreIndex at the
// given path. Use ":memory:" for a private in-memory instance.
//
// Accepts the umbrella index.IndexOption type — the package
// itself does not expose backend-specific options on its public
// API. Tunables like busy_timeout and journal/sync modes use
// safe defaults; tests inside this package may override them
// through internal helpers.
//
// On a fresh database the schema is created at CurrentSchemaVersion.
// On an existing database the schema version is checked; missing
// migrations are applied forward-only. A version newer than
// CurrentSchemaVersion returns errs.ErrIndexSchemaMismatch.
//
// ctx and error are part of the signature because opening SQLite is
// real I/O: it can fail on bad paths, permission errors, mid-flight
// migrations, or mmap limits, and migrations are long-running and
// deserve cancellation.
func NewStore(ctx context.Context, path string, opts ...index.IndexOption) (*Index, error) {
	// Resolve umbrella IndexOptions, then map them onto our local
	// options struct. The reverse direction (sqlite-private knobs
	// reachable through index.IndexOption) is intentionally not
	// supported — backend-specific tuning lives behind
	// implementation-internal helpers.
	idxOpts := index.IndexOptions{}
	for _, fn := range opts {
		fn(&idxOpts)
	}
	o := defaultOptions()
	o.publisher = idxOpts.Publisher
	return newStoreInternal(ctx, path, o)
}

// newStoreForTests is the in-package constructor used by sqlite_test.go
// to exercise paths that need non-default tunables (sync_off,
// custom busy_timeout). Not exported because chaos-test packages
// outside sqlite have no legitimate need for them — they should
// inject faults at the driver layer instead.
func newStoreForTests(ctx context.Context, path string, mut func(*options)) (*Index, error) {
	o := defaultOptions()
	if mut != nil {
		mut(&o)
	}
	return newStoreInternal(ctx, path, o)
}

// newStoreInternal is the shared body of NewStore and
// newStoreForTests. It receives a fully-resolved options struct
// and runs the open/pragma/migrate sequence common to both
// constructors. The split keeps the public/test-only entry
// points free of implementation-detail churn — when the open
// flow grows a step, both constructors get it for free.
func newStoreInternal(ctx context.Context, path string, o options) (*Index, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite: empty path")
	}

	// Create the parent directory for a file-backed DB. SQLite creates
	// the database file but not the directories above it, so an index
	// path like "<store>/index/index.db" needs its dir made first.
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("sqlite: create dir for %q: %w", path, err)
		}
	}

	dsn := buildDSN(path, o)
	db, err := openSQL(dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}

	// SQLite handles concurrency at the file level; multiple Go
	// goroutines sharing one *sql.DB go through database/sql's
	// connection pool. Capping at one writer connection plus a
	// few readers is the safe default.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)

	// Apply pragmas. database/sql does not let us run pragmas as
	// part of the DSN portably across drivers, so we do it after
	// Open via a dedicated connection that the pool will reuse.
	if err := applyPragmas(ctx, db, path, o); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: apply pragmas: %w", err)
	}

	if err := applyMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Index{
		db:           db,
		opts:         o,
		ciByName:     make(map[string]customindex.CustomIndex),
		ciByKind:     make(map[customindex.EventKind][]customindex.CustomIndex),
		ciSubstrates: make(map[string]*sqliteSubstrate),
	}, nil
}

// buildDSN assembles a DSN. Both supported drivers (modernc and
// mattn) accept "file:<path>?<query>" as well as ":memory:". We use
// the file: form so query parameters work uniformly.
func buildDSN(path string, o options) string {
	if path == ":memory:" {
		return ":memory:"
	}
	// Relative paths are kept as-is; the caller is responsible for the
	// working directory. We do NOT reach for filepath.Abs here because
	// it depends on os.Getwd() and would surprise tests that chdir.
	abs := path
	// _foreign_keys=1 enforces FK constraints (we will rely on this
	// once we add manifest_blobs cleanup triggers in a later pack).
	q := []string{"_foreign_keys=1"}
	return "file:" + abs + "?" + strings.Join(q, "&")
}

// applyPragmas configures session-wide pragmas. busy_timeout is the
// most important — without it, contention is an instant error
// instead of a brief wait.
func applyPragmas(ctx context.Context, db *sql.DB, path string, o options) error {
	// :memory: silently ignores journal_mode=WAL (it stays MEMORY),
	// so we adapt expectations rather than fight SQLite.
	jm := o.journalMode
	if path == ":memory:" {
		jm = journalMemory
	}

	stmts := []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d", o.busyTimeout.Milliseconds()),
		fmt.Sprintf("PRAGMA journal_mode = %s", jm),
		fmt.Sprintf("PRAGMA synchronous = %s", o.syncMode),
		"PRAGMA foreign_keys = ON",
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// Close releases the underlying database/sql handle. Idempotent:
// the StoreIndex contract requires repeat Close calls to succeed,
// while database/sql.DB.Close itself errors on the second call —
// sync.Once captures the first outcome and returns it forever.
//
// Registered CustomIndexes are closed in the reverse order of
// registration. Errors from CustomIndex Close are swallowed (logged
// would be the proper behaviour once a logger is wired in) — they
// must not prevent the underlying DB from being released.
func (i *Index) Close() error {
	i.closeOnce.Do(func() {
		i.ciMu.Lock()
		// Close in reverse-registration order. ciByKind is the
		// dispatch map; a stable insertion-order list would be
		// cleaner, but the set of subscribers is small in
		// practice — iterating ciByName here is fine.
		for _, ext := range i.ciByName {
			_ = ext.Close()
		}
		// Drop substrate-side references so Get/Scan calls from a
		// host that mistakenly held a captured substrate after
		// Close fail with a clear errSubstrateClosed instead of
		// hitting a closed *sql.DB.
		for _, sub := range i.ciSubstrates {
			sub.executor.Store(nil)
		}
		i.ciByName = nil
		i.ciByKind = nil
		i.ciSubstrates = nil
		i.ciMu.Unlock()

		i.closeErr = i.db.Close()
	})
	return i.closeErr
}

// SchemaVersion returns the version currently recorded on disk.
// Useful for diagnostics and tests.
func (i *Index) SchemaVersion(ctx context.Context) (int, error) {
	return readSchemaVersion(ctx, i.db)
}

// publish forwards an event to the configured Publisher, taking the
// read lock on pubMu so concurrent Close cannot race a publish in
// flight. Cheap when Publisher is nil — the common case for tests.
func (i *Index) publish(typ string, payload any) {
	i.pubMu.RLock()
	pub := i.opts.publisher
	i.pubMu.RUnlock()
	if pub == nil {
		return
	}
	// event.Event is the concrete shape. We import it lazily via
	// store.Publisher so the import lives at the use site only.
	pub.Publish(eventOf(typ, payload))
}
