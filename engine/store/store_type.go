package store

import (
	"log/slog"
	"sync"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/pipeline"
	"scrinium.dev/engine/store/internal/crypto"
	"scrinium.dev/engine/systemstore"
	"scrinium.dev/event"
)

// core holds the entire internal state of a Store. It is never
// exported and never returned directly: clients receive a Store
// interface from InitStore / OpenStore, implemented by the *store
// wrapper below, whose three facets (data/admin/system) all share one
// *core. Splitting the surface into facets keeps the artifact-facing
// methods (dataFacet) free of the administrative ones (adminFacet)
// while both operate on the same underlying state.
//
// Behaviour is split across sibling files by the facet each serves —
// data_*, admin_*, system_* — plus the lifecycle_/bootstrap_
// construction files. The private helpers (enterRead/Write/Admin,
// withWriteDEK, loadManifest, snapshotConfig, publish, …) stay on
// *core, reachable by every facet.
//
// Concurrency. state is guarded by stateMu; most data-path methods
// read it once at entry and proceed lock-free, so a long Put never
// blocks an admin state transition. Mutating admin methods hold the
// write lock for the transition only.
//
// Lock ordering — stateMu and cfgMu are each taken in isolation and
// released before returning, so they are never nested. The crypto
// material has its own mutex inside crypto.State (engine/store/internal/
// crypto); its methods take and release it entirely on their own, and the
// store never holds stateMu or cfgMu while calling into crypto.State, so
// that mutex is never nested with these two either.
type store struct {
	// Identity and dependencies.
	storeID string
	drv     driver.Driver
	index   index.StoreIndex
	pub     event.Publisher

	// log is the diagnostic logger, namespaced to the "scrinium" group
	// at construction (ADR-60). Never read directly — go through
	// logger() / componentLogger(), which substitute a discard logger
	// when nil so call sites never need a guard.
	log *slog.Logger

	// logStore is the "store"-component sublogger, derived from log once at
	// construction. componentLogger("store") — the hot trace path — returns
	// it instead of re-deriving the With attribute on every call.
	logStore *slog.Logger

	// activeConfig is the StoreConfig in effect for new operations,
	// replaced atomically by UpdateConfig under cfgMu.
	cfgMu        sync.RWMutex
	activeConfig config.StoreConfig

	// State machine, guarded by stateMu.
	stateMu sync.RWMutex
	state   domain.StoreState

	// indexComplete records that the index has been established, in THIS
	// session, as a complete picture of the manifests on the Location:
	// either it opened already populated, or recovery finished (ADR-118).
	// The orphan sweep is gated on it — over an unproven index the sweep
	// would delete the corpus. Deliberately session-scoped: a stored mark
	// would assert something about the index that nothing can verify.
	// Guarded by stateMu.
	indexComplete bool

	// recoverIndex is the recovery procedure supplied by the assembler
	// (store.WithIndexRecovery); allowRecovery is the caller's explicit
	// permission to rebuild an index that was supposed to survive
	// (store.WithAllowIndexRecovery). Both are set at construction and
	// never mutated: the deferred Unlock path needs them as much as open
	// does, and that path has no options in hand.
	recoverIndex  IndexRecoverer
	allowRecovery bool
	// lastConfigSeq is the store.config version this instance's
	// activeConfig was loaded from. Owned by the open path, UpdateConfig
	// and the liveness tick's freshness check (ADR-110, INV-110-7);
	// guarded by cfgMu.
	lastConfigSeq uint64

	// sessionOverlay holds the connection's class-III overrides
	// (ADR-110): populated at OpenStore from WithConfig, merged over
	// the active defaults by sessionConfig(). Immutable after open —
	// no lock needed. Zero for a connection without overrides.
	sessionOverlay config.StoreConfig

	// Liveness sentinel (ADR-111): offlineBySentinel marks an Offline
	// set by the probe (self-healable), substituted makes it sticky
	// (foreign store_id at our path — reopen only). Guarded by stateMu.
	offlineBySentinel bool
	substituted       bool
	maintenance       domain.MaintenanceMode
	closed            bool

	// livenessStop/livenessOnce own the sentinel goroutine lifecycle
	// (ADR-111); nil livenessStop = sentinel never started.
	livenessStop chan struct{}
	livenessOnce sync.Once

	// Plugin registries — set at construction, never mutated after.
	hashes       domain.HashRegistry
	transformers pipeline.TransformerRegistry

	// systemstore.Store facade, wired once at construction. nil only in
	// unit tests that build a *core by hand.
	system systemstore.Store

	// crypto holds the DEK, descriptor, passphrase provider, and key
	// resolver behind its own mutex (engine/store/internal/crypto). Its
	// lifecycle methods take that mutex internally and release it before
	// the store touches stateMu, so the two are never held at once.
	crypto *crypto.State
}

var _ Store = (*store)(nil)

// publish emits an event when a Publisher is configured. Cheap when
// nil — the common case for tests and minimal-stack hosts.
func (s *store) publish(typ string, payload any) {
	if s.pub == nil {
		return
	}
	s.pub.Publish(event.Event{Type: typ, Payload: payload})
}
