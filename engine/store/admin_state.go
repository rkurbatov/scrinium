package store

import (
	"context"
	"fmt"
	"log/slog"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/systemstore"
)

// State returns the current state of the Store. Cheap and
// lock-free for readers (RWMutex read).
func (s *store) State() domain.StoreState { return s.currentState() }

// MaintenanceMode returns the current maintenance mode. Like State,
// it is reachable in every state including Offline — surfaces that
// serve data from the index/projection past the store gates (webview
// listings) consult it per-request (ADR-111, INV-111-5).
func (s *store) MaintenanceMode() domain.MaintenanceMode { return s.maintenanceMode() }

// Capabilities returns the underlying Driver's capability mask.
// Stable for the lifetime of the Store; not cached because the
// Driver is the source of truth and a future Driver may want to
// change its mask after a runtime probe.
func (s *store) Capabilities() driver.CapabilityMask {
	return s.drv.Capabilities()
}

// SetMaintenanceMode transitions the Store into the requested
// maintenance regime; any → any is allowed.
//
// A transition into MaintenanceModeOffline blocks all subsequent
// operations except SetMaintenanceMode itself (back to None or
// ReadOnly). That escape hatch is not enforced through a state-machine
// matrix here; the priority-of-checks in each operation's entry point
// covers it (every method checks ErrStoreOffline at its boundary).
//
// Idempotent: setting the current mode again is a no-op success.
func (s *store) SetMaintenanceMode(ctx context.Context, mode domain.MaintenanceMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch mode {
	case domain.MaintenanceModeNone, domain.MaintenanceModeReadOnly, domain.MaintenanceModeOffline:
		// OK
	default:
		return fmt.Errorf("store.SetMaintenanceMode: invalid mode %d", mode)
	}

	s.stateMu.Lock()
	s.maintenance = mode
	s.stateMu.Unlock()

	// No event is emitted yet: a log-only signal would create surprise
	// state for the host, so deliberate silence is the safer default
	// until a proper MaintenanceModeChanged event exists. A Debug log is
	// not a host-visible event — safe to record the transition for
	// diagnostics. Lock-free: stateMu released above.
	s.componentLogger("store").LogAttrs(ctx, slog.LevelDebug, "maintenance mode set",
		storeIDAttr(s), maintenanceModeAttr(mode))
	return nil
}

// System returns the systemstore.Store facade. Reached only through
// AdminStore, so DataStore consumers cannot see system state.
func (s *store) System() systemstore.Store { return s.system }

// IndexComplete reports whether the index has been established, in this
// session, as a complete picture of the manifests on the Location (ADR-118).
//
// It is not on the Store facade on purpose: the only callers are the passes
// that delete by absence — the GC agent's media reconciliation — and for
// them "the index does not know this" is only evidence once completeness is
// proven. Everyone else has no business asking.
func (s *store) IndexComplete() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.indexComplete
}
