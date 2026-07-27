package rebuild

import (
	"context"
	"fmt"
	"sync"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/checkpointfmt"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// RebuildIndexAgent rebuilds the StoreIndex from manifests. It
// supports a fast path through a recent checkpoint with read-in of
// new objects and a full fallback Location scan. It is also used to
// restore store.json (when lost). Recovering the active StoreConfig
// (the max system/config version) is out of scope — see below.
type RebuildIndexAgent interface {
	agent.Agent

	// Stats returns a progress snapshot during execution (safe to
	// call from another goroutine). After Run, returns the final
	// statistics.
	Stats() RebuildStats
}

// NewRebuildIndexAgent creates a RebuildIndexAgent. Constructed by the
// assembler (Variant B) with the raw Driver and StoreIndex: the rebuild
// reads manifest files straight off the Location through the Driver
// (the index is exactly what is being rebuilt, so it cannot be the
// source) and writes the reconstructed rows through the StoreIndex.
// hostID owns the maintenance lease; storeID tags events.
//
// Scope on M3: the FullScan path reconstructs Blob manifests (Inline
// and Target) — the only manifest types that exist before M4 (Pack) and
// M5 (TOC/chunking). Encrypted manifests are decoded with the Store's own
// key material, obtained at run time (store.ManifestKeyProvider); an
// unencrypted Store has none and the scan stays Plain-only. The checkpoint
// fast-path (restoring a checkpoint produced by the checkpoint agent, then
// reading in the tail) is not yet wired into rebuild. Descriptor recovery
// from the Recovery Kit (rewriting a
// lost store.json) is implemented and runs before the scan when
// RecoveryKit is set; recovering the active StoreConfig (max system/config
// seq) is still out of scope (the kit carries no config). The remaining gaps
// are reported as explicit, non-silent boundaries rather than faked.
func NewRebuildIndexAgent(
	st store.Store,
	drv driver.Driver,
	idx index.StoreIndex,
	bus event.Publisher,
	hostID string,
	storeID string,
	cfg RebuildConfig,
	opts ...agent.AgentOption,
) (RebuildIndexAgent, error) {
	if st == nil || drv == nil || idx == nil || bus == nil {
		return nil, fmt.Errorf("rebuild.NewRebuildIndexAgent: store, driver, index and bus are required")
	}
	if hostID == "" {
		return nil, fmt.Errorf("rebuild.NewRebuildIndexAgent: hostID is required for the maintenance lease")
	}
	cfg = applyRebuildDefaults(cfg)
	return &rebuildAgent{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		store:     st, drv: drv, idx: idx, bus: bus,
		hostID: hostID, storeID: storeID, cfg: cfg,
	}, nil
}

// rebuildAgent is the concrete RebuildIndexAgent.
type rebuildAgent struct {
	store   store.Store
	drv     driver.Driver
	idx     index.StoreIndex
	bus     event.Publisher
	hostID  string
	storeID string
	cfg     RebuildConfig

	mu    sync.Mutex
	stats RebuildStats

	agent.BaseState
}

var _ RebuildIndexAgent = (*rebuildAgent)(nil)

// Validate checks the operation is applicable without doing irreversible
// work. A Checkpoint-source request needs an index that can restore a
// checkpoint and at least one checkpoint to exist; otherwise it returns
// ErrNoCheckpoint. The maintenance-mode gate is enforced by the Store's
// RunMaintenance entry point (the sanctioned caller), not here — the
// current mode is not exposed on the read surface.
func (a *rebuildAgent) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.cfg.Source == RebuildSourceCheckpoint {
		if _, ok := a.idx.(index.CheckpointRestorer); !ok {
			return fmt.Errorf("rebuild.Rebuild.Validate: Source=Checkpoint: index cannot restore checkpoints: %w", errs.ErrNoCheckpoint)
		}
		_, _, ok, err := checkpointfmt.Latest(ctx, a.store.System())
		if err != nil {
			return fmt.Errorf("rebuild.Rebuild.Validate: Source=Checkpoint: %w", err)
		}
		if !ok {
			return fmt.Errorf("rebuild.Rebuild.Validate: Source=Checkpoint: %w", errs.ErrNoCheckpoint)
		}
	}
	return nil
}

// AgentType is the short registry/event identifier.
func (a *rebuildAgent) AgentType() string { return "rebuild" }

// Run is the contract entry point: it rebuilds the index under the
// standard maintenance lifecycle (agent.RunLeased, ADR-94 — lease,
// events, state).
func (a *rebuildAgent) Run(ctx context.Context) (*domain.AgentResult, error) {
	return agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), a.rebuildCore)
}
