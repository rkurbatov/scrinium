package gc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/lease"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// GCConfig configures the GC Agent.
type GCConfig struct {
	// ScanInterval is the interval between Mark+Sweep cycles.
	ScanInterval time.Duration

	// BatchSize is the number of blobs in a single StoreIndex
	// transaction during a scan.
	BatchSize int

	// LeaseTTL is the hold time for gc/lease under
	// GCLeasePolicy: LeaderElection. Renew runs at half the TTL.
	LeaseTTL time.Duration

	// CompactionEnabled toggles compaction of partially dead .pack
	// volumes. The default in v1 is false (a deferred feature).
	CompactionEnabled bool

	// CompactionThreshold — a dead_ratio at or above this value
	// triggers compaction. Default 0.5.
	CompactionThreshold float64

	// MinPackAge — packs younger than this are not compacted.
	// Fresh packs may collapse on their own without paying a
	// repacking cost.
	MinPackAge time.Duration

	// MaxCompactionsPerCycle bounds the I/O load per GC cycle.
	MaxCompactionsPerCycle int
}

// GCStats are the statistics of a single GC cycle.
type GCStats struct {
	ScannedBlobs        int64
	MarkedBlobs         int64
	RemovedBlobs        int64
	FreedBytes          int64
	CompactedPacks      int64
	CompactedFreedBytes int64
}

// GCAgent is the background reaper of orphan blobs governed by
// Two-Phase Deletion. User-managed: the engine does not start the GC
// automatically — the deletion policy is a deployment-specific
// decision.
type GCAgent interface {
	agent.Agent

	// RunNow executes one full Mark+Sweep cycle and returns. Used
	// for manual runs and tests; unlike Run it does not block on
	// ScanInterval.
	RunNow(ctx context.Context) (GCStats, error)
}

// NewGCAgent creates a GC Agent. Constructed by the assembler with the
// raw Driver and StoreIndex alongside store.Store: Mark/Sweep walk
// ListOrphanBlobs on the index and act on tombstone files through the
// driver, while StoreConfig (TombstoneGracePeriod, GCLeasePolicy) is
// read from the Store's Config(). hostID owns the gc lease under
// LeaderElection; storeID tags events.
//
// Per-store by invariant (§5.3.2): the agent only ever touches its own
// Location. Cross-store moves are between two indexes, so GC never
// competes with migration over a blob in one index.
func NewGCAgent(
	st store.Store,
	drv driver.Driver,
	idx gcIndex,
	bus event.Publisher,
	hostID string,
	storeID string,
	cfg GCConfig,
	opts ...agent.AgentOption,
) (GCAgent, error) {
	if st == nil || drv == nil || idx == nil || bus == nil {
		return nil, fmt.Errorf("gc.NewGCAgent: store, driver, index and bus are required")
	}
	if hostID == "" {
		return nil, fmt.Errorf("gc.NewGCAgent: hostID is required for the gc lease")
	}
	cfg = applyGCDefaults(cfg)
	return &gcAgent{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		store:     st, drv: drv, idx: idx, bus: bus,
		hostID: hostID, storeID: storeID, cfg: cfg,
	}, nil
}

const (
	gcLeaseName           = "store.agent.gc.lease"
	defaultGCScanInterval = 1 * time.Hour
	defaultGCBatchSize    = 100
	defaultGCLeaseTTL     = 5 * time.Minute
	defaultMinPackAge     = 168 * time.Hour // 7 days
)

func applyGCDefaults(cfg GCConfig) GCConfig {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = defaultGCScanInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultGCBatchSize
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultGCLeaseTTL
	}
	if cfg.CompactionThreshold <= 0 {
		cfg.CompactionThreshold = 0.5
	}
	if cfg.MinPackAge <= 0 {
		cfg.MinPackAge = defaultMinPackAge
	}
	return cfg
}

// gcIndex is the slice of index.StoreIndex the GC agent depends on: it
// lists orphan blobs, resolves a blob to its physical address, and
// deletes a confirmed orphan. Declaring the narrow port here — rather
// than accepting the full StoreIndex — keeps GC decoupled from index
// methods it never calls and lets a test supply a small fake. The
// concrete *sqlite.Index, and any full index.StoreIndex value, satisfies
// it structurally.
type gcIndex interface {
	ListOrphanBlobs(ctx context.Context, cb func(blobRef string) error) error
	Resolve(ctx context.Context, blobRef string) (domain.PhysicalAddress, error)
	DeleteOrphanBlob(ctx context.Context, blobRef string) (removed bool, err error)
}

// gcAgent is the concrete GCAgent.
type gcAgent struct {
	store   store.Store
	drv     driver.Driver
	idx     gcIndex
	bus     event.Publisher
	hostID  string
	storeID string
	cfg     GCConfig

	agent.BaseState
}

var _ GCAgent = (*gcAgent)(nil)

// gcFactory builds the GC agent from the registry (ADR-51).
type gcFactory struct{}

func (gcFactory) Name() string { return "gc" }

func (gcFactory) Build(st store.Store, cfg any, deps agent.AgentDeps) (agent.Agent, error) {
	c, _ := cfg.(GCConfig) // zero value on mismatch -> defaults
	return NewGCAgent(st, deps.Driver, deps.Index, deps.Publisher, deps.HostID, deps.StoreID, c, agent.WithAgentLogger(deps.Logger))
}

// AgentType is the short registry/event identifier.
func (a *gcAgent) AgentType() string { return "gc" }

// Validate checks preconditions. GC needs no maintenance mode; the gc
// lease is acquired inside Run, so the only precondition here is a live
// context.
func (a *gcAgent) Validate(ctx context.Context) error { return ctx.Err() }

// Run performs one Mark+Sweep cycle and returns its AgentResult. The
// agent is periodically invoked by the scheduler (ADR-69), not a
// resident loop: an interrupted cycle resumes from the Store-held
// progress (orphan re-selection) on the next call. The maintenance
// lifecycle (lease, events, state) is agent.RunLeased (ADR-94); Run
// supplies only the Mark+Sweep pass.
func (a *gcAgent) Run(ctx context.Context) (*domain.AgentResult, error) {
	return agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		stats, err := a.runCycle(ctx)
		return gcStatsMap(stats), err
	})
}

// RunNow executes one full Mark+Sweep cycle and returns the typed stats
// — the manual/test entry of the GCAgent interface. Runs the same leased
// lifecycle as Run (agent.RunLeased, ADR-94).
func (a *gcAgent) RunNow(ctx context.Context) (GCStats, error) {
	var stats GCStats
	_, err := agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		var werr error
		stats, werr = a.runCycle(ctx)
		return gcStatsMap(stats), werr
	})
	return stats, err
}

// maintenanceSpec is the RunLeased configuration shared by Run and
// RunNow. The lease is conditional: LeaderElection coordinates via
// gc/lease, SingleHost relies on location.lock and takes none
// (leaseRequired).
func (a *gcAgent) maintenanceSpec() agent.MaintenanceSpec {
	return agent.MaintenanceSpec{
		AgentType:    "gc",
		StoreID:      a.storeID,
		Lease:        lease.Config{Name: gcLeaseName, HostID: a.hostID, AgentType: "gc", TTL: a.cfg.LeaseTTL},
		LeaseEnabled: a.leaseRequired(a.store.Config().GCLeasePolicy),
		Terminal:     event.EventAgentCycle,
		TerminalMode: agent.TerminalEveryCycle,
		Bus:          a.bus,
		Driver:       a.drv,
	}
}

func gcStatsMap(s GCStats) map[string]int64 {
	return map[string]int64{
		"scanned_blobs": s.ScannedBlobs,
		"marked_blobs":  s.MarkedBlobs,
		"removed_blobs": s.RemovedBlobs,
		"freed_bytes":   s.FreedBytes,
	}
}

// runCycle executes one Mark+Sweep pass — no lifecycle; agent.RunLeased
// (ADR-94) owns lease/events/state. Mark tombstones every ref_count=0
// blob; media reconciliation tombstones blob files the index has never
// heard of (ADR-118, media_reconcile.go); Sweep removes tombstone files
// whose grace period has elapsed and drops their index rows.
func (a *gcAgent) runCycle(ctx context.Context) (GCStats, error) {
	if err := ctx.Err(); err != nil {
		return GCStats{}, err
	}
	var stats GCStats
	grace := a.store.Config().TombstoneGracePeriod

	markErr := a.mark(ctx, &stats)
	// Media reconciliation runs between Mark and Sweep so a file it marks
	// waits out the same grace period as everything else (ADR-118).
	mediaErr := a.reconcileMedia(ctx, &stats)
	sweepErr := a.sweep(ctx, grace, &stats)

	if err := agent.FirstNonCtxErr(markErr, mediaErr, sweepErr); err != nil {
		return stats, fmt.Errorf("gc.GC.RunNow: %w", err)
	}
	return stats, nil
}

// leaseRequired decides whether this cycle coordinates via gc/lease.
// LeaderElection always does; SingleHost never does (location.lock
// already guarantees a single process). Auto defers to SingleHost here
// — multi-host promotion is a deployment decision the assembler makes
// explicit, not something GC infers at runtime.
func (a *gcAgent) leaseRequired(policy config.GCLeasePolicy) bool {
	return policy == config.GCLeaseLeaderElection
}

// mark tombstones every orphan blob. Orphan refs are collected first,
// then acted on — MarkTombstone/Resolve must not run while the
// ListOrphanBlobs cursor is open (nested index/driver work under an open
// cursor deadlocks the connection pool).
func (a *gcAgent) mark(ctx context.Context, stats *GCStats) error {
	var refs []string
	listErr := a.idx.ListOrphanBlobs(ctx, func(blobRef string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		refs = append(refs, blobRef)
		return nil
	})
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.ScannedBlobs++
		addr, err := a.idx.Resolve(ctx, ref)
		if err != nil {
			if errors.Is(err, errs.ErrArtifactNotFound) {
				continue // raced with a parallel sweep
			}
			return err
		}
		if addr.Path == "" {
			// Packed blob (lives inside a .pack); standalone-file GC
			// does not tombstone it. Pack compaction (deferred) owns
			// those. Skip.
			continue
		}
		if err := a.drv.MarkTombstone(ctx, addr.Path); err != nil {
			if agent.IsCtxErr(err) {
				return err
			}
			continue // transient driver error: next cycle retries
		}
		stats.MarkedBlobs++
	}
	return listErr
}

// sweep removes tombstone files past the grace period and drops their
// index rows. Same collect-then-act discipline as mark.
func (a *gcAgent) sweep(ctx context.Context, grace time.Duration, stats *GCStats) error {
	var refs []string
	listErr := a.idx.ListOrphanBlobs(ctx, func(blobRef string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		refs = append(refs, blobRef)
		return nil
	})
	now := time.Now()
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		addr, err := a.idx.Resolve(ctx, ref)
		if err != nil {
			if errors.Is(err, errs.ErrArtifactNotFound) {
				continue
			}
			return err
		}
		if addr.Path == "" {
			continue // packed; not a standalone tombstone
		}
		marked, since, err := a.drv.TombstoneInfo(ctx, addr.Path)
		if err != nil {
			if agent.IsCtxErr(err) {
				return err
			}
			continue
		}
		if !marked {
			continue // not tombstoned yet (a future Mark will catch it)
		}
		// Grace check: a marker is eligible once its age is at least
		// the grace period. grace == 0 means "sweep immediately", so
		// any marker qualifies — using >= (not >) and guarding the
		// zero case keeps sub-second ModTime jitter from skipping it.
		if grace > 0 && now.Sub(since) < grace {
			continue // still within grace
		}

		// Grace elapsed: physically remove the tombstone marker (NOT
		// addr.Path — Mark renamed the original to "<path>.tombstone",
		// so the original no longer exists). A missing marker means the
		// blob was Revived; RemoveTombstone treats it as a no-op.
		if err := a.drv.RemoveTombstone(ctx, addr.Path); err != nil {
			if agent.IsCtxErr(err) {
				return err
			}
			continue
		}
		// Drop the index row, but only if still an orphan: a Revive
		// between TombstorneInfo and here bumps ref_count, and
		// DeleteOrphanBlob then keeps the row.
		removed, err := a.idx.DeleteOrphanBlob(ctx, ref)
		if err != nil {
			if agent.IsCtxErr(err) {
				return err
			}
			continue
		}
		if !removed {
			continue // revived; leave it
		}
		stats.RemovedBlobs++
		stats.FreedBytes += addr.Size
		a.bus.Publish(event.Event{
			Type:    event.EventBlobPhysicallyDeleted,
			Payload: event.BlobPhysicallyDeletedPayload{BlobRef: ref},
		})
	}
	return listErr
}
