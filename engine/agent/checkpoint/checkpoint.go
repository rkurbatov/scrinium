package checkpoint

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/checkpointfmt"
	"scrinium.dev/engine/lease"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/systemstore"
	"scrinium.dev/event"
)

// --- Checkpoint Agent ---

// CheckpointConfig configures the Checkpoint Agent.
type CheckpointConfig struct {
	// Enabled toggles background checkpointting.
	Enabled bool

	// Interval is the periodic checkpoint interval.
	Interval time.Duration

	// ArtifactThreshold also triggers a checkpoint once this many
	// new artifacts have been added since the previous checkpoint.
	ArtifactThreshold int

	// Retention is the number of checkpoints to keep; older ones are
	// removed.
	Retention int

	// RecoveryOverlap is the recovery overlap: when loading a
	// checkpoint, RebuildIndexAgent re-reads objects that appeared
	// after checkpoint_created_at - RecoveryOverlap. It guards
	// against the edge case "an object was written between the
	// checkpoint and the crash".
	RecoveryOverlap time.Duration
}

// CheckpointStats are the statistics of a single checkpoint.
type CheckpointStats struct {
	CheckpointID string
	DBBytes      int64
	ArtifactsAt  int64
	CreatedAt    time.Time
}

// CheckpointAgent is the background creator of StoreIndex checkpoints
// via WriteCheckpoint + packing into the CAS. Engine-managed: launched
// for every Target Store with an available StoreIndex.
//
// Checkpoint Agent is creation only. StoreIndex recovery from a
// checkpoint is the job of RebuildIndexAgent (maintenance), which
// uses a fresh checkpoint as the starting point and reads in the
// new manifests through ListObjectsWithModTime.
type CheckpointAgent interface {
	agent.Agent

	// RunNow forces a checkpoint regardless of Interval and
	// ArtifactThreshold. Used before critical maintenance
	// operations (RebuildIndex, MigrateIndex).
	RunNow(ctx context.Context) (CheckpointStats, error)
}

const (
	checkpointLeaseName       = "store.agent.checkpoint.lease"
	defaultCheckpointIntv     = 6 * time.Hour
	defaultCheckpointKeep     = 3
	defaultCheckpointLeaseTTL = 15 * time.Minute
)

// NewCheckpointAgent creates a Checkpoint Agent. Constructed by the
// assembler (Variant B): it needs the StoreIndex to WriteCheckpoint a
// checkpoint file and the Store to publish that file into the CAS via
// System().Put. A checkpoint is engine state, not a user artifact;
// system artifacts live in their own system/ address space and are
// never indexed (ADR-85), so a checkpoint can never become a rebuild
// input of itself — that guarantee is structural now, not an opt-out
// flag on the write.
func NewCheckpointAgent(
	st store.Store,
	drv driver.Driver,
	idx index.StoreIndex,
	bus event.Publisher,
	hostID string,
	storeID string,
	cfg CheckpointConfig,
	opts ...agent.AgentOption,
) (CheckpointAgent, error) {
	if st == nil || drv == nil || idx == nil || bus == nil {
		return nil, fmt.Errorf("checkpoint.NewCheckpointAgent: store, driver, index and bus are required")
	}
	if hostID == "" {
		return nil, fmt.Errorf("checkpoint.NewCheckpointAgent: hostID is required for the checkpoint lease")
	}
	cfg = applyCheckpointDefaults(cfg)
	return &checkpointAgent{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		store:     st, drv: drv, idx: idx, bus: bus,
		hostID: hostID, storeID: storeID, cfg: cfg,
	}, nil
}

func applyCheckpointDefaults(cfg CheckpointConfig) CheckpointConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultCheckpointIntv
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultCheckpointKeep
	}
	return cfg
}

type checkpointAgent struct {
	store   store.Store
	drv     driver.Driver
	idx     index.StoreIndex
	bus     event.Publisher
	hostID  string
	storeID string
	cfg     CheckpointConfig

	agent.BaseState
}

var _ CheckpointAgent = (*checkpointAgent)(nil)

// checkpointFactory builds the Checkpoint agent from the registry (ADR-51).
type checkpointFactory struct{}

func (checkpointFactory) Name() string { return "checkpoint" }

func (checkpointFactory) Build(st store.Store, cfg any, deps agent.AgentDeps) (agent.Agent, error) {
	c, _ := cfg.(CheckpointConfig) // zero value on mismatch -> defaults
	return NewCheckpointAgent(st, deps.Driver, deps.Index, deps.Publisher, deps.HostID, deps.StoreID, c, agent.WithAgentLogger(deps.Logger))
}

// AgentType is the short registry/event identifier.
func (a *checkpointAgent) AgentType() string { return "checkpoint" }

// Validate checks preconditions. Checkpoint needs no maintenance mode;
// the checkpoint lease is acquired inside Run, so the only precondition
// here is a live context.
func (a *checkpointAgent) Validate(ctx context.Context) error { return ctx.Err() }

// Run takes one checkpoint and returns its AgentResult. A one-shot
// operation: the scheduler decides cadence (ADR-69). The maintenance
// lifecycle (lease, events, state) is agent.RunLeased (ADR-94); Run
// supplies only the checkpoint pass.
func (a *checkpointAgent) Run(ctx context.Context) (*domain.AgentResult, error) {
	return agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		stats, err := a.checkpointOnce(ctx)
		return checkpointStatsMap(stats), err
	})
}

// maintenanceSpec is the RunLeased configuration shared by Run and
// RunNow: the checkpoint lease (always taken) and the one-shot
// terminal (EventAgentCompleted on success).
func (a *checkpointAgent) maintenanceSpec() agent.MaintenanceSpec {
	return agent.MaintenanceSpec{
		AgentType:    "checkpoint",
		StoreID:      a.storeID,
		Lease:        lease.Config{Name: checkpointLeaseName, HostID: a.hostID, AgentType: "checkpoint", TTL: defaultCheckpointLeaseTTL},
		LeaseEnabled: true,
		Terminal:     event.EventAgentCompleted,
		TerminalMode: agent.TerminalOnSuccess,
		Bus:          a.bus,
		Driver:       a.drv,
	}
}

func checkpointStatsMap(s CheckpointStats) map[string]int64 {
	return map[string]int64{"db_bytes": s.DBBytes}
}

// RunNow forces a checkpoint regardless of Interval and
// ArtifactThreshold — the ad-hoc entry of the CheckpointAgent interface.
// It runs the same leased lifecycle as Run (agent.RunLeased, ADR-94):
// vacuum the index into a temp file, publish it into the CAS under
// index_checkpoint/<ts> (unindexed), then prune past Retention.
func (a *checkpointAgent) RunNow(ctx context.Context) (CheckpointStats, error) {
	var stats CheckpointStats
	_, err := agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		var werr error
		stats, werr = a.checkpointOnce(ctx)
		return checkpointStatsMap(stats), werr
	})
	return stats, err
}

func (a *checkpointAgent) checkpointOnce(ctx context.Context) (CheckpointStats, error) {
	now := time.Now().UTC()
	id := checkpointfmt.ID(now)

	// WriteCheckpoint needs an empty destination on an OS path. A temp dir
	// keeps the partial vacuum off the Location until it is complete.
	tmpDir, err := os.MkdirTemp("", "scrinium-checkpoint-")
	if err != nil {
		return CheckpointStats{}, fmt.Errorf("mkdir temp: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	tmpPath := filepath.Join(tmpDir, id+".db")

	cw, ok := a.idx.(index.CheckpointWriter)
	if !ok {
		return CheckpointStats{}, fmt.Errorf("checkpoint: index %T does not support checkpoints (no index.CheckpointWriter)", a.idx)
	}
	if err := cw.WriteCheckpoint(ctx, tmpPath); err != nil {
		return CheckpointStats{}, fmt.Errorf("write checkpoint %q: %w", tmpPath, err)
	}

	info, err := os.Stat(tmpPath)
	if err != nil {
		return CheckpointStats{}, fmt.Errorf("stat checkpoint: %w", err)
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return CheckpointStats{}, fmt.Errorf("open checkpoint: %w", err)
	}
	defer f.Close()

	name := checkpointfmt.Prefix + id

	// Publish the .db as an external headless data artifact (ADR-105): a
	// checkpoint is far larger than an inline manifest's cap, so it cannot ride
	// inside the envelope. WriteHeadless stores it as a blob-backed, indexed
	// (orphan-safe) data artifact and returns its digest; the system artifact
	// under the checkpoint name is then a thin pointer envelope carrying only
	// {store_id, external_payload_ref}. Get resolves the pointer back to the
	// .db stream; Delete (prune) cascades to reap the blob.
	hw, ok := store.HeadlessOf(a.store)
	if !ok {
		return CheckpointStats{}, fmt.Errorf("checkpoint: store %T has no headless data plane", a.store)
	}
	digest, err := hw.WriteHeadless(ctx, f)
	if err != nil {
		return CheckpointStats{}, fmt.Errorf("write checkpoint blob %q: %w", name, err)
	}
	if err := a.store.System().Put(ctx,
		systemstore.NamedArtifact{Name: name, ExternalRef: digest},
	); err != nil {
		return CheckpointStats{}, fmt.Errorf("publish checkpoint %q: %w", name, err)
	}

	if err := a.pruneOldCheckpoints(ctx); err != nil {
		// Retention failure does not invalidate the checkpoint just taken.
		a.bus.Publish(event.Event{Type: event.EventAgentFailed, Payload: event.AgentFailedPayload{
			AgentType: "checkpoint", StoreID: a.storeID, Err: err,
		}})
	}

	return CheckpointStats{
		CheckpointID: id,
		DBBytes:      info.Size(),
		CreatedAt:    now,
	}, nil
}

// pruneOldCheckpoints keeps the newest Retention checkpoints and deletes the
// rest. Names embed a fixed-width, path-safe timestamp, so a string sort
// is a chronological sort.
//
// Note on dedup: if two checkpoints have byte-identical bodies (the index
// did not change between them) they share one CAS artifact (ADR-58), and
// System().Delete of one name drops that shared artifact, orphaning the
// other name's pointer. In practice consecutive checkpoints are taken over
// a changing index (or forced once before maintenance), so identical
// bodies do not arise; the case is noted rather than guarded, since the
// fix belongs to System() artifact sharing semantics, not to retention.
func (a *checkpointAgent) pruneOldCheckpoints(ctx context.Context) error {
	var names []string
	err := a.store.System().Walk(ctx, checkpointfmt.Prefix, func(name string, _ domain.Manifest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The checkpoint lease (store.agent.checkpoint.lease) and any other
		// foreign object share the Prefix but are not checkpoints. Skip
		// anything whose suffix is not a valid checkpoint timestamp — the
		// same discrimination Latest applies — so retention counts and
		// prunes only real checkpoints.
		if _, perr := checkpointfmt.ParseID(name); perr != nil {
			return nil
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk checkpoints: %w", err)
	}
	if len(names) <= a.cfg.Retention {
		return nil
	}
	sort.Strings(names) // oldest first
	excess := len(names) - a.cfg.Retention
	for _, old := range names[:excess] {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.store.System().Delete(ctx, old); err != nil {
			return fmt.Errorf("delete old checkpoint %q: %w", old, err)
		}
	}
	return nil
}
