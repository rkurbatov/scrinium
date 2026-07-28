package scrub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/lease"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// --- Scrub Agent ---

// ScrubConfig configures the Scrub Agent.
type ScrubConfig struct {
	// Enabled toggles background verification.
	Enabled bool

	// ScanInterval is the interval between verification cycles.
	ScanInterval time.Duration

	// MaxAge — blobs whose last_verified_at is older than
	// now() - MaxAge are eligible for verification.
	MaxAge time.Duration

	// MaxAgeNativeChecksum is an extended MaxAge for blobs on
	// media that report CapNativeChecksum. Silent bit rot is
	// impossible there, so the verification rate can be lowered.
	MaxAgeNativeChecksum time.Duration

	// BatchSize is the number of blobs in a single StoreIndex
	// fetch.
	BatchSize int

	// Force makes a pass verify every blob and manifest regardless of
	// last_verified_at (the staleness cutoff is ignored). Used for an
	// ad-hoc full re-scan after a suspected media fault. Background
	// passes leave it false.
	Force bool
}

// ScrubStats are the statistics of a single Scrub cycle.
type ScrubStats struct {
	ScannedBlobs  int64
	VerifiedBlobs int64
	FailedBlobs   int64

	// DuplicateHandles counts handles carrying more than one live
	// manifest row (decision R6): the manifests_artifact index is
	// non-UNIQUE by design (safe two-phase form migration), so outside
	// an active migration a duplicate is a write-path bug — the schema
	// will not catch it, the scrub cycle does. Populated only when the
	// index implements index.DuplicateHandleAuditor.
	DuplicateHandles int64

	// ManifestFiles and ManifestRows are the census (ADR-118): manifest
	// files on the Location versus rows in the index. They are counted, not
	// acted on — a persistent gap is the only cheap signal that the index
	// has silently lost rows, which no question put to the index can
	// reveal. Zero for a backend that cannot enumerate its manifests.
	ManifestFiles int64
	ManifestRows  int64
}

// ScrubAgent is the background blob-integrity verifier.
// Engine-managed: a single Scrub Agent is launched automatically
// Agent for every registered Target Store.
type ScrubAgent interface {
	agent.Agent

	// RunNow performs one full verification pass over every blob
	// whose last_verified_at is older than MaxAge and returns. Used
	// for ad hoc runs after media-corruption suspicions.
	RunNow(ctx context.Context) (ScrubStats, error)
}

// NewScrubAgent creates a Scrub Agent. Constructed by the assembler
// (engine-internal) with the raw Driver and StoreIndex it needs
// alongside store.Store: the blob pass walks ListUnverifiedBlobs and
// the manifest pass walks ListUnverifiedManifests on the index, while
// the verification itself goes through the Store (VerifyBlobRef /
// VerifyManifest). hostID is the boot-unique process id owning the
// scrub lease; storeID tags emitted events and may be empty.
//
// The agent is per-store by design: it verifies and stamps only this
// store's blobs and manifests. Cross-store dedup does not change that —
// integrity is "this copy", and each store runs its own Scrub
// (2. Internals / Multistore: admin/maintenance ops are per-store).
func NewScrubAgent(
	st store.Store,
	drv driver.Driver,
	idx scrubIndex,
	bus event.Publisher,
	hostID string,
	storeID string,
	cfg ScrubConfig,
	opts ...agent.AgentOption,
) (ScrubAgent, error) {
	if st == nil || drv == nil || idx == nil || bus == nil {
		return nil, fmt.Errorf("scrub.NewScrubAgent: store, driver, index and bus are required")
	}
	if hostID == "" {
		return nil, fmt.Errorf("scrub.NewScrubAgent: hostID is required for the scrub lease")
	}
	cfg = applyScrubDefaults(cfg)
	return &scrubAgent{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		store:     st, drv: drv, idx: idx, bus: bus,
		hostID: hostID, storeID: storeID, cfg: cfg,
	}, nil
}

const (
	scrubLeaseName           = "store.agent.scrub.lease"
	defaultScrubScanInterval = 24 * time.Hour
	defaultScrubMaxAge       = 30 * 24 * time.Hour
	defaultScrubBatchSize    = 1000
	defaultScrubLeaseTTL     = 30 * time.Minute
)

func applyScrubDefaults(cfg ScrubConfig) ScrubConfig {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = defaultScrubScanInterval
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = defaultScrubMaxAge
	}
	if cfg.MaxAgeNativeChecksum <= 0 {
		// Default: same cadence as MaxAge. An operator lowers the rate
		// for native-checksum media explicitly.
		cfg.MaxAgeNativeChecksum = cfg.MaxAge
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultScrubBatchSize
	}
	return cfg
}

// scrubIndex is the slice of index.StoreIndex the Scrub agent depends
// on: it lists unverified blobs and manifests, maps a blob to the
// manifests that reference it, and stamps a verified blob/manifest.
// Declaring the narrow port here — rather than accepting the full
// StoreIndex — keeps Scrub decoupled from index methods it never calls
// and lets a test supply a small fake. The concrete *sqlite.Index, and
// any full index.StoreIndex value, satisfies it structurally.
type scrubIndex interface {
	ListUnverifiedBlobs(ctx context.Context, before time.Time, cb func(blobRef string) error) error
	ListUnverifiedManifests(ctx context.Context, before time.Time, cb func(domain.Manifest) error) error
	ManifestsByBlobRef(ctx context.Context, blobRef string, cb func(domain.Manifest) error) error
	MarkVerified(ctx context.Context, blobRef string, timestamp time.Time) error
	MarkManifestVerified(ctx context.Context, artifactID domain.ArtifactID, timestamp time.Time) error
}

// scrubAgent is the concrete ScrubAgent.
type scrubAgent struct {
	store   store.Store
	drv     driver.Driver
	idx     scrubIndex
	bus     event.Publisher
	hostID  string
	storeID string
	cfg     ScrubConfig

	agent.BaseState
}

var _ ScrubAgent = (*scrubAgent)(nil)

// scrubFactory builds the Scrub agent from the registry (ADR-51).
type scrubFactory struct{}

func (scrubFactory) Name() string { return "scrub" }

func (scrubFactory) Build(st store.Store, cfg any, deps agent.AgentDeps) (agent.Agent, error) {
	c, _ := cfg.(ScrubConfig) // zero value on mismatch -> defaults
	return NewScrubAgent(st, deps.Driver, deps.Index, deps.Publisher, deps.HostID, deps.StoreID, c, agent.WithAgentLogger(deps.Logger))
}

// AgentType is the short registry/event identifier.
func (a *scrubAgent) AgentType() string { return "scrub" }

// Validate checks preconditions. Scrub needs no maintenance mode; the
// scrub lease is acquired inside Run, so the only precondition here is
// a live context.
func (a *scrubAgent) Validate(ctx context.Context) error { return ctx.Err() }

// Run performs one verification pass and returns its AgentResult. The
// agent is periodically invoked by the scheduler (ADR-69): an
// interrupted pass resumes from last_verified_at on the next call. The
// maintenance lifecycle (lease, events, state) is agent.RunLeased
// (ADR-94); Run supplies only the verification pass.
func (a *scrubAgent) Run(ctx context.Context) (*domain.AgentResult, error) {
	return agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		stats, err := a.verify(ctx)
		return scrubStatsMap(stats), err
	})
}

// RunNow performs one verification pass under the scrub lease and
// returns the typed stats — the ad-hoc entry of the ScrubAgent
// interface. Runs the same leased lifecycle as Run (agent.RunLeased,
// ADR-94).
func (a *scrubAgent) RunNow(ctx context.Context) (ScrubStats, error) {
	var stats ScrubStats
	_, err := agent.RunLeased(ctx, &a.BaseState, a.maintenanceSpec(), func(ctx context.Context) (map[string]int64, error) {
		var werr error
		stats, werr = a.verify(ctx)
		return scrubStatsMap(stats), werr
	})
	return stats, err
}

// maintenanceSpec is the RunLeased configuration shared by Run and
// RunNow: the scrub lease (always taken) and the per-cycle terminal.
func (a *scrubAgent) maintenanceSpec() agent.MaintenanceSpec {
	return agent.MaintenanceSpec{
		AgentType:    "scrub",
		StoreID:      a.storeID,
		Lease:        lease.Config{Name: scrubLeaseName, HostID: a.hostID, AgentType: "scrub", TTL: defaultScrubLeaseTTL},
		LeaseEnabled: true,
		Terminal:     event.EventAgentCycle,
		TerminalMode: agent.TerminalEveryCycle,
		Bus:          a.bus,
		Driver:       a.drv,
	}
}

func scrubStatsMap(s ScrubStats) map[string]int64 {
	return map[string]int64{
		"scanned_blobs":     s.ScannedBlobs,
		"verified_blobs":    s.VerifiedBlobs,
		"failed_blobs":      s.FailedBlobs,
		"duplicate_handles": s.DuplicateHandles,
		"manifest_files":    s.ManifestFiles,
		"manifest_rows":     s.ManifestRows,
	}
}

// verify performs one verification pass — no lifecycle; agent.RunLeased
// (ADR-94) owns lease/events/state. A blob pass (ListUnverifiedBlobs →
// VerifyBlobRef → MarkVerified → cascade to consuming manifests) and a
// manifest pass (ListUnverifiedManifests, covering Inline artifacts the
// blob pass cannot reach). A blob that fails verification is recorded
// and the pass continues — one bad blob must not abort the scrub.
func (a *scrubAgent) verify(ctx context.Context) (ScrubStats, error) {
	var stats ScrubStats
	cutoff := a.cutoff(time.Now())

	// Phase A — blobs (the expensive plaintext check).
	//
	// Collect the work list first, then close the cursor before
	// verifying. VerifyBlobRef issues its own index queries
	// (ManifestsByBlobRef, loadManifest); running them while the
	// ListUnverifiedBlobs cursor is still open nests a query inside an
	// open result set on the same connection pool, which deadlocks or
	// hits a stale connection. Materialising the (bounded) ref list up
	// front keeps each index interaction independent.
	var blobRefs []string
	blobErr := a.idx.ListUnverifiedBlobs(ctx, cutoff, func(blobRef string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		blobRefs = append(blobRefs, blobRef)
		return nil
	})
	for _, blobRef := range blobRefs {
		if err := ctx.Err(); err != nil {
			blobErr = err
			break
		}
		stats.ScannedBlobs++
		switch err := a.store.VerifyBlobRef(ctx, blobRef); {
		case err == nil:
			stats.VerifiedBlobs++
			if err := a.idx.MarkVerified(ctx, blobRef, time.Now()); err != nil {
				a.Logger().Warn("scrub: failed to record blob verification", "blob_ref", blobRef, "err", err)
			}
			a.cascadeStampConsumers(ctx, blobRef)
		case errors.Is(err, errs.ErrArtifactNotFound):
			// No consuming manifest (race vs Delete/GC, or orphan):
			// skip, not a corruption. GC owns orphan removal.
		case agent.IsCtxErr(err):
			blobErr = err
		default:
			// Corruption: VerifyBlobRef already published
			// EventScrubFailed. Count and continue.
			stats.FailedBlobs++
		}
	}

	// Phase B — manifests (cheap; covers Inline artifacts with no blob).
	// Same collect-then-act discipline as Phase A.
	var manIDs []domain.ArtifactID
	manErr := a.idx.ListUnverifiedManifests(ctx, cutoff, func(m domain.Manifest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		manIDs = append(manIDs, m.ArtifactID)
		return nil
	})
	for _, id := range manIDs {
		if err := ctx.Err(); err != nil {
			manErr = err
			break
		}
		switch err := a.store.VerifyManifest(ctx, id); {
		case err == nil:
			if err := a.idx.MarkManifestVerified(ctx, id, time.Now()); err != nil {
				a.Logger().Warn("scrub: failed to record manifest verification", "artifact_id", id, "err", err)
			}
		case errors.Is(err, errs.ErrArtifactNotFound):
			// raced with Delete — skip.
		case agent.IsCtxErr(err):
			manErr = err
		default:
			stats.FailedBlobs++ // manifest corruption counts as a failure too
		}
	}

	// Phase C — duplicate-handle invariant (decision R6). By-assertion:
	// only when the backend implements the auditor capability. A hit is
	// legal only inside an active form migration; anything the operator
	// did not expect is a write-path bug, surfaced loudly but without
	// failing the pass — corruption counters are about data, this is
	// about the index invariant.
	if aud, ok := a.idx.(index.DuplicateHandleAuditor); ok && ctx.Err() == nil {
		switch dups, err := aud.ListDuplicateHandles(ctx); {
		case err != nil && !agent.IsCtxErr(err):
			a.Logger().Warn("scrub: duplicate-handle audit failed", "err", err)
		case err == nil && len(dups) > 0:
			stats.DuplicateHandles = int64(len(dups))
			for _, id := range dups {
				a.Logger().Warn("scrub: duplicate manifest rows for handle (legal only during an active form migration)",
					"artifact_id", id)
			}
		}
	}

	if err := agent.FirstNonCtxErr(blobErr, manErr); err != nil {
		return stats, fmt.Errorf("scrub.Scrub.RunNow: %w", err)
	}
	// The census closes the cycle: it costs a directory listing and one
	// walk of the manifest rows, reads nothing, and needs no key. It never
	// deletes — a disagreement is a reason to reconcile, reported here and
	// acted on elsewhere (ADR-118).
	if c, err := a.takeCensus(ctx); err == nil {
		stats.ManifestFiles, stats.ManifestRows = c.Files, c.Rows
		if !c.Agrees() {
			a.Logger().LogAttrs(ctx, slog.LevelWarn,
				"manifest census disagrees: the index is not the whole truth for this Location",
				slog.Int64("manifest_files", c.Files),
				slog.Int64("manifest_rows", c.Rows))
		}
	} else if !errors.Is(err, errNoCensus) {
		a.Logger().LogAttrs(ctx, slog.LevelInfo,
			"manifest census skipped", slog.String("reason", err.Error()))
	}

	return stats, nil
}

// cutoff is the staleness boundary: blobs/manifests last verified before
// it are eligible. Force verifies everything (zero time = "before now"
// for all rows, NULL included). Native-checksum media uses the relaxed
// MaxAgeNativeChecksum.
func (a *scrubAgent) cutoff(now time.Time) time.Time {
	if a.cfg.Force {
		// Everything is older than "now", so every row is eligible.
		return now
	}
	maxAge := a.cfg.MaxAge
	if a.store.Capabilities().Has(driver.CapNativeChecksum) {
		maxAge = a.cfg.MaxAgeNativeChecksum
	}
	return now.Add(-maxAge)
}

// cascadeStampConsumers re-verifies (cheaply) and stamps every manifest
// that references blobRef, after its blob has been confirmed. A
// multi-blob (TOC) manifest is only stamped once all of its blobs are
// fresh — until then VerifyManifest succeeds but the manifest reappears
// in the next manifest pass, which is harmless (the cheap check repeats).
func (a *scrubAgent) cascadeStampConsumers(ctx context.Context, blobRef string) {
	// Collect consumer ids first, then verify/stamp — VerifyManifest
	// queries the index (loadManifest), which must not run while the
	// ManifestsByBlobRef cursor is open (see Phase A note).
	var ids []domain.ArtifactID
	if err := a.idx.ManifestsByBlobRef(ctx, blobRef, func(m domain.Manifest) error {
		ids = append(ids, m.ArtifactID)
		return nil
	}); err != nil {
		a.Logger().Warn("scrub: failed to enumerate blob consumers", "blob_ref", blobRef, "err", err)
	}
	for _, id := range ids {
		if a.store.VerifyManifest(ctx, id) == nil {
			if err := a.idx.MarkManifestVerified(ctx, id, time.Now()); err != nil {
				a.Logger().Warn("scrub: failed to record manifest verification (cascade)", "artifact_id", id, "err", err)
			}
		}
		// A corrupt consumer manifest already emitted EventScrubFailed
		// inside VerifyManifest; the cascade does not abort on it.
	}
}
