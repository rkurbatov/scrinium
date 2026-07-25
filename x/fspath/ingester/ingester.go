package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// IngestMode is the operating mode of the Ingester.
type IngestMode string

const (
	// IngestModeOneShot — a single sweep of SourcePath; the agent finishes
	// once every found element has been processed.
	IngestModeOneShot IngestMode = "one-shot"

	// IngestModeWatch — continuous observation. Deferred (ADR-115 П-10):
	// deletions, renames and a lost subscription each need their own answer.
	IngestModeWatch IngestMode = "watch"
)

// Kind is what an element of the source is.
type Kind uint8

const (
	KindFile Kind = iota
	KindDir
)

// Decision is what the policy wants done with an element.
type Decision uint8

const (
	// Take — capture the element into an artifact.
	Take Decision = iota
	// Skip — do not capture, and do not come back to it (junk, filtered out).
	Skip
	// Defer — leave it for a later sweep (not ready, not wanted yet).
	Defer
)

// Element is one entry of the source as the agent sees it, and the only thing
// the policy gets to decide on. Path is relative to SourcePath and normalised,
// so it is already the key the artifact will be found by.
type Element struct {
	Path    string
	Kind    Kind
	Size    int64
	ModTime time.Time
	Mode    uint32
}

// Policy is the whole domain half of ingestion: what to take and what meaning
// to attach. Everything above it in this package is mechanism.
//
// Enrich returns additions to the artifact's pockets and put options. The Ext it
// returns is merged under the host's own schema keys; the agent's own vfsmeta
// block is written by the agent and is not the policy's to supply.
type Policy interface {
	Decide(el Element) Decision
	Enrich(el Element) (ext, usr json.RawMessage, opts []domain.PutOption, err error)
}

// TakeAll is the default policy: capture everything, add nothing.
type TakeAll struct{}

func (TakeAll) Decide(Element) Decision { return Take }
func (TakeAll) Enrich(Element) (json.RawMessage, json.RawMessage, []domain.PutOption, error) {
	return nil, nil, nil, nil
}

// DisposalPolicy says what happens to the source element after a successful
// capture.
type DisposalPolicy uint8

const (
	// Keep — leave the source untouched (default).
	Keep DisposalPolicy = iota
	// Remove — delete the source element, but only after the artifact is
	// durably indexed. The reverse order loses data on a crash between.
	Remove
)

// Config is the configuration of a single Ingester. One instance — one source.
type Config struct {
	// SourcePath is the source's root, interpreted by the driver.
	SourcePath string

	// Mode is OneShot (Watch is deferred).
	Mode IngestMode

	// BatchSize is the number of captures after which the watermark advances.
	// Default 100.
	BatchSize int

	// FlushTimeout bounds how long a partial batch waits before the watermark
	// advances anyway. Default 5s.
	FlushTimeout time.Duration

	// Concurrency is the number of workers reading and hashing. Default 4.
	Concurrency int

	// CaptureDirs captures directories as artifacts (ADR-114). Requires a
	// driver declaring CapDirEntries — over a driver without it this is an
	// error, not a silent omission: silently dropping directories yields a
	// tree that looks complete and is not.
	CaptureDirs bool

	// SettleWindow is the quiet period an element must have been unchanged for
	// before it is read. Default 2s. It applies ALWAYS; a driver's readiness
	// probe, when present, only vetoes on top of it.
	SettleWindow time.Duration

	// Disposal is what to do with the source element after capture.
	Disposal DisposalPolicy

	// Policy decides and enriches. nil means TakeAll.
	Policy Policy

	// Session tags every artifact of one sweep, so an interrupted sweep can be
	// rolled back as a group. Empty means a fresh generated id per sweep.
	Session domain.SessionID
}

const (
	defaultBatchSize    = 100
	defaultFlushTimeout = 5 * time.Second
	defaultConcurrency  = 4
	defaultSettleWindow = 2 * time.Second
)

func (c Config) withDefaults() Config {
	if c.Mode == "" {
		c.Mode = IngestModeOneShot
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = defaultFlushTimeout
	}
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.SettleWindow <= 0 {
		c.SettleWindow = defaultSettleWindow
	}
	if c.Policy == nil {
		c.Policy = TakeAll{}
	}
	return c
}

// pathIndex is the narrow read the agent needs from the filesystem-path
// extension: which artifacts sit at a path, and the vfsmeta block of one of
// them. It is what makes the already-captured probe possible, and it is why the
// ingester is the paired agent of that extension and does not work without it
// (ADR-115 П-4). *fspath.CustomIndex satisfies it structurally.
type pathIndex interface {
	Lookup(key string) ([]domain.ArtifactID, error)
	GetByID(id domain.ArtifactID) (json.RawMessage, bool, error)
}

// cursorStore is the narrow slice of the extension's scoped SystemStore the
// watermark lives in (ADR-101): durable, surviving an index rebuild, and not a
// path in anybody's configuration.
type cursorStore interface {
	Put(ctx context.Context, a systemNamedArtifact) error
	Get(ctx context.Context, name string) (domain.ReadHandle, error)
}

// Ingester is the agent that captures data from an external source.
type Ingester interface {
	agent.Agent

	// RunNow performs one sweep immediately, outside the schedule, and reports
	// what it did.
	RunNow(ctx context.Context) (Stats, error)
}

// Stats are the outcome of one sweep.
type Stats struct {
	Seen     int64 // elements the traversal reported
	Captured int64 // artifacts written
	Skipped  int64 // filtered out by the policy
	Deferred int64 // not ready, or the policy said later
	Known    int64 // already captured at this path with this mtime
	Failed   int64 // read or write errors; the source element is left in place
}

func (s Stats) toMap() map[string]int64 {
	return map[string]int64{
		"seen": s.Seen, "captured": s.Captured, "skipped": s.Skipped,
		"deferred": s.Deferred, "known": s.Known, "failed": s.Failed,
	}
}

// New creates an Ingester. User-managed: the host starts it explicitly.
//
// src is the source driver — the agent never touches a filesystem directly, so
// the same code ingests a local folder and an object store. target is the store
// artifacts are written to, paths is the filesystem-path extension's index
// (mandatory: without it the already-captured probe is impossible), cursor is
// the extension's scoped SystemStore.
//
// Returns errs.ErrNotImplemented for Watch mode, and an error when CaptureDirs
// is asked of a driver that does not declare CapDirEntries.
func New(
	src driver.Driver,
	target store.DataStore,
	paths pathIndex,
	cursor cursorStore,
	bus event.Publisher,
	cfg Config,
	opts ...agent.AgentOption,
) (Ingester, error) {
	if src == nil || target == nil || paths == nil || cursor == nil || bus == nil {
		return nil, fmt.Errorf("ingester.New: source, target, path index, cursor store and bus are required")
	}
	cfg = cfg.withDefaults()

	if cfg.Mode == IngestModeWatch {
		return nil, fmt.Errorf("%w: ingester watch mode (ADR-115 П-10)", errs.ErrNotImplemented)
	}
	if cfg.CaptureDirs && !src.Capabilities().Has(driver.CapDirEntries) {
		return nil, fmt.Errorf("ingester.New: CaptureDirs needs a driver declaring CapDirEntries; " +
			"this one reports objects only, so directories — empty ones especially — cannot be captured")
	}

	ing := &ingester{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		src:       src,
		target:    target,
		paths:     paths,
		cursor:    newWatermark(cursor),
		bus:       bus,
		cfg:       cfg,
	}
	if p, ok := src.(driver.ReadinessProber); ok {
		ing.probe = p
	}
	return ing, nil
}

type ingester struct {
	agent.BaseState

	src    driver.Driver
	target store.DataStore
	paths  pathIndex
	cursor *watermark
	bus    event.Publisher
	cfg    Config

	// probe is the driver's optional readiness veto (nil when the driver does
	// not implement it). It never replaces the settle window.
	probe driver.ReadinessProber
}

func (a *ingester) AgentType() string { return "ingester" }

// Validate checks what can be checked without touching the source: the mode,
// the directory capability, and a live context. The sweep takes no lease — one
// instance is one source, and two instances over one source are a host-level
// mistake the agent cannot detect.
func (a *ingester) Validate(ctx context.Context) error {
	if a.cfg.Mode != IngestModeOneShot {
		return fmt.Errorf("%w: ingester mode %q", errs.ErrNotImplemented, a.cfg.Mode)
	}
	return ctx.Err()
}

// Run performs one sweep under the standard agent lifecycle.
func (a *ingester) Run(ctx context.Context) (*domain.AgentResult, error) {
	return agent.RunLeased(ctx, &a.BaseState, a.spec(), func(ctx context.Context) (map[string]int64, error) {
		stats, err := a.sweep(ctx)
		return stats.toMap(), err
	})
}

// RunNow is Run with the typed stats.
func (a *ingester) RunNow(ctx context.Context) (Stats, error) {
	var stats Stats
	_, err := agent.RunLeased(ctx, &a.BaseState, a.spec(), func(ctx context.Context) (map[string]int64, error) {
		var werr error
		stats, werr = a.sweep(ctx)
		return stats.toMap(), werr
	})
	return stats, err
}

// spec configures the lifecycle. No lease: an ingester is bound to one source
// by construction, and the store-wide leases exist for store-wide maintenance.
func (a *ingester) spec() agent.MaintenanceSpec {
	return agent.MaintenanceSpec{
		AgentType:    "ingester",
		Terminal:     event.EventAgentCompleted,
		TerminalMode: agent.TerminalOnSuccess,
		Bus:          a.bus,
		Driver:       a.src,
	}
}

var _ Ingester = (*ingester)(nil)
