// Package scrinium is the front door to a Scrinium store. It turns a
// configuration — a YAML/JSON document or a Config built in code — into
// a live, ready-to-use store, and hands it back as a *ScriniumClient.
//
// A *ScriniumClient IS a store: it embeds store.Store, so Put/Get/Walk
// and the Admin() facet are called directly on it (c.Put(...), not
// c.Store().Put(...)). The only behavioural difference from a
// hand-assembled store is Close: c.Close() cascades, releasing the
// store, its index, and the projection together — the wiring the
// assembler owns because it created those pieces.
//
// Projection is non-nil only when the configuration asked for one
// (a `projection:` section); store-only consumers ignore it.
//
//	c, err := scrinium.Open(ctx, "file:///data/app")
//	defer c.Close()
//	c.Put(ctx, scrinium.Artifact{Payload: r})
//
// Built-in backends register by blank import, as in database/sql; pull
// in the ones a deployment uses (ADR-63):
//
//	import (
//	    _ "scrinium.dev/engine/driver/localfs"
//	    _ "scrinium.dev/engine/index/sqlite"
//	)
//
// Extensions are installed at build time with WithExtension, e.g. the
// by-path filesystem view:
//
//	import "scrinium.dev/x/fspath"
//	c, err := scrinium.Open(ctx, "file:///data/app",
//	    scrinium.WithExtension(fspath.NewExtension()))
package scrinium

import (
	"context"
	"time"

	"scrinium.dev/config/declarative"
	"scrinium.dev/domain"
	"scrinium.dev/engine/store"
	"scrinium.dev/event"
	"scrinium.dev/extension"
	"scrinium.dev/internal/assembly"
	"scrinium.dev/present"
	"scrinium.dev/projection"
)

// ScriniumClient is a live, assembled store. Obtain one from Open /
// Build / Load*; release it with Close.
type ScriniumClient struct {
	// Store is embedded: a *ScriniumClient is a store. Put/Get/Walk/
	// Capacity and Admin() are promoted, so callers use c.Put(...)
	// directly.
	store.Store

	// Projection is the read/write filesystem projection over the
	// store. Nil when the configuration had no `projection:` section —
	// store-only consumers never touch it.
	Projection *projection.Projection

	// MountSession is the boot-unique identifier for this assembly,
	// surfaced for daemons that report it in diagnostics.
	MountSession domain.SessionID

	// Info is assembly metadata for diagnostics (store URI, namespace,
	// editing policy, read-only flag, fresh-create flag).
	Info Info

	// asm is retained for its cascading Close, which unwinds the
	// store, index, and projection in the correct order, and for the
	// init-time RecoveryKit handoff. The embedded store.Store.Close
	// (which does NOT close the index, by design) is shadowed by
	// (*ScriniumClient).Close below.
	asm assembly.Assembly
}

// Artifact is the unit a client stores and reads back. Aliased from the
// domain package so a hello-world program can stay in one import.
type Artifact = domain.Artifact

// AgentResult is the outcome of a RunMaintenance call. Aliased from the
// domain package so single-package programs need not import domain.
type AgentResult = domain.AgentResult

// Event is a single message on the store/agent event bus. Aliased from
// the event package so single-package programs need not import it.
type Event = event.Event

// Data-plane options, re-exported from domain so single-package
// programs need not import domain directly. Rule: every public domain
// option has a scrinium.WithX re-export of the same value-function.
var (
	WithSession   = domain.WithSession   // PutOption
	WithBlobType  = domain.WithBlobType  // PutOption
	WithRetention = domain.WithRetention // PutOption
	WithRouting   = domain.WithRouting   // PutOption
	// WithParentRefs declares an artifact's source artifacts (edges into the
	// content-addressed DAG). The production record itself — relation kind,
	// operation, parameters — comes from the provenance extension's option.
	WithParentRefs = domain.WithParentRefs // PutOption
	WithColdRead   = domain.WithColdRead   // GetOption
)

// Info is assembly metadata an app may surface in diagnostics.
type Info = assembly.Info

// Config is the programmatic configuration shape. Re-exported so
// callers build it without importing the internal package.
type Config = declarative.Config

// StoreSpec describes a single store (driver + index + policy). Re-exported
// so Open/Build callers can construct a Config without the internal package.
type StoreSpec = declarative.StoreSpec

// Mode selects open/init behaviour for Build.
type Mode = assembly.Mode

const (
	ModeOpenOrInit = assembly.ModeOpenOrInit
	ModeOpen       = assembly.ModeOpen
	ModeInit       = assembly.ModeInit
)

// wrap turns an assembled Assembly into the public *ScriniumClient handle.
func wrap(a assembly.Assembly) *ScriniumClient {
	return &ScriniumClient{
		Store:        a.Store(),
		Projection:   a.Projection(),
		MountSession: a.MountSession(),
		Info:         a.Info(),
		asm:          a,
	}
}

// Close cascades: it releases the projection, the store, and the
// index together. It shadows the embedded store.Store.Close, which
// closes only the store (the index lifetime belongs to the host — here
// the assembler — so the cascade lives at this level).
func (c *ScriniumClient) Close() error {
	if c.asm == nil {
		return nil
	}
	return c.asm.Close()
}

// Extensions lists the extensions loaded into this client, as whole
// units (their descriptors), for diagnostics (e.g. a stats page). The
// client deals in extensions, not in the index/store axes they occupy;
// the raw StoreIndex is intentionally never surfaced.
func (c *ScriniumClient) Extensions() []extension.Descriptor { return c.asm.Extensions() }

// SchemaPresenters returns the schema-key → presenter registry assembled
// from the installed extensions (ADR-109), for surfaces that render Ext
// schema blocks.
func (c *ScriniumClient) SchemaPresenters() present.Registry { return c.asm.SchemaPresenters() }

// RecoveryKit returns the recovery-kit bytes produced when this client
// freshly initialised an encrypted store, and true. For a store that
// was opened (not created) or is unencrypted it returns (nil, false).
//
// The host MUST persist a returned kit out of band: it is the only path
// back into the store if the passphrase is lost. The same bytes are
// available later through the Admin facet's ExportRecoveryKit; this
// accessor is the convenience for the create path, paired with
// Info.Created.
func (c *ScriniumClient) RecoveryKit() ([]byte, bool) { return c.asm.RecoveryKit() }

// RunMaintenance builds the registered agent named kind (decoding cfg
// via its factory) and runs it once, returning the agent's result. It is
// the always-available manual trigger (ADR-69 level 1): no resident
// goroutine and no scheduler — the host calls it on its own cadence.
//
// kind must be registered. Agents register by blank import, the same way
// drivers and indexes do (ADR-63); pull in the ones a deployment uses:
//
//	import _ "scrinium.dev/engine/agent/gc"
//	res, err := c.RunMaintenance(ctx, "gc", gc.GCConfig{})
func (c *ScriniumClient) RunMaintenance(ctx context.Context, kind string, cfg any) (*AgentResult, error) {
	return c.asm.RunMaintenance(ctx, kind, cfg)
}

// Subscribe registers fn to receive every store and agent event and
// returns a function that removes it (idempotent, safe from any
// goroutine). The bus is synchronous: fn runs inline on the publishing
// goroutine, so keep it quick and non-blocking. Events emitted during
// assembly are missed by a post-build Subscribe; to catch those, pass
// WithEventHandler at Open/Build time.
//
//	unsub := c.Subscribe(func(e scrinium.Event) { … })
//	defer unsub()
func (c *ScriniumClient) Subscribe(fn func(Event)) func() {
	return c.asm.Subscribe(fn)
}

// ScheduleEvery registers agent kind to run every interval through the
// built-in scheduler, which the client ticks on real time. Requires
// WithStandardScheduler at Open/Build; without it there is no resident
// scheduler and this returns an error. The agent is rebuilt from the
// registry on each run, so kind must be registered (blank import).
//
//	c, _ := scrinium.Open(ctx, uri, scrinium.WithStandardScheduler())
//	c.ScheduleEvery("gc", time.Hour, gc.GCConfig{})
func (c *ScriniumClient) ScheduleEvery(kind string, every time.Duration, cfg any) error {
	return c.asm.ScheduleEvery(kind, every, cfg)
}

// ScheduleCron registers agent kind to run on a cron expression through
// the built-in scheduler. Requires WithStandardScheduler at Open/Build
// and a cron adapter enabled with cron.Enable (from
// scrinium.dev/engine/agent/cron); without either it returns an error.
//
//	c, _ := scrinium.Open(ctx, uri, scrinium.WithStandardScheduler(), cron.Enable())
//	c.ScheduleCron("scrub", "0 3 * * *", scrub.ScrubConfig{})
func (c *ScriniumClient) ScheduleCron(kind string, expr string, cfg any) error {
	return c.asm.ScheduleCron(kind, expr, cfg)
}

// Open assembles a store from a single driver URI, creating it if
// absent (ModeOpenOrInit). The simplest entry point — no config
// document, no projection.
//
//	c, err := scrinium.Open(ctx, "file:///data/app")
func Open(ctx context.Context, driverURI string, opts ...BuildOption) (*ScriniumClient, error) {
	return Build(ctx, Config{Store: &StoreSpec{Driver: driverURI}}, opts...)
}

// Build assembles a store from a programmatic Config.
func Build(ctx context.Context, cfg Config, opts ...BuildOption) (*ScriniumClient, error) {
	a, err := assembly.Build(ctx, cfg, opts...)
	if err != nil {
		return nil, err
	}
	return wrap(a), nil
}

// BuildOption tunes Build/Open/Load* (e.g. WithMode, WithExtension).
type BuildOption = assembly.BuildOption

// WithMode sets the open/init behaviour (default ModeOpenOrInit).
func WithMode(m Mode) BuildOption { return assembly.WithMode(m) }

// WithEventHandler registers an event handler before assembly begins, so
// it observes events emitted during Open/Init as well as every later
// store and agent event. For subscriptions added after Open, use
// (*ScriniumClient).Subscribe. A nil handler is ignored.
func WithEventHandler(fn func(Event)) BuildOption { return assembly.WithEventHandler(fn) }

// WithForceReinit assembles a FRESH store over an existing one, destroying it.
// The descriptor, every system artifact, every manifest and the index's content
// go; blob payload stays behind as reclaimable orphans until GC reaches it. The
// operation is irreversible and assumes nothing else is using the location.
//
// It requires WithMode(ModeInit) — under the default ModeOpenOrInit an existing
// store simply opens, so a caller passing this alone would be holding a
// destructive flag that never fires. Add WithPurgeOnReinit to remove the
// payload as well and leave the location bare.
//
//	c, err := scrinium.Open(ctx, uri,
//	    scrinium.WithMode(scrinium.ModeInit),
//	    scrinium.WithForceReinit(), scrinium.WithPurgeOnReinit())
func WithForceReinit() BuildOption { return assembly.WithForceReinit() }

// WithPurgeOnReinit extends WithForceReinit to blob payload: after it, nothing
// of the previous store survives at the location — no bytes waiting for GC, no
// tombstone markers. Requires WithForceReinit.
func WithPurgeOnReinit() BuildOption { return assembly.WithPurgeOnReinit() }

// WithStandardScheduler runs the built-in scheduler: one goroutine ticks
// it on real time and runs due agents, stopped on Close. Without it the
// client keeps no resident goroutine — agents run only via RunMaintenance.
// Add schedules with ScheduleEvery / ScheduleCron. Hosts that want to own
// the clock build on the primitives (agent.Scheduler) directly.
func WithStandardScheduler() BuildOption { return assembly.WithStandardScheduler() }

// WithSchedule sets, at build time, the schedule of an agent kind. expr is
// a cron string ("0 3 * * *"; requires cron.Enable) or an interval string
// ("6h"). It overrides a schedule declared in config for the kind, and a
// repeat call for the same kind replaces it (replace-by-kind). Declaring a
// schedule raises the scheduler even without WithStandardScheduler.
func WithSchedule(kind, expr string) BuildOption { return assembly.WithSchedule(kind, expr) }

// WithAgentConfig overrides, at build time, the kind-specific config handed
// to an agent's factory. A repeat call for the same kind replaces it.
func WithAgentConfig(kind string, cfg any) BuildOption { return assembly.WithAgentConfig(kind, cfg) }

// WithExtension installs extensions into the client being built, e.g.
// scrinium.WithExtension(fspath.NewExtension()) to enable the by-path
// projection view. It works with Open, Build, and the Load* functions.
// Accumulates across calls; a nil extension is ignored.
func WithExtension(exts ...extension.Extension) BuildOption {
	return assembly.WithExtension(exts...)
}

// WithPassphrase supplies the encryption passphrase provider for opening or
// unlocking an encrypted store imperatively (Open/Build), without a config
// policy declaring it. Takes precedence over a policy-derived passphrase.
func WithPassphrase(p domain.PassphraseProvider) BuildOption {
	return assembly.WithPassphrase(p)
}

// LoadYAML / LoadInitYAML / LoadOrInitYAML assemble from a YAML
// configuration document. JSON variants mirror them. opts are the same
// build-time options Open/Build accept (e.g. WithExtension), applied on
// top of the parsed config.
func LoadYAML(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadYAML(ctx, data, opts...))
}

func LoadInitYAML(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadInitYAML(ctx, data, opts...))
}

func LoadOrInitYAML(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadOrInitYAML(ctx, data, opts...))
}

func LoadJSON(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadJSON(ctx, data, opts...))
}

func LoadInitJSON(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadInitJSON(ctx, data, opts...))
}

func LoadOrInitJSON(ctx context.Context, data []byte, opts ...BuildOption) (*ScriniumClient, error) {
	return wrapErr(assembly.LoadOrInitJSON(ctx, data, opts...))
}

// Explain resolves a YAML/JSON config and returns a human-readable
// description of the store it would assemble (driver, index, policy,
// extensions) without building it — for debugging "what does my config
// produce". Thin facade over the internal assembler.
func Explain(ctx context.Context, data []byte) ([]byte, error) {
	return assembly.Explain(ctx, data)
}

func wrapErr(a assembly.Assembly, err error) (*ScriniumClient, error) {
	if err != nil {
		return nil, err
	}
	return wrap(a), nil
}
