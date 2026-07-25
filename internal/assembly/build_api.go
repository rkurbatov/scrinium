package assembly

import (
	"context"
	decl "scrinium.dev/config/declarative"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/event"
	"scrinium.dev/extension"
)

// Mode selects what Build does when the described store does or does
// not already exist.
type Mode int

const (
	// ModeOpenOrInit opens the store, creating it if absent. The
	// forgiving default — good for "just give me a working store".
	ModeOpenOrInit Mode = iota
	// ModeOpen opens an existing store and fails if it is absent.
	ModeOpen
	// ModeInit creates a fresh store and fails if one already exists.
	ModeInit
)

func (m Mode) internal() buildMode {
	switch m {
	case ModeOpen:
		return modeOpen
	case ModeInit:
		return modeInit
	default:
		return modeOpenOrInit
	}
}

// Build assembles a store from a programmatically-constructed Config,
// returning a live assembly.Assembly. It is the direct counterpart to
// the Load* functions: those parse bytes into a Config and call Build;
// callers holding a Config in hand call Build without round-tripping
// through YAML.
//
// Build applies defaults and validates before assembling, so a
// half-filled Config (just a Store driver, say) becomes a fully
// defaulted store. The mode defaults to ModeOpenOrInit; pass
// WithMode to change it.
//
//	asm, err := Build(ctx, decl.Config{
//	    Store: &decl.StoreSpec{Driver: "file:///data/app"},
//	})
//
// The Config is treated as owned by Build for the duration of the call
// (defaults are applied in place); do not mutate it concurrently.
func Build(ctx context.Context, cfg decl.Config, opts ...BuildOption) (Assembly, error) {
	o := Options{mode: ModeOpenOrInit}
	for _, opt := range opts {
		opt(&o)
	}
	if err := prepare(&cfg); err != nil {
		return nil, err
	}
	return build(ctx, &cfg, &o)
}

// BuildOption tunes a Build call.
type BuildOption func(*Options)

// Options is the opaque set of build-time options a BuildOption mutates.
// The type is exported so feature packages in subtrees (e.g. the cron
// adapter) can define their own BuildOption, but its fields are private:
// applications use the With* helpers, and feature adapters use the
// exported setter SPI (e.g. SetCronParser). This keeps option invariants
// owned by the assembler.
type Options struct {
	mode              Mode
	eventHandlers     []func(event.Event)
	standardScheduler bool
	cronParser        agent.CronParser
	// schedules and agentConfigs are build-time, per-kind overrides
	// (Reference §9.5 stage 2). schedules maps an agent kind to a schedule
	// expression (cron string or interval string); agentConfigs maps a kind
	// to its config. Both are last-wins per kind (replace-by-kind). They are
	// applied to the scheduler during assembly (§9.7).
	schedules    map[string]string
	agentConfigs map[string]any
	// extensions are the per-build extensions supplied via WithExtension.
	// They are installed alongside any registered process-wide
	// (RegisterExtension); the assembler special-cases none (ADR-88/98).
	extensions []extension.Extension
	// passphrase, when set via WithPassphrase, supplies the encryption
	// passphrase provider directly (imperative path); it takes precedence
	// over one derived from the policy.
	passphrase domain.PassphraseProvider
	// forceReinit and purgeOnReinit carry the two destructive store options
	// through to InitStore. They are only meaningful with ModeInit, and the
	// store phase refuses any other combination rather than accepting a flag
	// that would never fire (see openStore).
	forceReinit   bool
	purgeOnReinit bool
}

// SetCronParser installs the cron expression parser used by ScheduleCron.
// It is the SPI for cron adapter packages (scrinium.dev/engine/agent/cron
// calls it from the option it exports); applications do not call it
// directly.
func (o *Options) SetCronParser(p agent.CronParser) { o.cronParser = p }

// WithPassphrase supplies the encryption passphrase provider directly
// (imperative path), taking precedence over a policy-derived one. Use it to
// open or unlock an encrypted store without a policy declaring the passphrase.
func WithPassphrase(p domain.PassphraseProvider) BuildOption {
	return func(o *Options) { o.passphrase = p }
}

// WithMode sets the open/init behaviour (default ModeOpenOrInit).
func WithMode(m Mode) BuildOption {
	return func(o *Options) { o.mode = m }
}

// WithForceReinit lets an init assemble over an existing store, DESTROYING it:
// descriptor, system artifacts, manifests and index content go, blob payload
// stays as reclaimable orphans (store.WithForceReinit has the full account).
// The operation is irreversible.
//
// It requires WithMode(ModeInit). Not the forgiving default and not ModeOpen:
// under ModeOpenOrInit an existing store simply opens, so the flag would sit
// there looking armed and never fire — a shape that invites exactly one kind of
// accident. Being explicit means the destructive intent and the mode that can
// act on it are stated together.
func WithForceReinit() BuildOption {
	return func(o *Options) { o.forceReinit = true }
}

// WithPurgeOnReinit extends WithForceReinit to blob payload: nothing of the old
// store survives, not even bytes waiting for GC. Requires WithForceReinit.
func WithPurgeOnReinit() BuildOption {
	return func(o *Options) { o.purgeOnReinit = true }
}

// WithEventHandler registers an event handler before assembly begins, so
// it observes events emitted during Build/Init as well as every later
// store and agent event. Pass it more than once to register several. For
// subscriptions added after the client exists, use the client's Subscribe
// (which returns an unsubscribe). A nil handler is ignored.
func WithEventHandler(fn func(event.Event)) BuildOption {
	return func(o *Options) {
		if fn != nil {
			o.eventHandlers = append(o.eventHandlers, fn)
		}
	}
}

// WithStandardScheduler runs the built-in scheduler (ADR-69 level 2): one
// goroutine ticks the interval-based primitive on real time and runs due
// agents, and it is stopped on Close. Without it there is no resident
// goroutine (level 1): agents run only when the host calls RunMaintenance.
// Schedules are added with the client's ScheduleEvery/ScheduleCron. A host
// that wants to own the clock itself drives agent.Scheduler directly on the
// primitives, not through the client (level 3).
func WithStandardScheduler() BuildOption {
	return func(o *Options) { o.standardScheduler = true }
}

// WithSchedule sets, at build time, the schedule of an agent kind: expr is
// either a cron string ("0 3 * * *", requires a cron adapter) or an
// interval string parseable by time.ParseDuration ("6h"). It overrides any
// schedule the config declared for the kind, and a repeat call for the same
// kind replaces it (replace-by-kind, Reference §9.5/§9.7). Declaring a
// schedule raises the scheduler even without WithStandardScheduler. An empty
// kind or expr is ignored; the kind is validated during assembly.
func WithSchedule(kind, expr string) BuildOption {
	return func(o *Options) {
		if kind == "" || expr == "" {
			return
		}
		if o.schedules == nil {
			o.schedules = make(map[string]string)
		}
		o.schedules[kind] = expr
	}
}

// WithAgentConfig overrides, at build time, the kind-specific config handed
// to an agent's factory. A repeat call for the same kind replaces it. An
// empty kind is ignored.
func WithAgentConfig(kind string, cfg any) BuildOption {
	return func(o *Options) {
		if kind == "" {
			return
		}
		if o.agentConfigs == nil {
			o.agentConfigs = make(map[string]any)
		}
		o.agentConfigs[kind] = cfg
	}
}

// WithExtension installs one or more extensions into the client being
// built, in addition to any registered process-wide (RegisterExtension).
// Each is installed as a whole (ADR-88); its parts — index axis, view
// (ViewProvider), behavior wrapper, paired agents — are discovered by
// assertion at assembly (ADR-98). It works on every build entry point
// (Build, Open, and the Load* functions, which thread it through). Pass it
// more than once to accumulate; a nil extension is ignored.
func WithExtension(exts ...extension.Extension) BuildOption {
	return func(o *Options) {
		for _, e := range exts {
			if e != nil {
				o.extensions = append(o.extensions, e)
			}
		}
	}
}
