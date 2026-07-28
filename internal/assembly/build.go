package assembly

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	decl "scrinium.dev/config/declarative"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/errs"
	"scrinium.dev/event"
	"scrinium.dev/extension"
	"scrinium.dev/present"
	"scrinium.dev/projection"
)

type buildMode int

const (
	modeOpen buildMode = iota
	modeInit
	modeOpenOrInit
)

// build turns a validated, defaulted Config into an assembled stack. It
// assembles the single-store path (the one the engine fully supports
// today); everything that depends on not-yet-wired components returns
// errs.ErrNotImplemented with a pointer to the milestone chunk that
// lands it.
func build(ctx context.Context, c *decl.Config, opts *Options) (Assembly, error) {
	if len(c.Stores) > 0 {
		return nil, fmt.Errorf("scrinium: multistore assembly is not wired yet (M4/S1): %w", errs.ErrNotImplemented)
	}
	if c.Store == nil {
		return nil, fmt.Errorf("scrinium: no store to build")
	}
	return buildSingle(ctx, c, opts)
}

func buildSingle(ctx context.Context, c *decl.Config, opts *Options) (_ Assembly, retErr error) {
	bs := &buildState{
		ctx:           ctx,
		c:             c,
		spec:          c.Store,
		mode:          opts.mode.internal(),
		opts:          opts,
		providedViews: map[string]customindex.ProvidedView{},
		presenters:    present.Registry{},
		stopTicker:    func() {},
	}
	if err := decl.GuardUnsupportedPolicy(bs.spec.Policy); err != nil {
		return nil, err
	}

	// On any failure below, unwind the cleanups gathered so far in LIFO
	// order. On success they are retained — closeFn folds them into the
	// Assembly's own teardown.
	defer func() {
		if retErr == nil {
			return
		}
		for i := len(bs.cleanups) - 1; i >= 0; i-- {
			bs.cleanups[i]()
		}
	}()

	// Each phase reads what earlier phases populated on bs and records its
	// own rollback cleanup; the order is load-bearing (index before store
	// open, store open before wrappers/env, etc.) so it stays explicit.
	if err := bs.dialBackends(); err != nil {
		return nil, err
	}
	if err := bs.installExtensions(); err != nil {
		return nil, err
	}
	if err := bs.openStore(); err != nil {
		return nil, err
	}
	if err := bs.composeWrappers(); err != nil {
		return nil, err
	}
	if err := bs.wireExtensionEnv(); err != nil {
		return nil, err
	}
	if err := bs.buildScheduler(); err != nil {
		return nil, err
	}

	bs.mountSession = domain.NewMountSessionID()
	projFacade, err := bs.buildProjection()
	if err != nil {
		return nil, err
	}

	return bs.assemble(projFacade), nil
}

// buildState threads the accumulating single-store assembly across the
// buildSingle phases. Each phase populates the fields later phases read and
// appends to cleanups; buildSingle's deferred rollback unwinds cleanups
// (LIFO) on failure, and assemble folds them into the Assembly's closeFn on
// success. It is internal to the assembler — not the shape New returns.
type buildState struct {
	ctx  context.Context
	c    *decl.Config
	spec *decl.StoreSpec
	mode buildMode
	opts *Options

	cleanups []func()

	drv          driver.Driver
	idx          index.StoreIndex
	bus          event.EventBus
	st           store.Store
	created      bool
	kit          []byte
	mountSession domain.SessionID

	// Extension contributions discovered in installExtensions and consumed
	// by later phases. exts is the installed set (registry + WithExtension);
	// the rest are the per-axis contributions unioned across it.
	exts          []extension.Extension
	loadedExts    []extension.Descriptor
	wrapFactories []wrapper.Factory
	extAgents     []extension.Agent
	providedViews map[string]customindex.ProvidedView
	presenters    present.Registry

	agentDeps  agent.AgentDeps
	sched      agent.Scheduler
	stopTicker func()
}

// assemble builds the LIFO closeFn over the live resources, fills the
// diagnostic Info from the optional namer capabilities, and returns the
// constructed Assembly.
func (bs *buildState) assemble(projFacade *projection.Projection) Assembly {
	// closeFn unwinds in dependency order — scheduler first (its ticker
	// touches the store), then projection, store, index — and collects every
	// error rather than only the first, so one failing Close cannot mask
	// another. Idempotency is the assembly's job.
	closeFn := func() error {
		var errs []error
		if bs.sched != nil {
			bs.stopTicker()
			if err := bs.sched.Stop(context.Background()); err != nil {
				errs = append(errs, err)
			}
		}
		// A parting checkpoint of the index (ADR-118). It makes the next
		// open a restore plus a tail instead of a walk over every manifest,
		// and for a store whose index is ephemeral (Paranoid, ADR-119) that
		// is the difference between opening in a moment and re-reading the
		// whole corpus with the key.
		//
		// Deliberately best-effort and deliberately silent about failure:
		// the recovery procedure is required to work with a checkpoint of
		// any age and with none at all, so a checkpoint that did not happen
		// costs time and nothing else. It runs after the scheduler has
		// stopped (no agent is writing) and before the store closes (the key
		// is still there).
		bs.finalCheckpoint()
		if projFacade != nil {
			if err := projFacade.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if err := bs.st.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := bs.idx.Close(); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}

	info := Info{StoreURI: bs.spec.Driver, Created: bs.created}
	// Backend names for diagnostics, via the optional namer capabilities.
	// Both drv and idx are the raw constructed instances here, before any
	// store-internal wrapping, so the assertions reach the concrete backends.
	if n, ok := bs.drv.(driver.DriverNamer); ok {
		info.StoreDriver = n.DriverName()
	}
	if n, ok := bs.idx.(index.DriverNamer); ok {
		info.IndexDriver = n.DriverName()
	}
	if effProj := bs.c.Projection; effProj != nil {
		info.Editing = effProj.Editing
		info.ReadOnly = effProj.ReadOnly
	}

	return &asm{
		store:        bs.st,
		index:        bs.idx,
		proj:         projFacade,
		mountSession: bs.mountSession,
		info:         info,
		recoveryKit:  bs.kit,
		closeFn:      closeFn,
		agentDeps:    bs.agentDeps,
		bus:          bs.bus,
		sched:        bs.sched,
		cronParser:   bs.opts.cronParser,
		extensions:   bs.loadedExts,
		presenters:   bs.presenters,
	}
}

// finalCheckpoint takes one checkpoint on the way out, if the deployment has
// the checkpoint agent available at all. Errors are logged, never returned:
// see the call site for why a missing checkpoint is not a failure.
func (bs *buildState) finalCheckpoint() {
	ctx, cancel := context.WithTimeout(context.Background(), finalCheckpointBudget)
	defer cancel()

	ag, err := agent.Build("checkpoint", bs.st, nil, bs.agentDeps)
	if err != nil {
		// The agent is not linked into this build; nothing to do.
		return
	}
	if _, err := bs.st.RunMaintenance(ctx, ag); err != nil && bs.agentDeps.Logger != nil {
		bs.agentDeps.Logger.LogAttrs(ctx, slog.LevelInfo,
			"final checkpoint skipped", slog.String("reason", err.Error()))
	}
}

// finalCheckpointBudget bounds the parting checkpoint so a shutdown cannot
// hang on a slow or unreachable backend. Exceeding it is the same as not
// having a checkpoint: the next open pays with a full pass.
const finalCheckpointBudget = 30 * time.Second
