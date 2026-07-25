package assembly

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"os"
	decl "scrinium.dev/config/declarative"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/hashing"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// store.go — store-construction phases (openStore, composeWrappers) and the
// store/index helpers they rely on: open/init, policy→StoreConfig, the hash
// registry, the unsupported-policy guard, and the wrappedStore composite.

// openStore derives the StoreConfig and passphrase from policy (a
// WithPassphrase option wins), creates the shared event bus and attaches
// build-time handlers before anything publishes, then opens or initialises
// the store. It records the store rollback and fails fast on a locked store.
func (bs *buildState) openStore() error {
	// StoreConfig + passphrase from the policy.
	cfg, _ := decl.StoreConfigFromPolicy(bs.spec.Policy)
	pp, err := passphraseProvider(bs.ctx, bs.spec.Policy)
	if err != nil {
		return fmt.Errorf("scrinium: passphrase: %w", err)
	}
	if bs.opts.passphrase != nil {
		pp = bs.opts.passphrase // WithPassphrase: option takes precedence over policy
	}

	// Event bus: shared by the store and the agents the assembler wires, so
	// a host can subscribe to both through one channel. Build-time handlers
	// (WithEventHandler) are attached now, before anything publishes, so
	// they observe events emitted during assembly.
	bus := event.NewEventBus()
	for _, h := range bs.opts.eventHandlers {
		bus.Subscribe(h)
	}
	bs.bus = bus

	storeOpts := []store.StoreOption{
		store.WithStoreIndex(bs.idx),
		store.WithHashRegistry(hashRegistry()),
		store.WithConfig(cfg),
		store.WithPublisher(bus),
	}
	if pp != nil {
		storeOpts = append(storeOpts, store.WithPassphrase(pp), store.WithAutoUnlock())
	}
	reinitOpts, err := reinitOptions(bs.mode, bs.opts)
	if err != nil {
		return err
	}
	storeOpts = append(storeOpts, reinitOpts...)

	st, created, kit, err := openOrInitStore(bs.ctx, bs.drv, bs.mode, storeOpts)
	if err != nil {
		return err
	}
	bs.st = st
	bs.created = created
	bs.kit = kit
	bs.cleanups = append(bs.cleanups, func() {
		if err := st.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "scrinium: store close on rollback: %v\n", err)
		}
	})
	if st.State() == domain.StateLocked {
		return fmt.Errorf("scrinium: store is locked; check the encryption passphrase")
	}
	return nil
}

// composeWrappers applies the behavior axis (Tier 3): the store's data
// plane is decorated by each extension wrapper innermost-first, then the
// full Store is preserved via a composite so administrative/maintenance
// operations pass through unchanged. The stack is validated (ADR-75) before
// composing. A no-wrapper build leaves the store untouched.
func (bs *buildState) composeWrappers() error {
	if len(bs.wrapFactories) == 0 {
		return nil
	}
	descs := make([]wrapper.Descriptor, len(bs.wrapFactories))
	for i, f := range bs.wrapFactories {
		descs[i] = f.Descriptor()
	}
	if verr := wrapper.Validate(descs, wrapper.ValidateOptions{}); verr != nil {
		return fmt.Errorf("scrinium: wrapper composition: %w", verr)
	}
	data := store.DataStore(bs.st)
	for _, f := range bs.wrapFactories {
		d, werr := f.Wrap(data, wrapper.Deps{Publisher: bs.bus})
		if werr != nil {
			return fmt.Errorf("scrinium: apply extension wrapper: %w", werr)
		}
		data = d
	}
	bs.st = wrappedStore{DataStore: data, AdminStore: bs.st}
	return nil
}

// openOrInitStore opens or initialises the store per mode. It reports
// whether the store was freshly created and, for a fresh encrypted
// store, the recovery-kit bytes the host must persist (nil otherwise).
func openOrInitStore(
	ctx context.Context,
	drv driver.Driver,
	mode buildMode,
	opts []store.StoreOption,
) (st store.Store, created bool, kit []byte, err error) {
	switch mode {
	case modeOpen:
		st, err := store.OpenStore(ctx, drv, opts...)
		if err != nil {
			return nil, false, nil, fmt.Errorf("scrinium: open store: %w", err)
		}
		return st, false, nil, nil
	case modeInit:
		return initStore(ctx, drv, opts)
	case modeOpenOrInit:
		st, err := store.OpenStore(ctx, drv, opts...)
		if err == nil {
			return st, false, nil, nil
		}
		if !isNotFound(err) {
			return nil, false, nil, fmt.Errorf("scrinium: open store: %w", err)
		}
		return initStore(ctx, drv, opts)
	default:
		return nil, false, nil, fmt.Errorf("scrinium: unknown build mode %d", mode)
	}
}

// initStore creates a fresh store and surfaces the recovery kit. For an
// unencrypted store InitStore returns no kit (empty slice); for an
// encrypted one it returns the kit the host must persist out of band —
// the assembler hands it up through the Assembly (Info.Created +
// RecoveryKit).
func initStore(ctx context.Context, drv driver.Driver, opts []store.StoreOption) (store.Store, bool, []byte, error) {
	st, kit, err := store.InitStore(ctx, drv, opts...)
	if err != nil {
		return nil, false, nil, fmt.Errorf("scrinium: init store: %w", err)
	}
	return st, true, kit, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, errs.ErrStoreNotFound)
}

func hashRegistry() domain.HashRegistry {
	return hashing.NewHashRegistry().
		Register("sha256", func() hash.Hash { return sha256.New() })
}

// wrappedStore presents a full Store whose data plane is decorated by one
// or more behavior wrappers (Tier 3) while administrative and maintenance
// operations pass through to the underlying store unchanged. DataStore
// and AdminStore have disjoint method sets, so embedding both is
// unambiguous (engine/store/store.go).
type wrappedStore struct {
	store.DataStore
	store.AdminStore
}

// reinitOptions translates the two destructive build options into store
// options, and refuses the combinations in which they would be a lie.
//
// Both are irreversible and both only act inside InitStore, so the mode is not
// a detail to be inferred: under ModeOpenOrInit an existing store opens and the
// flag never fires, which is the worst possible shape for a destructive
// setting — armed in the caller's mind, inert in fact. ModeInit is therefore
// required rather than assumed, and the error says so.
func reinitOptions(mode buildMode, opts *Options) ([]store.StoreOption, error) {
	switch {
	case !opts.forceReinit && !opts.purgeOnReinit:
		return nil, nil
	case !opts.forceReinit:
		return nil, fmt.Errorf(
			"scrinium: WithPurgeOnReinit requires WithForceReinit " +
				"(a purge is how a forced re-init destroys payload, not a mode of its own)")
	case mode != modeInit:
		return nil, fmt.Errorf(
			"scrinium: WithForceReinit requires WithMode(ModeInit); " +
				"under any other mode an existing store is opened and nothing is destroyed")
	}

	out := []store.StoreOption{store.WithForceReinit()}
	if opts.purgeOnReinit {
		out = append(out, store.WithPurgeOnReinit())
	}
	return out, nil
}
