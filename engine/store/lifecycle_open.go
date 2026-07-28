package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/internal/aead"
	"scrinium.dev/engine/store/internal/crypto"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/engine/store/internal/keyring"
	"scrinium.dev/engine/store/internal/reconcile"
	"scrinium.dev/errs"
)

// OpenStore opens an existing Store at the Location served by drv.
//
// Behaviour:
//  1. Read both descriptor replicas (store.descriptor,
//     store.descriptor.backup) and reconcile them. If
//     both are absent → errs.ErrStoreNotFound. If both are
//     unrecoverable (corrupted, or one corrupted + one absent)
//     → errs.ErrStoreCorrupted. If both are valid but content
//     differs at the same Sequence →
//     errs.ErrDescriptorSplitBrain. Otherwise the canonical
//     replica is selected (sequence-wins; equal-content
//     short-circuit) and any heal action is performed against
//     the Driver before proceeding.
//  2. Validate that a StoreIndex was provided via WithStoreIndex.
//     core never opens an index itself; the caller is responsible
//     for the dependency. Missing → error.
//  4. Load the active StoreConfig (the max system/config version).
//     Defaults are applied; immutable parameters are validated.
//  5. Validate WithConfig (when supplied) against the active
//     config: immutable mismatch → errs.ErrConfigMismatch.
//     A caller without WithConfig accepts the on-disk config
//     as-is — a legitimate scenario for diagnostic tools and
//     projection-only consumers.
//  6. State machine and bootstrap:
//     - DEKEncrypted=false: Plain DEK in descriptor, used
//     directly. Run Orphan Scan, transition to
//     StateUnlocked.
//     - DEKEncrypted=true + WithAutoUnlock: invoke the
//     configured PassphraseProvider with Reason="unlock",
//     unwrap DEK, run Orphan Scan, transition to
//     StateUnlocked.
//     - DEKEncrypted=true without WithAutoUnlock: skip the
//     Orphan Scan and transition to StateLocked. The next
//     Store.Unlock call completes bootstrap.
//
// The split between an encrypted DEK and encrypted manifests is
// deliberate: they are independent axes of the configuration, and a
// Store with a passphrase-protected DEK plus Plain manifests is a
// useful intermediate.
func OpenStore(ctx context.Context, drv driver.Driver, opts ...StoreOption) (Store, error) {
	if drv == nil {
		return nil, errors.New("store.OpenStore: nil driver")
	}

	// wrap is the local error-prefix closure for this function. The
	// 13+ fmt.Errorf("store.OpenStore: ...: %w") sites threaded the
	// same prefix manually; centralising it here means a single
	// edit if the prefix ever changes.
	wrap := func(stage string, err error) error {
		if stage == "" {
			return fmt.Errorf("store.OpenStore: %w", err)
		}
		return fmt.Errorf("store.OpenStore: %s: %w", stage, err)
	}

	// --- Resolve options ---

	o := storeOptions{}
	for _, fn := range opts {
		fn(&o)
	}

	// --- StoreIndex dependency ---
	//
	// Required up-front: readSystemConfig needs it; refusing here
	// gives a clearer error than failing later.

	idx := o.storeIndex
	if idx == nil {
		return nil, fmt.Errorf(
			"store.OpenStore: WithStoreIndex is required (see DI Example)")
	}
	if o.hashRegistry == nil {
		return nil, fmt.Errorf(
			"store.OpenStore: WithHashRegistry is required to read system.config")
	}

	// --- Read, reconcile, and heal the descriptor ---

	desc, err := loadCanonicalDescriptor(ctx, drv, o.hashRegistry, optsLogger(o, "store"), wrap)
	if err != nil {
		return nil, err
	}

	// --- Load and validate the active StoreConfig ---

	active, overlay, cfgSeq, err := loadActiveConfig(ctx, drv, o, wrap)
	if err != nil {
		return nil, err
	}

	// A store created Paranoid must not be reopened against an index
	// that persists readable on disk — the config is authoritative and
	// the index may differ from the one it was created with (ADR-56).
	if err := guardIndexAtRest(active.ManifestCrypto, idx); err != nil {
		return nil, wrap("", err)
	}

	// --- Branch on DEK protection state ---

	if !desc.DEKEncrypted {
		// Plain DEK: descriptor carries the raw key. Bootstrap
		// proceeds straight to Unlocked.
		s, err := buildStore(ctx, o, drv, idx, active, desc, desc.DEK)
		if err != nil {
			return nil, wrap("", err)
		}
		s.sessionOverlay = overlay
		s.lastConfigSeq = cfgSeq
		s.crypto.PromoteResolverIfDefault()
		if err := settleIndex(ctx, s, planIndexOpen(ctx, active.ManifestCrypto, idx, drv, o.allowRecovery)); err != nil {
			return nil, wrap("", err)
		}
		if err := unlockBootstrap(ctx, s, o.publisher); err != nil {
			return nil, wrap("", err)
		}
		s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "store opened",
			storeIDAttr(s), stateAttr(domain.StateUnlocked),
			slog.Bool("encrypted_dek", false))
		s.startLiveness(o.livenessInterval)
		return s, nil
	}

	// Encrypted DEK below.

	if !o.autoUnlock {
		// Stop in StateLocked — caller will follow up with
		// Store.Unlock. Orphan Scan is deferred until DEK is
		// available so the locked Store is a strictly minimal
		// in-memory shell.
		s, err := buildStore(ctx, o, drv, idx, active, desc, nil /* dek */)
		if err != nil {
			return nil, wrap("", err)
		}
		s.sessionOverlay = overlay
		s.lastConfigSeq = cfgSeq
		s.stateMu.Lock()
		s.state = domain.StateLocked
		s.stateMu.Unlock()
		s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "store opened",
			storeIDAttr(s), stateAttr(domain.StateLocked),
			slog.Bool("encrypted_dek", true))
		s.startLiveness(o.livenessInterval)
		return s, nil
	}

	// AutoUnlock: invoke the provider, derive KEK, unwrap DEK.
	if o.passphrase == nil {
		return nil, fmt.Errorf("store.OpenStore: %w: WithAutoUnlock requires WithPassphrase",
			errs.ErrPassphraseRequired)
	}
	passphrase, err := crypto.CallProvider(ctx, o.passphrase, domain.PassphraseHint{
		StoreID: desc.StoreID,
		Reason:  "unlock",
	})
	if err != nil {
		return nil, wrap("", err)
	}
	if desc.KDFParams == nil {
		// Defensive: a descriptor with DEKEncrypted=true must
		// have KDFParams (Validate enforces). Reaching this
		// branch means the Validate contract has drifted.
		aead.Wipe(passphrase)
		return nil, fmt.Errorf("%w: descriptor reports DEKEncrypted=true without KDFParams",
			errs.ErrStoreCorrupted)
	}
	dek, err := keyring.UnwrapDEK(desc.DEK, *desc.KDFParams, passphrase)
	aead.Wipe(passphrase)
	if err != nil {
		return nil, wrap("", err)
	}

	s, err := buildStore(ctx, o, drv, idx, active, desc, dek)
	if err != nil {
		aead.Wipe(dek)
		return nil, wrap("", err)
	}
	s.sessionOverlay = overlay
	s.lastConfigSeq = cfgSeq
	s.crypto.PromoteResolverIfDefault()
	if err := settleIndex(ctx, s, planIndexOpen(ctx, active.ManifestCrypto, idx, drv, o.allowRecovery)); err != nil {
		aead.Wipe(dek)
		return nil, wrap("", err)
	}
	if err := unlockBootstrap(ctx, s, o.publisher); err != nil {
		return nil, wrap("", err)
	}
	s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "store opened",
		storeIDAttr(s), stateAttr(domain.StateUnlocked),
		slog.Bool("encrypted_dek", true), slog.Bool("auto_unlock", true))
	s.startLiveness(o.livenessInterval)
	return s, nil
}

// loadCanonicalDescriptor reads both descriptor replicas, reconciles
// them into the canonical one, and heals any on-disk divergence. Location
// is the source of truth. Both absent → errs.ErrStoreNotFound;
// unrecoverable → errs.ErrStoreCorrupted; split-brain and malformed-replica
// errors pass through so callers can branch with errors.Is.
func loadCanonicalDescriptor(ctx context.Context, drv driver.Driver, hashes domain.HashRegistry, log *slog.Logger, wrap func(string, error) error) (*descriptor.Descriptor, error) {
	l0, l1, l0s, l1s, err := reconcile.ReadBoth(ctx, drv, hashes)
	if err != nil {
		// Non-recoverable I/O error from the Driver — propagate.
		return nil, wrap("read descriptor", err)
	}
	rec, err := reconcile.Reconcile(l0, l0s, l1, l1s)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, errs.ErrStoreNotFound
	case errors.Is(err, errs.ErrStoreCorrupted):
		return nil, errs.ErrStoreCorrupted
	case err != nil:
		// Split-brain, malformed-replica branches — pass through
		// unchanged so callers can identify by errors.Is.
		return nil, wrap("", err)
	}
	desc := rec.Canonical

	// Reconcile selected the canonical descriptor; if the on-disk state
	// diverges, write the side that needs updating. Each heal is a single
	// atomic Put; a crash mid-heal leaves the same divergence and the
	// next OpenStore re-applies the step. Idempotent.
	//
	// A heal means the two descriptor replicas were out of sync on disk —
	// operator-relevant (Warn): it explains why a Store may have come up
	// after an interrupted admin write. Logged before the heal so a heal
	// failure below is preceded by the diagnosis. Lock-free: no *store
	// exists yet.
	if rec.Action != reconcile.HealNone {
		log.LogAttrs(ctx, slog.LevelWarn, "descriptor replicas diverged; healing",
			slog.String("store_id", desc.StoreID),
			slog.Uint64("sequence", desc.Sequence),
			slog.String("heal_action", rec.Action.String()))
	}
	if err := healReplicas(ctx, drv, hashes, desc, rec.Action); err != nil {
		return nil, wrap("", err)
	}

	return desc, nil
}

// loadActiveConfig reads the active StoreConfig (the max system/config
// version), applies defaults, and validates it. When the caller
// supplied WithConfig, the class-aware connection check runs
// (ADR-110): class I divergence — ErrConfigMismatch; class II —
// ErrGovernanceMismatch (change via UpdateConfig, not by connecting);
// class III — returned as the session overlay for this connection. A
// caller without WithConfig accepts the on-disk config as-is — a
// legitimate scenario for diagnostic tools and projection-only
// consumers.
func loadActiveConfig(ctx context.Context, drv driver.Driver, o storeOptions, wrap func(string, error) error) (config.StoreConfig, config.StoreConfig, uint64, error) {
	active, seq, err := readConfig(ctx, drv, o.hashRegistry)
	if err != nil {
		return config.StoreConfig{}, config.StoreConfig{}, 0, wrap("read system.config", err)
	}
	active = config.ApplyDefaults(active)
	if err := config.ValidateImmutable(active); err != nil {
		return config.StoreConfig{}, config.StoreConfig{}, 0, fmt.Errorf("%w: system.config produced invalid config: %v",
			errs.ErrStoreCorrupted, err)
	}
	var overlay config.StoreConfig
	if o.cfg != nil {
		overlay, err = config.PlanConnection(*o.cfg, active)
		if err != nil {
			return config.StoreConfig{}, config.StoreConfig{}, 0, err
		}
	}
	return active, overlay, seq, nil
}
