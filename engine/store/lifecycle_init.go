package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/internal/aead"
	"scrinium.dev/engine/store/internal/crypto"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/engine/store/internal/keyring"
	"scrinium.dev/errs"
)

// InitStore creates a new Store at the Location served by drv.
//
// Behaviour:
//  1. Probe the Driver for an existing descriptor. If one is
//     present and WithForceReinit is NOT set, return
//     errs.ErrStoreAlreadyExists.
//  2. With WithForceReinit, wipe the structural state: both
//     descriptor replicas, every system artifact, every manifest
//     file, and the StoreIndex's content. Blob payload survives,
//     with its index rows kept at ref_count 0 — so the new Store
//     starts on top of the old bytes and GC reclaims them at its
//     own pace. WithPurgeOnReinit removes the payload too, along
//     with any tombstone markers, leaving the Location bare. Both
//     levels live in reinit.go; a forced re-init requires a
//     StoreIndex implementing index.Resetter and refuses without
//     one rather than wiping half of it.
//  3. Generate a fresh StoreID. Apply config defaults. Validate
//     immutable parameters.
//  4. Validate that a StoreIndex was provided via WithStoreIndex.
//     core does NOT open the index itself — the caller wires the
//     concrete implementation (sqlite.NewStore, in-memory, etc.)
//     and passes it as a dependency. This keeps core free of any
//     import dependency on index/* packages (DAG: core ← index).
//  5. Generate a 32-byte DEK from crypto/rand. The DEK is
//     generated unconditionally; encryption can be
//     turned on later through SetPassphrase without re-keying.
//  6. If WithPassphrase is configured, derive a KEK through
//     Argon2id and wrap the DEK with AES-256-GCM. The wrapped
//     DEK plus its KDFParams land in the descriptor.
//     Otherwise the DEK is stored in the descriptor in
//     plaintext — semantically honest: no passphrase, no
//     protection.
//  7. Write both descriptor replicas.
//  8. Construct the *store object in StateUnlocked and return.
//     For encrypted Stores, also return the Recovery Kit
//     bytes — the host MUST persist them before reporting
//     success to the user.
//
// The phases above map to helpers: prepareInitLocation (1–2),
// the DEK lifecycle inline (5–6, kept visible for auditability),
// and persistInitState (7). The shared construct+unlock tail
// (buildStore, unlockBootstrap) lives in lifecycle_construct.go.
//
// Recovery Kit:
//   - nil for Plain-DEK Stores (no encryption to recover).
//   - non-nil text bytes for encrypted Stores.
//
// Refusal cases:
//   - ManifestCrypto != Plain without WithPassphrase →
//     errs.ErrPassphraseRequired. An unprotected DEK plus
//     encrypted manifests is the worst-of-both-worlds shape:
//     anyone who reads the descriptor gets the keys to all the
//     manifests for free.
func InitStore(ctx context.Context, drv driver.Driver, opts ...StoreOption) (Store, []byte, error) {
	if drv == nil {
		return nil, nil, errors.New("store.InitStore: nil driver")
	}

	// wrap is the local error-prefix closure for this function, also
	// threaded into the phase helpers so every site reads
	// "store.InitStore: <stage>: %w" with a single definition.
	wrap := func(stage string, err error) error {
		if stage == "" {
			return fmt.Errorf("store.InitStore: %w", err)
		}
		return fmt.Errorf("store.InitStore: %s: %w", stage, err)
	}

	// --- Resolve options ---

	o := storeOptions{}
	for _, fn := range opts {
		fn(&o)
	}

	// Apply defaults to the requested config (the user may have
	// passed nothing, or only some fields).
	cfg := config.StoreConfig{}
	if o.cfg != nil {
		cfg = *o.cfg
	}
	cfg = config.ApplyDefaults(cfg)
	// WithIdentityMode (and the WithCoalesced/WithUnique shorthands) sets
	// the immutable IdentityMode at init, overriding the default after
	// ApplyDefaults so the explicit choice wins. Ignored at OpenStore,
	// where the persisted value is authoritative.
	if o.identityMode != "" {
		cfg.IdentityMode = o.identityMode
	}
	if err := config.ValidateImmutable(cfg); err != nil {
		return nil, nil, wrap("invalid config", err)
	}

	// --- Validate the StoreIndex dependency ---
	//
	// core does not import any concrete index implementation. The
	// caller is expected to construct one (sqlite.NewStore,
	// in-memory, or any other) and pass it via WithStoreIndex.
	// This keeps the dependency graph one-way: core ← index.
	//
	// Checked BEFORE the location probe: a forced re-init empties the
	// index as part of the wipe, so the dependency has to be in hand
	// before anything on disk is touched. It also means a missing
	// index is reported without having mutated the Location.

	idx := o.storeIndex
	if idx == nil {
		return nil, nil, fmt.Errorf(
			"store.InitStore: WithStoreIndex is required (see DI Example)")
	}

	// --- Refuse a purge that was not asked to destroy anything ---
	//
	// WithPurgeOnReinit only means something alongside WithForceReinit:
	// on its own it describes how to destroy a store nobody authorised
	// destroying. Silently ignoring it is how a caller ends up believing
	// a purge happened.
	if o.purgeOnReinit && !o.forceReinit {
		return nil, nil, fmt.Errorf(
			"store.InitStore: WithPurgeOnReinit requires WithForceReinit " +
				"(a purge is how a forced re-init destroys payload, not a mode of its own)")
	}

	// --- Probe for existing descriptor; honour WithForceReinit ---

	if err := prepareInitLocation(ctx, drv, idx, o, optsLogger(o, "store"), wrap); err != nil {
		return nil, nil, err
	}

	// --- Refuse encrypted-manifest configs without a passphrase ---
	//
	// Sealed and Paranoid only make sense alongside a
	// wrapped DEK: encrypting manifests against a plaintext key
	// stored in the descriptor provides no protection, just
	// operational pain. Caught here at InitStore so the user
	// sees the conflict before any disk I/O.
	if cfg.ManifestCrypto != config.ManifestCryptoPlain && o.passphrase == nil {
		return nil, nil, fmt.Errorf("store.InitStore: %w: ManifestCrypto=%q requires WithPassphrase",
			errs.ErrPassphraseRequired, cfg.ManifestCrypto)
	}

	// --- Refuse a crypto mode the index cannot honour ---
	//
	// Paranoid promises that nothing about the store is readable on
	// disk; an index persisting in the clear holds the same fields and
	// defeats the mode (ADR-56). Checked here, before any payload I/O.
	if err := guardIndexAtRest(cfg.ManifestCrypto, idx); err != nil {
		return nil, nil, fmt.Errorf("store.InitStore: %w", err)
	}

	// --- Generate identity and DEK ---
	//
	// DEK is generated for every Store regardless of crypto
	// configuration. When WithPassphrase is set the DEK is
	// wrapped with the resulting KEK; otherwise it lives in the
	// descriptor in plaintext. In both branches dek is the in-memory
	// unwrapped value, kept alive on *store after construction. Its
	// lifetime is owned here: every error path below wipes it.

	storeID := uuid.NewString()

	dek, err := keyring.GenerateDEK()
	if err != nil {
		return nil, nil, wrap("", err)
	}

	desc := &descriptor.Descriptor{
		StoreID:       storeID,
		SchemaVersion: descriptor.CurrentSchemaVersion,
		Sequence:      1,
	}

	var kit []byte
	if o.passphrase != nil {
		wrapped, kdfParams, kitBytes, ierr := crypto.InitEncryptedDEK(
			ctx, storeID, dek, o.passphrase, cfg.KDFParams)
		if ierr != nil {
			aead.Wipe(dek)
			return nil, nil, wrap("", ierr)
		}
		desc.DEK = wrapped
		desc.DEKEncrypted = true
		desc.KDFParams = &kdfParams
		kit = kitBytes
	} else {
		// Plaintext DEK path. The descriptor carries the raw key.
		// Validate enforces "DEKEncrypted=false ⇒ KDFParams
		// absent"; we don't fight that rule.
		desc.DEK = dek
		desc.DEKEncrypted = false
	}

	// --- Persist descriptor and system.config ---

	cfgSeq, err := persistInitState(ctx, drv, o.hashRegistry, cfg, desc, wrap)
	if err != nil {
		aead.Wipe(dek)
		return nil, nil, err
	}

	// --- Construct *store ---

	s, err := buildStore(ctx, o, drv, idx, cfg, desc, dek)
	if err != nil {
		aead.Wipe(dek)
		return nil, nil, wrap("", err)
	}
	s.crypto.PromoteResolverIfDefault()
	// A just-initialised Location holds nothing, so the index knows the whole
	// (empty) manifest set: completeness is true by vacuous truth (ADR-118).
	s.markIndexComplete()
	if err := unlockBootstrap(ctx, s, o.publisher); err != nil {
		aead.Wipe(dek)
		return nil, nil, wrap("", err)
	}
	s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "store initialised",
		storeIDAttr(s),
		slog.Bool("encrypted_dek", desc.DEKEncrypted),
		manifestCryptoAttr(cfg.ManifestCrypto),
		slog.Bool("recovery_kit", kit != nil))
	s.lastConfigSeq = cfgSeq
	s.startLiveness(o.livenessInterval)
	return s, kit, nil
}

// persistInitState writes the descriptor (both replicas) and persists the
// active StoreConfig as system.config. A hash registry is required for the
// config write (system.config must be readable before the Store opens for
// users). The descriptor is written first so a config-write failure still
// leaves a readable Store identity behind.
func persistInitState(ctx context.Context, drv driver.Driver, hashes domain.HashRegistry, cfg config.StoreConfig, desc *descriptor.Descriptor, wrap func(string, error) error) (uint64, error) {
	if err := descriptor.WriteBoth(ctx, drv, hashes, desc); err != nil {
		return 0, wrap("write descriptor", err)
	}
	if hashes == nil {
		return 0, fmt.Errorf(
			"store.InitStore: WithHashRegistry is required to persist system.config")
	}
	seq, err := writeConfig(ctx, drv, hashes, cfg)
	if err != nil {
		return 0, wrap("write system.config", err)
	}
	return seq, nil
}
