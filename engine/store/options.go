package store

import (
	"time"

	"log/slog"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/pipeline"
	"scrinium.dev/event"
)

// StoreOption is an option for the Store constructor. It applies to
// InitStore and OpenStore. The order in which options are passed is
// irrelevant.
type StoreOption func(*storeOptions)

// storeOptions is the resolved aggregate of all StoreOptions.
type storeOptions struct {
	forceReinit      bool
	purgeOnReinit    bool
	cfg              *config.StoreConfig
	storeIndex       index.StoreIndex
	publisher        event.Publisher
	hashRegistry     domain.HashRegistry
	livenessInterval time.Duration
	readRegistry     pipeline.TransformerRegistry
	keyResolver      pipeline.KeyResolver
	passphrase       domain.PassphraseProvider
	autoUnlock       bool
	identityMode     config.IdentityMode
	logger           *slog.Logger
}

// WithForceReinit allows InitStore to run on top of an existing Store,
// destroying it. The operation is IRREVERSIBLE and assumes the Location is not
// in use — it neither tombstones nor coordinates with other hosts.
//
// What it removes: both descriptor replicas, every system artifact (config
// versions, agent cursors, checkpoint pointers), every manifest file, and the
// StoreIndex's content.
//
// What it keeps: blob payload, together with its index rows at ref_count 0. The
// new Store therefore starts on top of the old bytes — GC reclaims them at its
// own pace, and until then a Plain Store's dedup may reuse them. Add
// WithPurgeOnReinit to remove the payload as well.
//
// It requires a StoreIndex implementing index.Resetter and refuses without one:
// a forced re-init that cannot empty the index would produce a Store that looks
// fresh while carrying the previous one's rows.
//
// On a Location with no Store it destroys nothing and is not an error.
func WithForceReinit() StoreOption {
	return func(o *storeOptions) { o.forceReinit = true }
}

// WithPurgeOnReinit, alongside WithForceReinit, extends the destruction to blob
// payload: the blob index rows go with everything else, the files under every
// blob root are removed, and tombstone markers left behind by a GC cycle that
// had marked but not yet swept are removed too. Afterwards the Location holds a
// fresh Store and nothing else.
//
// Without WithForceReinit it is an error rather than a no-op: on its own it
// describes how to destroy a Store nobody authorised destroying, and ignoring
// it silently is how a caller comes to believe a purge happened.
func WithPurgeOnReinit() StoreOption {
	return func(o *storeOptions) { o.purgeOnReinit = true }
}

// WithConfig provides the Store configuration. At InitStore it
// fixes the immutable parameters. At OpenStore it is checked
// against the configuration loaded from the active system/config version —
// a divergence in immutable fields produces errs.ErrConfigMismatch.
func WithConfig(cfg config.StoreConfig) StoreOption {
	return func(o *storeOptions) { o.cfg = &cfg }
}

// WithStoreIndex provides the StoreIndex implementation. Required.
func WithStoreIndex(idx index.StoreIndex) StoreOption {
	return func(o *storeOptions) { o.storeIndex = idx }
}

// WithPublisher provides a Publisher implementation for emitting
// events.
func WithPublisher(p event.Publisher) StoreOption {
	return func(o *storeOptions) { o.publisher = p }
}

// WithLogger provides the *slog.Logger the Store and its components log
// against. Optional: without it the Store is silent (slog.DiscardHandler).
// A nil logger is treated as "silent" — WithLogger(nil) never panics and
// is equivalent to omitting the option.
//
// The Store namespaces the supplied logger once at construction
// (WithGroup("scrinium")) and derives per-component subloggers from it
// (componentLogger). Hosts therefore pass a plain root logger; the engine
// adds its own structure.
func WithLogger(l *slog.Logger) StoreOption {
	return func(o *storeOptions) { o.logger = l }
}

// WithLivenessInterval overrides the liveness-sentinel probe period
// (ADR-111). Zero = default (5 s); negative = sentinel disabled (hosts
// running their own probe, or tests constructing partial stores).
func WithLivenessInterval(d time.Duration) StoreOption {
	return func(o *storeOptions) { o.livenessInterval = d }
}

// WithHashRegistry provides the registry of hash algorithms.
// Required. Used by the Pipeline runner, Recovery Agent, and
// parsers.
func WithHashRegistry(r domain.HashRegistry) StoreOption {
	return func(o *storeOptions) { o.hashRegistry = r }
}

// WithReadRegistry provides the registry of transformation plugins.
// Required when StoreConfig.Pipeline is non-empty.
func WithReadRegistry(r pipeline.TransformerRegistry) StoreOption {
	return func(o *storeOptions) { o.readRegistry = r }
}

// WithKeyResolver provides the key-resolver plugin. By default the
// engine uses StaticKeyResolver populated with the DEK from the
// descriptor.
func WithKeyResolver(r pipeline.KeyResolver) StoreOption {
	return func(o *storeOptions) { o.keyResolver = r }
}

// WithPassphrase provides the KEK provider. Required when
// ManifestCrypto != Plain. With Plain it is ignored.
func WithPassphrase(provider domain.PassphraseProvider) StoreOption {
	return func(o *storeOptions) { o.passphrase = provider }
}

// WithAutoUnlock instructs OpenStore to call Unlock automatically on
// an encrypted Store. Without this flag, OpenStore returns the Store
// in StateLocked.
func WithAutoUnlock() StoreOption {
	return func(o *storeOptions) { o.autoUnlock = true }
}

// WithIdentityMode sets the immutable IdentityMode (ADR-73) at InitStore.
// IdentityModeUnique (default) mixes a fresh nonce into every handle so
// each Put is distinct; IdentityModeCoalesced omits the nonce so identical
// content+identity collapses to a single artifact. The mode is fixed at
// init and validated — not changed — at OpenStore.
func WithIdentityMode(mode config.IdentityMode) StoreOption {
	return func(o *storeOptions) { o.identityMode = mode }
}

// WithCoalesced is shorthand for WithIdentityMode(IdentityModeCoalesced):
// identical content+identity coalesces to one artifact (WORM-archive
// semantics).
func WithCoalesced() StoreOption {
	return WithIdentityMode(config.IdentityModeCoalesced)
}

// WithUnique is shorthand for WithIdentityMode(IdentityModeUnique), the
// default: every Put yields a distinct handle.
func WithUnique() StoreOption {
	return WithIdentityMode(config.IdentityModeUnique)
}
