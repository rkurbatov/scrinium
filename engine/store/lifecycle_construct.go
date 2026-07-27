package store

// Machinery shared by InitStore and OpenStore: building a *store,
// healing descriptor replicas, and the bootstrap-into-Unlocked
// transition. Kept here so neither constructor reaches into the other.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/store/internal/crypto"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/engine/store/internal/orphanscan"
	"scrinium.dev/engine/store/internal/reconcile"
	"scrinium.dev/engine/systemstore"
	"scrinium.dev/event"
)

// buildStore constructs the *store value and wires the systemStore
// facade. The caller has already defaulted and validated cfg and
// supplied a non-nil drv and idx; this function does not re-check them.
func buildStore(
	ctx context.Context,
	o storeOptions,
	drv driver.Driver,
	idx index.StoreIndex,
	cfg config.StoreConfig,
	desc *descriptor.Descriptor,
	dek []byte,
) (*store, error) {
	c := &store{
		storeID:      desc.StoreID,
		drv:          drv,
		index:        idx,
		pub:          o.publisher,
		log:          resolveLogger(o.logger),
		activeConfig: cfg,
		state:        domain.StateBootstrapping,
		hashes:       o.hashRegistry,
		transformers: o.readRegistry,
		crypto:       crypto.New(desc, dek, o.passphrase, o.keyResolver, drv),
	}
	// Derive the hot "store"-component logger once; componentLogger("store")
	// returns this rather than allocating a With wrapper on every call.
	c.logStore = c.logger().With(slog.String("component", "store"))
	// systemstore.Store facade over the pointer-free layout (ADR-85). Besides
	// the driver, the hash registry, the active config (for its immutable
	// ContentHasher), and a logger, it takes the authoritative store_id
	// (stamped into every envelope on write, checked on read — ADR-104), the
	// CryptoProvider (policy DEK/keyID on write, KeyProvider on read — ADR-104
	// §2c), and the ExternalResolver (resolve/delete external_payload_ref
	// targets — ADR-105; the store itself satisfies it). No StoreIndex and no
	// write indirection: the inline-manifest write is self-contained in named.
	c.system = systemstore.New(drv, o.hashRegistry, cfg, desc.StoreID, c.crypto, c, c.log)

	// Reject an illegal pipeline composition at construction time
	// (InitStore / OpenStore): a crypto (AEAD) stage must be terminal,
	// so a compressor after a crypto plugin is errs.ErrInvalidPipeline
	// (2. Internals/03 Cryptography). Composition only — an unregistered
	// algorithm is not an open-time failure; it surfaces at Put as
	// errs.ErrUnsupportedAlgorithm. No-op for an empty pipeline; skipped
	// when no transformer registry is configured.
	if len(cfg.Pipeline) > 0 && c.transformers != nil {
		if err := c.pipelineRunner().ValidateComposition(cfg.Pipeline); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// unlockBootstrap completes the bootstrap-into-Unlocked transition
// shared by InitStore, both OpenStore paths, and the deferred
// Store.Unlock path. The caller has produced a *store in
// StateBootstrapping with the DEK populated; unlockBootstrap runs the
// Orphan Scan, publishes the report, and flips state to StateUnlocked.
//
// An Orphan Scan error propagates with the *store left in
// StateBootstrapping; the caller decides whether to retry, fall back to
// Locked, or surface the failure.
// orphanScanCursorName is the system-artifact name of the orphan-scan
// timestamp cursor (ADR-104 §6: advisory state, keep=0 cell, read directly).
const orphanScanCursorName = "store.agent.orphanscan.last"

func unlockBootstrap(ctx context.Context, c *store, pub event.Publisher) error {
	report, err := orphanscan.RecoverOrphans(ctx, c.drv, c.index)
	if err != nil {
		return fmt.Errorf("orphan scan: %w", err)
	}
	// Record the scan timestamp as a cursor system artifact (ADR-104 §6) so it
	// survives an index rebuild. Cold path (one write per open), so it is read
	// straight from the artifact when needed — no in-memory cache (S-19).
	// keep=0 cell: a single current value, overwritten in place. Best-effort:
	// a failure is appended to the report for observability but does not block
	// the transition — the timestamp is a diagnostic aid, not a liveness gate.
	if putErr := c.system.Put(ctx, systemstore.NamedArtifact{
		Name:    orphanScanCursorName,
		Payload: strings.NewReader(time.Now().UTC().Format(time.RFC3339)),
		Keep:    systemstore.KeepCell(),
	}); putErr != nil {
		report.Errors = append(report.Errors,
			fmt.Errorf("unlockBootstrap: persist orphan-scan cursor: %w", putErr))
	}
	orphanscan.PublishOrphanReport(pub, report)

	c.stateMu.Lock()
	c.state = domain.StateUnlocked
	c.stateMu.Unlock()
	return nil
}

// healReplicas applies Reconcile's repair action: writes the
// damaged or missing replica from the canonical descriptor.
// HealNone is a no-op; the four healing actions reduce to two
// distinct disk operations (write L0 only, write L1 only) since
// the canonical content already lives on the surviving side.
func healReplicas(ctx context.Context, drv driver.Driver, hashes domain.HashRegistry, canonical *descriptor.Descriptor, action reconcile.Action) error {
	switch action {
	case reconcile.HealNone:
		return nil
	case reconcile.HealL0FromL1, reconcile.HealBothFromL1:
		// HealL0FromL1: L0 was missing/corrupted, rewrite it.
		// HealBothFromL1: sequence-divergence, L1 won, rewrite L0.
		// Same disk operation; distinct names preserve diagnostic
		// detail in logs.
		return descriptor.WriteReplica(ctx, drv, hashes, canonical, descriptor.L0)
	case reconcile.HealL1FromL0, reconcile.HealBothFromL0:
		return descriptor.WriteReplica(ctx, drv, hashes, canonical, descriptor.L1)
	default:
		return fmt.Errorf("core: unknown ReconcileAction %d", int(action))
	}
}
