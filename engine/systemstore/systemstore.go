package systemstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/internal/aead"
	"scrinium.dev/engine/internal/cas"
	"scrinium.dev/engine/internal/named"
	"scrinium.dev/internal/slogx"
)

// NamedArtifact is an engine-internal service artifact, addressed by a
// slash-separated Name rather than by content hash. Unlike a data-plane
// domain.Artifact it carries no Ext/Usr metadata — system payloads are
// small, opaque service blobs (config versions, agent cursors, index
// snapshots). The Name is the address: Put writes the payload as a new
// version of the name; Get reads the active version; Delete removes the
// name. Versioning, activation (max seq), exclusive-create publishing,
// and verify-on-read integrity live in engine/internal/named (ADR-85).
//
// Named addressing is a deliberately small facility for the engine's
// own data — not a general user-facing primitive — which is why it
// lives behind Adminconfig.System() and uses its own type rather than
// overloading domain.Artifact.
type NamedArtifact struct {
	// Name is the dot-separated name under which the artifact is
	// stored and later retrieved (e.g. "store.agent.orphanscan.last";
	// planar keyspace, ADR-100 — slashes belong to the path projection,
	// never to the logical name).
	Name string

	// Payload is the artifact body. System payloads are small enough to
	// buffer in memory.
	Payload io.Reader

	// ExternalRef, when non-empty, makes this a pointer artifact (ADR-105):
	// the envelope carries {store_id, external_payload_ref=ExternalRef} and no
	// inline payload (Payload is ignored). The digest names a headless
	// blob-backed data artifact (e.g. a checkpoint .db) too large for an inline
	// manifest; Get resolves it transparently to a stream, Delete cascades to
	// reap it.
	ExternalRef domain.ManifestDigest

	// Keep selects the storage form (ADR-100/101). It is optional:
	//   nil             → the default, keep=1 (atomic versioned "latest",
	//                     no history). Forgetting Keep is safe — it never
	//                     yields the exclusive-cell (lock) form.
	//   *Keep == 0      → exclusive cell: one fixed slot (<name>), no
	//                     versions, overwrite in place (the keep=0 / lock
	//                     form). Opt-in only — build it with KeepCell().
	//   *Keep ∈ [1,255] → versions: <name>.<seq> (flat, ADR-100), active = max(seq),
	//                     pruned to *Keep retained. Build with KeepVersions(n).
	Keep *uint8
}

// KeepCell marks an NamedArtifact as a keep=0 exclusive cell: a single
// fixed slot, overwritten in place (ADR-100/101). The lock form.
func KeepCell() *uint8 { var k uint8; return &k }

// KeepVersions marks an NamedArtifact as keep=n versioned storage
// (<name>.<seq>, active = max, pruned to n retained). n must be ≥ 1; n=0
// is the cell form — use KeepCell for that.
func KeepVersions(n uint8) *uint8 { return &n }

// Store is the facade for engine-internal service artifacts: versioned
// configuration, agent cursors, index snapshots, and the like, each
// addressed by a slash-separated name. Artifacts are stored outside the
// content-addressed index, in their own address space, and are invisible
// to the data-plane Walk.
type Store interface {
	// Put writes an NamedArtifact in the form its Keep selects (ADR-101):
	// keep=0 overwrites the exclusive cell in place; keep≥1 publishes a
	// new version (active = max seq) and prunes to Keep retained. Keep is
	// optional — nil defaults to keep=1 (versioned latest, no history).
	Put(ctx context.Context, a NamedArtifact) error

	// Get opens the active version (max seq) or, for a keep=0 name, the
	// cell. Returns errs.ErrArtifactNotFound when the name has never been
	// written.
	Get(ctx context.Context, name string) (domain.ReadHandle, error)

	// Delete removes every version AND any cell of name. Idempotent:
	// deleting an absent name returns nil.
	Delete(ctx context.Context, name string) error

	// Walk iterates over every name with the given prefix in
	// alphabetical order, yielding the active manifest for each — both
	// versioned actives and keep=0 cells (e.g. the lease).
	Walk(ctx context.Context, prefix string, cb func(name string, m domain.Manifest) error) error

	// PointerRef returns the external payload reference of a pointer
	// artifact (ADR-105) without opening the payload. It exists for index
	// recovery (ADR-118): restoring a checkpoint has to reach the payload
	// WITHOUT the index, and Get resolves it through the data plane, which
	// asks the index for the blob address. The caller opens the payload its
	// own way. Returns ok=false for a name whose envelope carries an inline
	// payload instead.
	PointerRef(ctx context.Context, name string) (ref domain.ManifestDigest, ok bool, err error)
}

// systemStore is the Store facade over the pointer-free layout (ADR-85,
// engine/internal/named). Every system name maps to a flat key
// named/<name>.<seq> (ADR-100), no subdirectories; the
// active version is max(seq); a write claims the next seq with an
// exclusive create. System artifacts are never indexed in StoreIndex and
// never written under manifests/ — they live in their own address space,
// so they are invisible to the data-plane Walk (handle-IS-NULL) by
// construction rather than by an index filter.
// CryptoProvider supplies the crypto material a system-artifact write/read
// needs per the store's ManifestCrypto policy (ADR-104 §2c). Declared here,
// not imported from engine/store/internal/crypto (unreachable across the
// engine root), and structurally satisfied by the store's *crypto.State. A
// Plain store is never asked for a DEK; KeyProvider returns nil and the read
// path decodes plaintext.
type CryptoProvider interface {
	// DEKForWrite returns a private DEK copy for an encrypting write (the
	// caller wipes it). Errors if the store is Locked or has no resolver.
	// Never called for Plain.
	DEKForWrite(crypto config.ManifestCrypto) ([]byte, error)
	// WriteKeyID is the KeyID a new encrypted artifact records. Empty for an
	// unencrypted config.
	WriteKeyID() string
	// KeyProvider adapts the resolver for decoding encrypted manifests on
	// read. nil for an unencrypted config.
	KeyProvider() domain.KeyProvider
}

// ExternalResolver resolves and deletes the external headless data-artifact
// payload a pointer envelope references via external_payload_ref (ADR-105).
// Declared here, structurally satisfied by the store's *store (which owns the
// data plane). A system artifact whose envelope carries an external ref is a
// thin pointer; its bytes live in a headless blob-backed artifact resolved by
// digest.
type ExternalResolver interface {
	OpenExternal(ctx context.Context, ref domain.ManifestDigest) (domain.ReadHandle, error)
	DeleteExternal(ctx context.Context, ref domain.ManifestDigest) error
}

type systemStore struct {
	drv      driver.Driver
	hashes   domain.HashRegistry
	cfg      config.StoreConfig // immutable fields only (ContentHasher); see New
	storeID  string             // authoritative store_id (descriptor), stamped on write, checked on read
	crypto   CryptoProvider     // ADR-104 §2c: policy DEK/keyID on write, KeyProvider on read
	external ExternalResolver   // ADR-105: resolve/delete external_payload_ref targets
	log      *slog.Logger
}

// defaultKeepVersions is the form an NamedArtifact takes when Keep is nil
// (unset): keep=1 — atomic, pointerless "latest" with no retained
// history. The safe default — forgetting Keep yields working versioned
// storage, never the exclusive-cell (lock) form, which requires an
// explicit KeepCell(). History (keep>1) is opt-in via KeepVersions(n);
// the config writer keeps its own history through named directly.
const defaultKeepVersions = 1

// Compile-time check that the concrete type satisfies the contract.
var _ Store = (*systemStore)(nil)

// New wires the facade. It needs the driver (the layout is on-disk), the hash
// registry (verify-on-read and the content hash of each write), the active
// config (for its immutable ContentHasher), the store's authoritative store_id
// (stamped into every artifact's envelope on write and checked on read,
// ADR-104), a CryptoProvider (policy DEK/keyID on write, KeyProvider on read —
// ADR-104 §2c; a Plain store supplies a nil-DEK provider), an ExternalResolver
// (resolve/delete external_payload_ref targets — ADR-105), and a logger for
// best-effort prune failures. No StoreIndex and no write indirection: the
// inline-manifest write is self-contained in named.
func New(
	drv driver.Driver,
	hashes domain.HashRegistry,
	cfg config.StoreConfig,
	storeID string,
	crypto CryptoProvider,
	external ExternalResolver,
	log *slog.Logger,
) Store {
	return &systemStore{
		drv:      drv,
		hashes:   hashes,
		cfg:      cfg,
		storeID:  storeID,
		crypto:   crypto,
		external: external,
		log:      log,
	}
}

// Put writes an NamedArtifact in the form its Keep selects (ADR-101). The
// payload is buffered (system payloads are small) and encoded as an
// inline manifest. Keep is optional: nil defaults to keep=1 (atomic
// versioned "latest", no history) — the safe default, so a forgotten
// Keep never silently produces the exclusive-cell (lock) form. keep=0
// (KeepCell) overwrites the exclusive cell in place (a lock's
// exclusive-acquire discipline is a caller-side policy over
// named.WriteCell, not here); keep≥1 (KeepVersions) claims the
// next seq and prunes older versions best-effort.
func (ss *systemStore) Put(ctx context.Context, a NamedArtifact) error {
	if err := named.ValidateName(a.Name); err != nil {
		return err
	}
	// Seal into the store-identity envelope (ADR-104), then build the inline
	// manifest over it. name fills the manifest's identity slot. A pointer
	// artifact (ExternalRef set, ADR-105) wraps {store_id, external_payload_ref}
	// with no inline body — its payload lives in a headless data artifact.
	var envBytes []byte
	var err error
	if a.ExternalRef != "" {
		envBytes, err = wrapExternalEnvelope(ss.storeID, string(a.ExternalRef))
	} else {
		body, berr := io.ReadAll(a.Payload)
		if berr != nil {
			return fmt.Errorf("system store: read payload for %q: %w", a.Name, berr)
		}
		envBytes, err = wrapEnvelope(ss.storeID, body)
	}
	if err != nil {
		return fmt.Errorf("system store: %q: %w", a.Name, err)
	}
	// Crypto by policy (ADR-104 §2c): the inline payload follows the store's
	// ManifestCrypto. Plain → as-is; Sealed/Paranoid → sealed under a DEK
	// borrowed from the crypto provider (wiped once the manifest is built).
	// The bootstrap chain (descriptor/config/lease) never reaches here.
	mc := ss.cfg.ManifestCrypto
	var dek []byte
	var keyID string
	if mc != "" && mc != config.ManifestCryptoPlain {
		dek, err = ss.crypto.DEKForWrite(mc)
		if err != nil {
			return fmt.Errorf("system store: %q: %w", a.Name, err)
		}
		defer aead.Wipe(dek)
		keyID = ss.crypto.WriteKeyID()
	}
	fileBytes, _, err := named.BuildInlineManifest(a.Name, envBytes, string(ss.cfg.ContentHasher), ss.hashes, mc, dek, keyID)
	if err != nil {
		return fmt.Errorf("system store: build %q: %w", a.Name, err)
	}

	// Keep selects the form. nil → the default (keep=1, versioned); the
	// exclusive cell is opt-in only, via KeepCell() (*Keep == 0).
	keep := defaultKeepVersions
	if a.Keep != nil {
		keep = int(*a.Keep)
	}

	if keep == 0 {
		// keep=0 — exclusive cell. Put overwrites in place (last-write
		// wins); a lock's create-if-absent acquire uses
		// named.WriteCell(exclusive=true) directly.
		if err := named.WriteCell(ctx, ss.drv, a.Name, fileBytes, false); err != nil {
			return fmt.Errorf("system store: put cell %q: %w", a.Name, err)
		}
		return nil
	}

	// keep≥1 — versioned: publish next seq, then prune to keep.
	if _, _, err := named.ClaimVersion(ctx, ss.drv, a.Name, fileBytes); err != nil {
		return fmt.Errorf("system store: put %q: %w", a.Name, err)
	}
	// Retention is GC, not a liveness step: a prune failure leaves an
	// invisible old version for the next prune to reclaim and never
	// invalidates the version just written.
	if err := named.Prune(ctx, ss.drv, a.Name, keep); err != nil {
		ss.logger().LogAttrs(ctx, slog.LevelWarn,
			"system artifact prune failed (old versions left for next prune)",
			slog.String("name", a.Name), slog.String("error", err.Error()))
	}
	return nil
}

// Get opens the active version (max seq) or, when the name has no
// versions, the keep=0 cell. A name is exactly one form, so at most one
// resolves. Returns errs.ErrArtifactNotFound when the name has never
// been written.
func (ss *systemStore) Get(ctx context.Context, name string) (domain.ReadHandle, error) {
	env, m, err := ss.loadEnvelope(ctx, name)
	if err != nil {
		return nil, err
	}
	if env.ExternalPayloadRef != "" {
		// Pointer envelope (ADR-105): the payload is an external headless data
		// artifact (e.g. a checkpoint .db) too large for an inline manifest.
		// Resolve it to a streaming handle; identity was already enforced by
		// openEnvelope inside loadEnvelope.
		return ss.external.OpenExternal(ctx, domain.ManifestDigest(env.ExternalPayloadRef))
	}
	return cas.NewInlinePayloadHandle(m, env.InlinePayload), nil
}

// PointerRef reads the envelope and reports its external payload reference,
// leaving the payload untouched. See the interface for why this exists apart
// from Get.
func (ss *systemStore) PointerRef(ctx context.Context, name string) (domain.ManifestDigest, bool, error) {
	env, _, err := ss.loadEnvelope(ctx, name)
	if err != nil {
		return "", false, err
	}
	if env.ExternalPayloadRef == "" {
		return "", false, nil
	}
	return domain.ManifestDigest(env.ExternalPayloadRef), true, nil
}

// loadEnvelope resolves a name to its active manifest (a versioned active or a
// keep=0 cell) and opens the envelope — verifying store_id against this store's
// descriptor (foreign rejected, ADR-104). Shared by Get (which then serves the
// payload) and Delete (which reads external_payload_ref to cascade the GC). An
// absent name surfaces as errs.ErrArtifactNotFound from the load.
func (ss *systemStore) loadEnvelope(ctx context.Context, name string) (envelope, domain.Manifest, error) {
	seq, found, err := named.ResolveActiveSeq(ctx, ss.drv, name)
	if err != nil {
		return envelope{}, domain.Manifest{}, fmt.Errorf("system store: get %q: %w", name, err)
	}
	var m domain.Manifest
	if found {
		path, perr := named.VersionPath(name, seq)
		if perr != nil {
			return envelope{}, domain.Manifest{}, perr
		}
		m, err = named.LoadWithKeys(ctx, ss.drv, ss.hashes, path, ss.crypto.KeyProvider())
	} else {
		// No versions — try the cell. LoadCell maps an absent cell to
		// errs.ErrArtifactNotFound, so this is also the not-found path.
		m, err = named.LoadCellWithKeys(ctx, ss.drv, ss.hashes, name, ss.crypto.KeyProvider())
	}
	if err != nil {
		return envelope{}, domain.Manifest{}, err
	}
	env, err := openEnvelope(m.InlineBlob, ss.storeID)
	if err != nil {
		return envelope{}, domain.Manifest{}, fmt.Errorf("system store: get %q: %w", name, err)
	}
	return env, m, nil
}

// Delete removes every version AND any cell of name. A name is one form,
// but removing both is form-agnostic and idempotent (each is a no-op
// when absent).
func (ss *systemStore) Delete(ctx context.Context, name string) error {
	// Cascade (ADR-105): a pointer artifact's external headless payload must be
	// deleted too, or its blob leaks (ref_count never reaches zero, GC never
	// reaps it). Best-effort read — an absent/foreign/inline/malformed artifact
	// simply skips the cascade; the removal below proceeds regardless.
	if env, _, err := ss.loadEnvelope(ctx, name); err == nil && env.ExternalPayloadRef != "" {
		if derr := ss.external.DeleteExternal(ctx, domain.ManifestDigest(env.ExternalPayloadRef)); derr != nil {
			return fmt.Errorf("system store: delete %q: external payload: %w", name, derr)
		}
	}
	if err := named.RemoveAll(ctx, ss.drv, name); err != nil {
		return fmt.Errorf("system store: delete %q: %w", name, err)
	}
	if err := named.RemoveCell(ctx, ss.drv, name); err != nil {
		return fmt.Errorf("system store: delete %q: %w", name, err)
	}
	return nil
}

// Walk iterates over every name with the given prefix in alphabetical
// order, yielding the active manifest for each — both versioned actives
// (ListActive) and keep=0 cells (ListCells, e.g. the lease). A name is
// one form, so the merged lists never overlap.
func (ss *systemStore) Walk(ctx context.Context, prefix string, cb func(name string, m domain.Manifest) error) error {
	versions, err := named.ListActive(ctx, ss.drv, prefix)
	if err != nil {
		return err
	}
	cells, err := named.ListCells(ctx, ss.drv, prefix)
	if err != nil {
		return err
	}
	all := append(versions, cells...)
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	for _, a := range all {
		m, err := named.LoadWithKeys(ctx, ss.drv, ss.hashes, a.Path, ss.crypto.KeyProvider())
		if err != nil {
			// A version/cell that vanished or failed verification
			// mid-walk is skipped rather than aborting the rest.
			continue
		}
		if err := cb(a.Name, m); err != nil {
			return err
		}
	}
	return nil
}

// logger returns the systemStore's logger, never nil. Mirrors the
// store-level nil-safety so call sites need no guard.
func (ss *systemStore) logger() *slog.Logger {
	return slogx.OrDiscard(ss.log)
}
