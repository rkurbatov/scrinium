package customindex

import (
	"context"
	"errors"

	"scrinium.dev/domain"
)

type CustomIndex interface {
	// Name is the stable identifier for this custom index. Used
	// as the namespace prefix in Substrate. Must be
	// unique among custom indexes registered to the same backend.
	// Recommended: dotted, lower-case, like "scrinium.fspathindex".
	Name() string

	// SchemaVersion is the current data layout version. The
	// backend persists the version of the most-recent successful
	// Setup; on a later registration with a different version,
	// Setup is called with the stored value as oldVersion and
	// the custom index migrates.
	SchemaVersion() int

	// Setup runs once per registration, inside the registration
	// transaction. oldVersion is 0 on the first registration; a
	// positive value is the persisted SchemaVersion from a
	// previous run. Implementations migrate from oldVersion to
	// SchemaVersion().
	Setup(ctx context.Context, store Substrate, oldVersion int) error

	// Close releases custom index-side resources. Backend storage
	// remains owned by the StoreIndex.
	Close() error
}

// Observer is an OPTIONAL capability (detected by assertion, ADR-88) for a
// CustomIndex that must react to index-level events outside the projection
// model. Most custom indexes do not implement it — the Indexer capability
// (Index/Unindex) covers the common projection case. The two existing
// EventKinds correspond exactly to IndexManifest/DeleteManifest; Observer is
// only needed when the reaction is not expressible as projected keys.
type Observer interface {
	// Subscribe returns the event kinds this index observes. Called once
	// at Register; the result is cached.
	Subscribe() []EventKind

	// Apply is invoked for each subscribed event, inside the backend's
	// transaction. Mutations through store land in the same transaction
	// as the main index write; an error returned here aborts both.
	Apply(ctx context.Context, store Substrate, kind EventKind, args EventArgs) error
}

// EventKind names an index-level operation a custom index can
// observe. The set is closed (defined here, exhaustive switch
// in implementations).
type EventKind uint8

const (
	// EventKindManifestIndexed corresponds to StoreIndex.IndexManifest.
	// EventArgs.Manifest carries the full manifest just written;
	// EventArgs.ArtifactID is a duplicate of Manifest.ArtifactID.
	EventKindManifestIndexed EventKind = iota

	// EventKindManifestDeleted corresponds to StoreIndex.DeleteManifest.
	// EventArgs.ArtifactID is the id of the deleted artifact;
	// EventArgs.BlobRefs lists the blobs the deletion decremented;
	// EventArgs.Manifest is zero.
	EventKindManifestDeleted
)

// String returns a human-readable name for the kind. Used in
// error messages and diagnostic events.
func (k EventKind) String() string {
	switch k {
	case EventKindManifestIndexed:
		return "ManifestIndexed"
	case EventKindManifestDeleted:
		return "ManifestDeleted"
	default:
		return "EventKind(?)"
	}
}

// EventArgs carries operation-specific arguments for Apply. Not
// every field is populated for every kind — see the EventKind
// constants for per-kind documentation.
type EventArgs struct {
	Manifest   domain.Manifest
	ArtifactID domain.ArtifactID
	BlobRefs   []string
}

// --- Capability roster (ADR-88) ---
//
// A CustomIndex declares WHAT it can do by implementing optional
// capability sub-interfaces below; the backend detects them by
// assertion (if r, ok := ext.(Resolver); ok), never by a Class field.
// Resolver is the pack/chunk overlay capability (ADR-86/87). Indexer
// (write-side projection) and the Accessor family (KeyLookup/PrefixScan/
// RangeScan, read-side) are the metadata-search capabilities (ADR-78).
// The accounting/compaction roster (GCParticipant, Compactor) lands with
// the GC contract. A CustomIndex implements whichever it needs; the
// backend detects each by assertion, never by a Class field.

// PlacementOverlay is the physical location of an artifact whose
// storage is OWNED by an index custom index rather than the core
// (ADR-88/86) — the overlay counterpart of the core's plain
// resolution. A packed artifact lives as two slices of a .pack
// volume (its member manifest and its blob) plus the pipeline
// parameters needed to decode the blob.
//
// The type lives in the contract, not in a concrete owner package,
// so the core resolve path can consult any Resolver without
// importing the owner. Owners map their internal representation onto
// this shape.
type PlacementOverlay struct {
	// PackBlobRef is the blob_ref of the .pack volume the slices live
	// in. The driver range-reads this volume.
	PackBlobRef string

	// ManifestOffset/ManifestSize locate the member-manifest slice
	// within the volume.
	ManifestOffset int64
	ManifestSize   int64

	// BlobOffset/BlobSize locate the member-blob slice within the
	// volume.
	BlobOffset int64
	BlobSize   int64

	// PipelineParams carries the decode parameters for the member
	// blob — opaque to the core, handed back to the pipeline.
	PipelineParams []byte
}

// Resolver is the optional capability a CustomIndex implements
// to OVERLAY physical placement for the artifacts it owns
// (ADR-88/86). The core resolves plain blobs itself; for anything it
// does not find, it probes registered Resolvers, each covering its
// own structure. The core never branches on artifact type —
// ownership of the index record decides who answers, so a new
// structural kind needs a new owner, not a core edit.
//
// A Resolver is MANDATORY for reading the structure it owns: with no
// owner registered, the artifact is correctly unresolvable
// (structurally, not by hiding). Capability is detected by interface
// assertion, not by Class.
type Resolver interface {
	CustomIndex

	// ResolvePacked reports the placement of an artifact this
	// custom index owns, by its ArtifactID. The second return is false
	// when the artifact is not owned here — the caller continues to
	// the next resolver or falls back to plain. Reads see committed
	// state.
	ResolvePacked(ctx context.Context, artifactID domain.ArtifactID) (PlacementOverlay, bool, error)
}

// Key is a position in a CustomIndex's OWN ordered key space — a path for a
// tree index — written via Substrate inside Index and scanned by the Accessor
// family (KeyLookup/PrefixScan/RangeScan over own tables, §9.7). It is distinct
// from a Projection: equality projection into the standard table (proj_ext) is
// expressed by RETURNING Projections from Index, not Keys.
type Key string

// Projection is one equality row an Indexer emits for the core to write into
// the standard table (proj_ext, §9.6). The index supplies Field/Value; the core
// stamps the manifest digest and ext_name (= Name()), so an index cannot
// project under another index's name (Principle 9). proj_ext is the
// core-maintained equality store; an index keeps NO own tables for it. Several
// Projections per Index call are allowed — the PK (digest, ext_name, field)
// holds multiple fields per index.
//
// There is no projection of the usr pocket: usr is a detachable part of the
// content, which the system does not interpret and does not search (Principle
// 3, ADR-77 amendment).
type Projection struct {
	// Field is the projected field name within the ext pocket ("nsid",
	// "path", …) — independent of the index Name (ext_name): Walk(ns) reads
	// ext_name="namespace", field="nsid".
	Field string

	// Value is the projected scalar: a JSON scalar as text.
	Value string
}

// Indexer is the optional write-side capability (ADR-78) — the SINGLE mechanism
// that populates the index (Principle 11): a pure function of the
// manifest, run by the core inside the index-write transaction on Put. Index
// (a) RETURNS Projections the core writes into proj_ext — equality lookup over
// the index's own area, core-maintained (§9.6); and/or (b) writes its OWN tables via
// store — prefix-trees, counters (§9.7), scanned later by the Accessor family.
// namespace projects (ext "nsid"); fspathindex writes an own path tree. Unindex is
// symmetric on Delete/Rollback, recomputing from the manifest — the index never
// touches foreign rows by ArtifactID (Principle 9); the core removes the
// manifest's proj_ext rows by digest. Both sides are idempotent over the manifest
// (Put/Delete, not conditional) so the backend may replay after crash recovery
// (§9.10). Index sees the passport {digest, ext}, not the content
// (Principle 3); determinism lets rebuild reproduce an identical index
// (Corollary 15). Writes through store happen in the same transaction.
type Indexer interface {
	Index(ctx context.Context, store Substrate, m domain.Manifest) ([]Projection, error)
	Unindex(ctx context.Context, store Substrate, m domain.Manifest) error
}

// KeyLookup, PrefixScan, and RangeScan are the read-side Accessor family
// (ADR-78/88), implemented selectively: a CustomIndex exposes only the
// access shapes its key space supports — equality, subtree, range — with
// no forced stubs (an equality index does not carry a dummy ScanRange).
// They are narrow read handles used directly, without a projection view
// (an ingester calls ScanPrefix(dir); namespace calls Lookup(nsid)).
// Cardinality is many-to-many: a key maps to 0..N artifacts. What that
// multiplicity MEANS — a collision or a normal bucket — is the consuming
// view's call (07 Projection), not the accessor's. The backend detects
// each capability by assertion (if l, ok := ci.(KeyLookup); ok).
type KeyLookup interface {
	Lookup(key string) ([]domain.ArtifactID, error)
}

// PrefixScan walks the children of a prefix (a directory listing /
// subtree). The callback receives each key and the artifacts under it;
// returning an error stops the scan.
type PrefixScan interface {
	ScanPrefix(prefix string, cb func(Key, []domain.ArtifactID) error) error
}

// RangeScan walks keys in the half-open ordered range [lo, hi). The
// callback receives each key and the artifacts under it; returning an
// error stops the scan.
type RangeScan interface {
	ScanRange(lo, hi string, cb func(Key, []domain.ArtifactID) error) error
}

// Substrate is the backend-agnostic data plane a custom index
// uses for its own state. All mutations are scoped to the
// surrounding transaction; reads see committed state.
//
// Tables are auto-namespaced by custom index Name; the same `table`
// argument from two custom indexes does not collide.
type Substrate interface {
	// Put writes (or overwrites) a value for the given key.
	// Idempotent.
	Put(table, key string, value []byte) error

	// Get retrieves the value for a key.
	// (value, true, nil) — key present.
	// (nil, false, nil) — key absent.
	// Errors are reserved for backend-infrastructure failures.
	Get(table, key string) ([]byte, bool, error)

	// Delete removes a key. No-op if the key is absent (no
	// "not found" error).
	Delete(table, key string) error

	// DeletePrefix removes every key whose lexicographic value
	// starts with prefix. An empty prefix is rejected to make
	// "delete all" an explicit, deliberate operation (callers
	// who really want it must Scan and Delete one by one).
	DeletePrefix(table, prefix string) error

	// Scan iterates entries with the given prefix in
	// lexicographic key order. cb returning ErrStopScan stops
	// the walk without an error; any other error is propagated.
	Scan(table, prefix string, cb func(key string, value []byte) error) error

	// Inc adds delta to the int64 counter at key, creating it at delta
	// if absent, and returns the new value. It is an aggregate accumulator
	// for custom indexes maintaining incremental counts (e.g. artifacts per
	// tag, per-namespace totals) as part of their derived state.
	//
	// Atomicity is guaranteed only WITHIN the backend transaction — that is,
	// from Setup or Apply. Called from the read-side substrate it returns
	// ErrIncOutsideApply: outside the shared transaction the read-modify-write
	// cannot be atomic, and a counter bumped on a read path would serialise
	// the backend on its writer lock. Bump counters from Apply, where the
	// update is transactional and cannot drift from the main index write.
	Inc(table, key string, delta int64) (int64, error)
}

// Registry is the surface returned by
// StoreIndex.CustomIndexes. The only mutation is Register; backends
// expose nothing else through this interface.
type Registry interface {
	// Register attaches a custom index to the index. Setup runs
	// in a single transaction; failure rolls back any work the
	// custom index did during Setup, persisted schema_version is
	// not bumped, and no subscriptions are recorded.
	//
	// Returns ErrAlreadyRegistered if Name() collides with an
	// already-registered custom index.
	// Returns ErrSchemaRegression if SchemaVersion() is less
	// than the persisted value for this Name().
	Register(ctx context.Context, ext CustomIndex) error
}

// Sentinel errors. Wrapped by backends with %w so callers can
// errors.Is against them.
var (
	// ErrStopScan is returned by an Substrate.Scan callback
	// to stop the iteration without surfacing it as an error.
	ErrStopScan = errors.New("scrinium/index: stop scan")

	// ErrAlreadyRegistered is returned by Register when an
	// custom index with the same Name() is already registered.
	ErrAlreadyRegistered = errors.New("scrinium/index: custom index already registered")

	// ErrSchemaRegression is returned by Register when the
	// custom index's SchemaVersion() is less than the persisted
	// value. Backends do not auto-downgrade.
	ErrSchemaRegression = errors.New("scrinium/index: custom index schema version regressed")

	// ErrBackendMismatch is returned by custom index Setup when
	// the backend does not implement an interface the custom index
	// requires (typically a backend escape hatch like SQLBackend).
	ErrBackendMismatch = errors.New("scrinium/index: custom index incompatible with backend")

	// ErrEmptyPrefix is returned by DeletePrefix when called
	// with an empty prefix. "Delete all rows of a table" is an
	// explicit operation; callers must Scan and Delete to get it.
	ErrEmptyPrefix = errors.New("scrinium/index: empty prefix in DeletePrefix")

	// ErrIncOutsideApply is returned by Inc when it is called outside a
	// backend transaction — i.e. from the read-side substrate rather than
	// from within Setup or Apply. Inc is an aggregate accumulator whose
	// atomicity and consistency with the main index write depend on the
	// shared transaction; bumping a counter on a read path cannot be atomic
	// and would serialise the backend. Maintain counters from Apply.
	ErrIncOutsideApply = errors.New("scrinium/index: Inc called outside a transaction (Apply/Setup)")
)

// Info is the public, backend-agnostic descriptor of a
// registered index custom index. Surfaces (stats endpoints, debug
// pages, examples) consume this rather than reaching into a
// concrete backend type.
//
// Backends that report registered custom indexes return slices of
// this type via Lister. The type is intentionally
// flat — no behaviour, no pointers — so it can travel through
// any layer without dragging dependencies.
type Info struct {
	// Name is the custom index's stable identifier
	// (CustomIndex.Name()).
	Name string

	// SchemaVersion is the persisted schema version for this
	// custom index on this backend, after the most recent successful
	// Setup. Used to surface migration state.
	SchemaVersion int
}

// Host is the optional capability a StoreIndex backend
// exposes when it supports registering host-side custom indexes.
//
// Backends that support custom indexes implement this; the rest are
// transparently skipped by callers that type-assert it. Lives
// here (not on store.StoreIndex) so the core package needs no
// import of engine/index — the assertion happens at the wiring
// layer instead.
type Host interface {
	// CustomIndexes returns the registry through which CustomIndex
	// implementations are attached to this backend.
	CustomIndexes() Registry
}

// Lister is the optional capability a StoreIndex backend
// exposes when it can enumerate currently-registered custom indexes.
//
// Distinct from Host (the registration-side capability)
// because read and write surfaces are conceptually independent —
// a future read-only proxy backend might list without registering,
// or a constrained backend might register without listing. In
// practice today's sqlite backend implements both.
//
// Returns an empty slice (never nil) when no custom indexes are
// registered. Order is unspecified — callers that need stable
// listings sort by Name.
type Lister interface {
	ListCustomIndexes() []Info
}
