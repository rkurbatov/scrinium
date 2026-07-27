package index

import (
	"context"
	"time"

	"scrinium.dev/domain"
)

// StoreIndex is the index of a single Store. Every mutating method
// encapsulates its transaction inside; the calling code never
// drives transactions explicitly.
//
// Implementations (in-memory, sqlite, postgres) live in subpackages
// of index/.
//
// Every method that performs I/O takes a context.Context as the
// first argument. Backends honour cancellation: a deadline expiry
// or explicit cancel during a SQL call surfaces as the standard
// context error, classified through the backend's error mapper to
// the appropriate scrinium sentinel where applicable. The single
// exception is Close, which is an idempotent shutdown step and
// follows the standard io.Closer signature.
type StoreIndex interface {
	// Writes and deletes.

	// IndexManifest registers an artifact in the index. Loose,
	// composite and headless pack containers index uniformly (ADR-87/92):
	// every blob in blob_refs is registered, ref-counted and linked
	// positionally — a composite's chunk list lives in blob_refs and the
	// core keeps its ref_count. Pack PLACEMENT (the per-member slice map)
	// is owned by the bundler's custom-index Resolver (ADR-86),
	// recorded out-of-band via its RecordPack API — the core index holds
	// no pack state.
	IndexManifest(
		ctx context.Context,
		m domain.Manifest,
		addr domain.PhysicalAddress,
	) error

	// DeleteManifest performs a logical deletion keyed by manifest
	// digest (the table PK): a single transaction that decrements
	// ref_count for each blob the manifest references (read from
	// manifest_blobs — the authoritative set) and removes the
	// manifest row and its edges. Idempotent: deleting an absent
	// digest is a no-op.
	DeleteManifest(ctx context.Context, digest domain.ManifestDigest) error

	// Resolution and existence checks.

	// Resolve returns the physical address for a BlobRef.
	Resolve(ctx context.Context, blobRef string) (domain.PhysicalAddress, error)

	// ResolveManifestDigest returns the current ManifestDigest (the
	// on-disk filename) for a floating ArtifactID (handle). The read
	// path uses it to find the manifest file. (false, nil) when the
	// handle is unknown.
	ResolveManifestDigest(ctx context.Context, id domain.ArtifactID) (domain.ManifestDigest, bool, error)

	// ManifestExistsByDigest reports whether a manifest row references
	// the given ManifestDigest. The Orphan Scan uses it: manifest files
	// are named by digest, so this maps a listed file back to a row.
	ManifestExistsByDigest(ctx context.Context, digest domain.ManifestDigest) (bool, error)

	// ExistsByContent is an exact check by the composite dedup key
	// (ContentHash, OriginalSize, CryptoIdentity) for regular blobs.
	// CryptoIdentity is empty for Plain blobs, in which case the key
	// degrades to (ContentHash, OriginalSize) — see
	// ADR-58. A hit means a byte-reproducible duplicate exists; the
	// caller may drop its staging blob and reference the survivor.
	ExistsByContent(ctx context.Context, hash domain.ContentHash, originalSize int64, crypto domain.CryptoIdentity) (blobRef string, exists bool, err error)

	// ExistsByHash is the chunk-deduplication probe with tombstone
	// distinction, used by chunker.Wrapper. Like ExistsByContent it
	// keys on the full dedup triple (ContentHash, OriginalSize,
	// CryptoIdentity) — a chunk is anonymous in name but not in size
	// (its length is known) or in crypto-identity, so under
	// EncryptedDedup=Disabled two encrypted chunks of the same
	// plaintext must not collapse (ADR-58). CryptoIdentity is empty
	// for a Plain chunk, degrading the key to (ContentHash,
	// OriginalSize). The return value distinguishes a live blob from a
	// tombstoned one (BlobExists / BlobIsTombstone / BlobNotFound).
	ExistsByHash(ctx context.Context, hash domain.ContentHash, originalSize int64, crypto domain.CryptoIdentity) (domain.BlobExistStatus, error)

	// GetRefCount returns the current reference count of a blob.
	GetRefCount(ctx context.Context, blobRef string) (int, error)

	// Iteration. Implementations are required to stream through the
	// callback rather than load the whole result set into memory.

	// IterateManifests iterates over every user manifest (artifact_id
	// present), namespace-agnostic. Returns blob and toc; pack is
	// excluded. Namespace-filtered iteration is not a core concern —
	// extensions filter via ListByExtField over their projection.
	IterateManifests(ctx context.Context, cb func(domain.Manifest) error) error

	// QueryByExtField streams the ArtifactIDs whose projected ext field
	// extName.field equals value, read from proj_ext (read-side of the
	// Indexer projection, ADR-78/88). It is the lower level for Store.Walk
	// over a namespace (extName="namespace", field="nsid") and for basic
	// metadata-search; v1 is equality only (a richer language is M7). Only
	// visible artifacts surface — handle-less rows (system artifacts, pack
	// containers) are excluded by construction (artifact_id IS NULL). The
	// callback may return fs.SkipAll to stop early without an error.
	QueryByExtField(ctx context.Context, extName, field, value string, cb func(domain.ArtifactID) error) error

	// ListByExtField is the manifest-yielding form of QueryByExtField: it
	// streams the index-resident Manifest of each artifact whose projected
	// ext field extName.field equals value (proj_ext, §9.6), no manifest-file
	// I/O. It is the proj_ext-backed listing primitive behind a namespace
	// Walk (extName="namespace", field="nsid") and any equality listing over
	// a projected ext field; the core attaches no meaning to extName/field.
	// Handle-less rows are excluded (artifact_id IS NULL). Equality only (v1).
	ListByExtField(ctx context.Context, extName, field, value string, cb func(domain.Manifest) error) error

	// ListOrphanBlobs iterates over blobs with ref_count = 0. Used
	// by the GC Agent.
	ListOrphanBlobs(ctx context.Context, cb func(blobRef string) error) error

	// DeleteOrphanBlob removes a blob's index row, but ONLY if its
	// ref_count is still 0. The GC Agent calls it after the physical
	// file has been swept (Driver.Remove past the grace period); the
	// ref_count = 0 guard makes it race-safe against a concurrent
	// Revive — if another host re-referenced the blob between Sweep
	// and this call, ref_count is no longer 0 and the row is kept.
	// A blob_ref that is absent or no longer orphaned is a no-op (the
	// returned removed flag reports whether a row was deleted).
	DeleteOrphanBlob(ctx context.Context, blobRef string) (removed bool, err error)

	// ListUnverifiedBlobs iterates over blobs whose last_verified_at
	// is older than `before`. Used by the Scrub Agent's blob pass.
	// Named symmetrically with ListUnverifiedManifests below: blobs
	// carry the expensive plaintext check, manifests the cheap
	// metadata re-hash.
	ListUnverifiedBlobs(ctx context.Context, before time.Time, cb func(blobRef string) error) error

	// ManifestsByBlobRef iterates over every manifest that references
	// the given blobRef, via the manifest_blobs edge table. Used by
	// the Scrub Agent's cascade: after a physical blob is re-hashed,
	// each consuming manifest is cheaply re-verified and stamped. A
	// blob shared by N artifacts (dedup) yields N manifests; all share
	// the same ContentHash and pipeline (both are content-derived), so
	// any one is a valid source for the blob's expected hash.
	//
	// The yielded Manifest carries the index-resident fields only
	// (no Pipeline/LayoutHeader — those live in the manifest file);
	// callers that need them read the file.
	ManifestsByBlobRef(ctx context.Context, blobRef string, cb func(domain.Manifest) error) error

	// ListUnverifiedManifests iterates over manifests whose
	// last_verified_at is older than `before`. Used by the Scrub
	// Agent's manifest pass, which covers Inline artifacts: they have
	// no blobs row and so never surface through ListUnverifiedBlobs.
	ListUnverifiedManifests(ctx context.Context, before time.Time, cb func(domain.Manifest) error) error

	// GetBySession returns every ArtifactID with the given
	// SessionID. Used by RollbackSession.
	GetBySession(ctx context.Context, sessionID domain.SessionID) ([]domain.ArtifactID, error)

	// Verification and maintenance.

	// MarkVerified updates last_verified_at for a blob.
	MarkVerified(ctx context.Context, blobRef string, timestamp time.Time) error

	// MarkManifestVerified updates last_verified_at for a manifest
	// (the manifest-level scrub stamp). Set by the Scrub
	// Agent once an artifact is fully verified: for a single-blob
	// artifact after its blob and manifest pass; for Inline artifacts
	// after the manifest re-hash; for multi-blob (TOC) artifacts once
	// every referenced blob is fresh.
	MarkManifestVerified(ctx context.Context, artifactID domain.ArtifactID, timestamp time.Time) error

	// Lifecycle.

	// Close releases resources held by the index — database
	// connections, file handles, internal goroutines. The host
	// application owns the StoreIndex's lifetime (DI contract: see
	// store.WithStoreIndex doc) and must call Close after the Store
	// has been shut down.
	//
	// Idempotent: a second Close on an already-closed index returns
	// nil. Operations on a closed index return an implementation-
	// defined error; do not call Close while reads are in flight.
	Close() error
}
