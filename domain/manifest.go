package domain

import (
	"encoding/json"
	"io"
	"time"
)

// Artifact is the abstraction at the system boundary (input/output).
// It consists of a byte stream (Payload) and two metadata blocks
// per ADR-54:
//
//   - Ext: Scrinium-custom index data the engine itself reads
//     (vfsmeta and friends).
//   - Usr: opaque host-application data — tags, business
//     attributes; the engine never inspects them.
//
// Each block has a 64 KiB limit (MaxExtSize, MaxUsrSize).
type Artifact struct {
	Payload io.Reader

	Ext json.RawMessage
	Usr json.RawMessage
}

// LayoutHeader is a service attribute inside a Manifest that
// "freezes" the physical-projection rules applied at write time.
type LayoutHeader struct {
	BlobStorage string
}

// Layout* are the canonical values for LayoutHeader.BlobStorage.
// Distinct from BlobStorage* (which is the StoreConfig-time policy):
// the configuration says "Inline", but the resolved layout
// for any specific manifest is either LayoutInline (the payload fit
// the inline limit) or LayoutTarget (it overflowed).
//
// Production code must compare against these constants, not the
// equivalent string literals.
const (
	LayoutInline = "Inline"
	LayoutTarget = "Target"
)

// PipelineStage is a single transformation stage in the Pipeline.
//
// KeyID is populated for crypto stages whose plugin resolves its
// DEK through a store.KeyResolver. On Put the Encoder records the
// KeyID the engine resolved (ResolveWriteKey) and passed via
// EncodeContext; on Get the Decoder looks up candidate keys for
// the recorded KeyID. The field is empty for non-crypto stages
// and for crypto plugins that pin the DEK at factory construction
// time (single-key factory wiring).
type PipelineStage struct {
	Algorithm string
	Hash      string
	IV        []byte
	KeyID     string
}

// Manifest is the logical passport of an Artifact.
//
// Reference model (ADR-92): a manifest carries an identity slot
// (handle / name / empty) plus two ordered reference arrays —
// BlobRefs (blobs: physical content) and HandleRefs (edges to other
// artifacts: the content-addressed DAG). Direction is implied by the
// slot: a filled slot consumes its members (top-down, +ref_count); an
// empty slot (pack container) places them (bottom-up, no ref_count).
//
// The plain blob is BlobRefs[0] (ADR-92).
type Manifest struct {
	// ArtifactID is the floating external identity (handle):
	// PRF(NK, cd‖md). It is what the outside world holds and what
	// Put returns. SERIALISED in the body — it is an input computed
	// from cd‖md (+ nonce in Unique mode), not the hash of the file,
	// so it must be stored to be reproducible and to survive index
	// loss (rebuild reads it from the manifest). Stable across form
	// changes; changes only on content (cd) or naming-key-domain
	// change. Empty for system artifacts (slot filled by Name) and
	// pack containers (slot empty).
	ArtifactID ArtifactID

	// Name is the identity slot of a system artifact
	// (config.StoreConfig.<seq>, store.agent.orphanscan.last,
	// extension.bundler.migration.pending, …), written via
	// AdminStore.System().Put (ADR-85). Present ⟺ system artifact;
	// the user Put path never sets it. Empty for user artifacts and
	// pack containers.
	Name string

	// Digest is the manifest digest — hash of the full serialised
	// file bytes (header included). In-memory ONLY: it is the hash
	// of the body, so it cannot live inside the body. It is the
	// on-disk filename and the form-verifier; it CHANGES on repack.
	// Set at two places only:
	//   - artifact.ComputeManifestDigest, after encoding;
	//   - store.loadManifest, from the path used to fetch the file.
	// The index maps ArtifactID (handle) → Digest.
	Digest ManifestDigest

	// IdentityMetaHash is md = H(canon(identity-meta)), an input to
	// ArtifactID. SERIALISED. The identity partition is empty by
	// default → md is a constant token; an application may opt
	// specific fields into identity. Descriptive metadata
	// (CreatedAt, Usr) and per-run fields are never in md.
	IdentityMetaHash string

	// IdentityNonce is 16 random bytes mixed into ArtifactID in
	// IdentityMode=Unique (makes the handle unique per Put); absent
	// in Coalesced. SERIALISED, so the handle stays reproducible.
	IdentityNonce []byte

	SessionID SessionID
	CreatedAt time.Time

	ContentHash  ContentHash
	OriginalSize int64

	// HashAlgo is the content-address algorithm ("sha256"/"blake3"): the
	// store's immutable ContentHasher, recorded once (ADR-93). The
	// bare-hex content_hash / blob_refs and the manifest digest are
	// re-hashable from it without StoreConfig (self-description, Principle 4).
	HashAlgo string

	// BlobRefs is the ordered array of blob references the manifest
	// owns (ADR-92/93), at most 65535. Direction is implied by the
	// identity slot:
	//   - plain blob: a single element;
	//   - composite (chunker, "composite" flag in Ext): the ordered
	//     list of chunk hashes — the source of truth, no TOC blob
	//     (ADR-87);
	//   - pack container (empty slot): [toc_blob, pack_blob] (ADR-86).
	// Empty for the Inline strategy (the inline blob is not tracked).
	BlobRefs []BlobRef

	// HandleRefs is the ordered array of edges to OTHER artifacts —
	// the content-addressed DAG (ADR-92), at most 65535. Elements are
	// HandleRef (the handle address space), symmetric to BlobRefs/BlobRef.
	// Direction by slot: a filled slot consumes the targets (+ref_count,
	// top-down); an empty slot (pack container) places them (bottom-up, no
	// ref_count — the pack members). nil/empty for a plain blob and a
	// composite (whose members are blobs, in BlobRefs).
	HandleRefs []HandleRef

	LayoutHeader LayoutHeader
	Pipeline     []PipelineStage
	InlineBlob   []byte

	RetentionUntil time.Time
	KeyID          string

	Ext json.RawMessage
	Usr json.RawMessage
}

// IsUser reports whether the manifest is a user artifact — the
// identity slot is filled by the floating handle (ADR-92). User
// artifacts are the roots of the DAG visible to Walk/Get.
func (m *Manifest) IsUser() bool { return m.ArtifactID != "" }

// IsSystem reports whether the manifest is a system artifact — the
// identity slot is filled by Name (ADR-85). System artifacts are
// addressed by name and excluded from the user Walk.
func (m *Manifest) IsSystem() bool { return m.Name != "" }

// IsContainer reports whether the manifest is a headless pack
// container — both identity slots are empty (ADR-86/92). A container
// is the rebuild anchor of a pack volume: it carries
// BlobRefs = [toc_blob, pack_blob] and HandleRefs = members (as
// placement) and is excluded from the user Walk.
func (m *Manifest) IsContainer() bool { return m.ArtifactID == "" && m.Name == "" }

// PrimaryBlobRef returns the plain blob reference (BlobRefs[0]) — the
// single physical blob a plain-blob manifest owns — or "" when the
// manifest has no blobs (Inline). Convenience for readers that handled
// a single BlobRef before ADR-92; the array stays the source of truth.
func (m Manifest) PrimaryBlobRef() BlobRef {
	if len(m.BlobRefs) > 0 {
		return m.BlobRefs[0]
	}
	return ""
}

// IsComposite reports whether the manifest is a chunked composite —
// the chunker's "composite" flag is set in Ext (ADR-87). The flag is
// for the chunker custom index; the core does not branch on it. For a
// composite, BlobRefs holds the ordered chunk list.
func (m *Manifest) IsComposite() bool {
	if len(m.Ext) == 0 {
		return false
	}
	var probe struct {
		Composite bool `json:"composite"`
	}
	if err := json.Unmarshal(m.Ext, &probe); err != nil {
		return false
	}
	return probe.Composite
}

// Manifest and identifier format limits — fixed on-disk/protocol caps,
// NOT configurable. They come from the engine specification §5.6 and
// describe the wire format (identifier lengths, manifest block sizes,
// reference-array counts, manifest read bound); changing them would
// break compatibility, so they live with the manifest in domain rather
// than among the configurable bounds in package config.

// MaxNamespaceLen is the maximum byte length of Namespace.
// Returns errs.ErrNamespaceTooLong when exceeded.
// Standard FS/DB name limit.
const MaxNamespaceLen = 255

// MaxSessionIDLen is the maximum byte length of SessionID.
// Returns errs.ErrSessionIDTooLong when exceeded.
// Standard FS/DB name limit.
const MaxSessionIDLen = 255

// MaxKeyIDLength is the upper bound on KeyID byte length in the
// manifest file header per §7.1. The KeyID-length byte is one
// octet, so the KeyID itself can be at most 255 bytes; producers
// that hit the limit must shorten the identifier or prefix-hash
// it externally. Returns a wrapped error from engine/artifact when
// exceeded.
const MaxKeyIDLength = 255

// MaxExtSize is the maximum byte size of the Manifest Ext
// block (custom index data the engine itself reads — vfsmeta, etc).
// 64 KiB. Returns errs.ErrExtTooLarge when exceeded.
const MaxExtSize = 64 * 1024

// MaxUsrSize is the maximum byte size of the Manifest Usr block
// (opaque host-application data). 64 KiB. Returns
// errs.ErrUsrTooLarge when exceeded.
const MaxUsrSize = 64 * 1024

// MaxBlobRefs and MaxHandleRefs cap the manifest reference arrays
// (ADR-92/93): the on-disk chunk/member list is 16-bit length-counted, so
// each array holds at most 65535 entries. Exceeding either returns
// errs.ErrTooManyRefs on write. The encode path has no overall byte cap —
// it is bounded field-by-field — while reads are guarded by
// MaxManifestSize (below) against corrupt or oversized files.
const (
	MaxBlobRefs   = 65535
	MaxHandleRefs = 65535
)

// MaxManifestSize bounds a manifest file on READ, so a corrupt or hostile
// file cannot force an unbounded allocation before the per-field limits
// (checked post-parse) can fire. It is derived from the per-field maxima,
// not arbitrary: a worst-case well-formed manifest is
// (MaxBlobRefs + MaxHandleRefs) hex digests + MaxExtSize + MaxUsrSize +
// fixed overhead — ~9 MiB for SHA-256, ~17 MiB for SHA-512. 32 MiB leaves
// headroom for the longest registered hash. The encode path does NOT use
// this — it is bounded field-by-field. Returns errs.ErrManifestTooLarge.
const MaxManifestSize = 32 * 1024 * 1024
