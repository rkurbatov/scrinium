package manifestfx

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
)

// syntheticDigest derives a stable, valid-shaped ManifestDigest from an
// artifact id. After the identity axis (ADR-83/92) the manifests table
// is keyed by manifest_digest, so two fixtures must carry two distinct
// digests to land two rows — distinct ids give distinct digests, while
// the same id re-used gives the same digest (idempotent re-index). The
// index treats the digest as an opaque PK; the production write path
// computes the real digest via artifact.ComputeManifestDigest, so this
// fixture value never reaches the store.
func syntheticDigest(id string) domain.ManifestDigest {
	sum := sha256.Sum256([]byte("manifestfx:" + id))
	return domain.ManifestDigest(hex.EncodeToString(sum[:]))
}

// Synthetic hashes used in fixtures. Cannot be const because
// strings.Repeat is a runtime call.
var (
	contentHashAaa = strings.Repeat("a", 64)
	blobRefBbb     = strings.Repeat("b", 64)
	// identityMetaCcc is a fixture-stable md (= H(canon(identity-meta)),
	// "sha256-"+hex). A user manifest's handle is PRF(NK, cd‖md), so a
	// valid user fixture must carry identity-meta (ADR-104 validateSlot).
	identityMetaCcc = "sha256-" + strings.Repeat("c", 64)
)

// Sample returns a minimal valid blob manifest with a fixed
// CreatedAt — byte-stable across runs for round-trip tests.
func Sample() domain.Manifest {
	return domain.Manifest{
		SessionID:    "sess-1",
		CreatedAt:    time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		ContentHash:  domain.ContentHash(contentHashAaa),
		OriginalSize: 4096,
		BlobRefs:     []domain.BlobRef{domain.BlobRef(blobRefBbb)},
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
	}
}

// Blob returns a small blob manifest with caller-supplied id and
// blobRef. ContentHash and OriginalSize are fixed — two manifests
// produced by Blob with different blobRef will collide on the
// (content_hash, original_size) UNIQUE index of the StoreIndex
// schema. Use this when:
//
//   - you only need one blob in the test;
//   - you want the dedup case (same blobRef passed twice or for
//     two different artifact IDs — bumps ref_count, no second row).
//
// For tests that need several distinct blobs, use BlobWithHash.
//
// CreatedAt is time.Now() — fine for index tests that don't check
// byte stability.
func Blob(id, blobRef string) domain.Manifest {
	return domain.Manifest{
		ArtifactID:       domain.ArtifactID(id),
		IdentityMetaHash: identityMetaCcc,
		Digest:           syntheticDigest(id),
		ContentHash:      domain.ContentHash(contentHashAaa),
		BlobRefs:         []domain.BlobRef{domain.BlobRef(blobRef)},
		OriginalSize:     1024,
		CreatedAt:        time.Now(),
	}
}

// BlobWithHash is Blob with a caller-controlled ContentHash and
// OriginalSize. Use whenever a test stages two or more independent
// blobs in the same index — distinct contentHash values keep the
// (content_hash, original_size) UNIQUE constraint happy.
//
// Convention for synthetic hashes: SyntheticHash('a') returns the
// canonical 64-char "a"-padded sha256-prefixed string.
func BlobWithHash(id, blobRef string, contentHash domain.ContentHash, originalSize int64) domain.Manifest {
	return domain.Manifest{
		ArtifactID:       domain.ArtifactID(id),
		IdentityMetaHash: identityMetaCcc,
		Digest:           syntheticDigest(id),
		ContentHash:      contentHash,
		BlobRefs:         []domain.BlobRef{domain.BlobRef(blobRef)},
		OriginalSize:     originalSize,
		CreatedAt:        time.Now(),
	}
}

// EncryptedBlobWithHash is BlobWithHash plus a single crypto
// Pipeline stage, so the index derives a non-empty crypto-identity
// ("<algo>/<keyID>") for the blob row. For ADR-58 dedup-key tests.
func EncryptedBlobWithHash(id, blobRef string, contentHash domain.ContentHash, originalSize int64, algo, keyID string) domain.Manifest {
	m := BlobWithHash(id, blobRef, contentHash, originalSize)
	m.Pipeline = []domain.PipelineStage{{Algorithm: algo, KeyID: keyID}}
	return m
}

// SyntheticHash builds a 64-hex-character bare hash (ADR-93) by
// repeating fillChar. Convenient when staging multiple blobs that
// must have distinct content hashes:
//
//	SyntheticHash('a') == "aaaa…aaaa"
//	SyntheticHash('b') == "bbbb…bbbb"
//
// fillChar must be a hex digit (0-9, a-f); the function does not
// validate — passing a non-hex character produces a string the
// hash registry will refuse to parse, which is the desired
// failure for a test feeding malformed input.
func SyntheticHash(fillChar byte) domain.ContentHash {
	// A content hash is bare hex (ADR-93), and the index stores it as raw
	// bytes — so a fixture cannot repeat an arbitrary character. The fill
	// char picks the byte instead: 'a' -> "6161...", 'x' -> "7878...".
	// Distinct chars still give distinct hashes, which is all a fixture
	// needs.
	return domain.ContentHash(strings.Repeat(hex.EncodeToString([]byte{fillChar}), 32))
}

// PhysAddr is a Location-workspace address at path.
func PhysAddr(path string) domain.PhysicalAddress {
	return domain.PhysicalAddress{
		Path: path,
	}
}

// ManifestWithVfsmetaPath builds a Blob manifest tagged with an vfsmeta
// virtual path — the common fixture for projection and surface tests
// that need an artifact placed at a known path in the by-path tree.
func ManifestWithVfsmetaPath(id, path string) domain.Manifest {
	m := Blob(id, strings.Repeat("b", 64))
	if err := AddVfsmetaPath(&m, path); err != nil {
		panic("manifestfx.ManifestWithVfsmetaPath: " + err.Error())
	}
	return m
}

// AddVfsmetaPath merges an fsmeta record carrying path into the
// manifest's Ext map under the "vfsmeta" key, preserving any other
// schemas already present.
func AddVfsmetaPath(m *domain.Manifest, path string) error {
	ext, err := vfsmeta.Embed(m.Ext, vfsmeta.FileSystem{Path: path})
	if err != nil {
		return err
	}
	m.Ext = ext
	return nil
}
