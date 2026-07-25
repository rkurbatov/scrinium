package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/artifact"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/hashing"
	"scrinium.dev/engine/layout"
	"scrinium.dev/engine/pipeline"
	"scrinium.dev/errs"
)

// casIndex is the slice of index.StoreIndex the write path depends on:
// it resolves a blob to its physical address, checks content existence for
// dedup, and registers the finished manifest. Declaring the narrow port
// here — rather than holding the full StoreIndex — keeps cas decoupled from
// index methods it never calls. The concrete *sqlite.Index, and any full
// index.StoreIndex value, satisfies it structurally.
type casIndex interface {
	IndexManifest(ctx context.Context, m domain.Manifest, addr domain.PhysicalAddress) error
	Resolve(ctx context.Context, blobRef string) (domain.PhysicalAddress, error)
	ResolveManifestDigest(ctx context.Context, id domain.ArtifactID) (domain.ManifestDigest, bool, error)
	ExistsByContent(ctx context.Context, hash domain.ContentHash, originalSize int64, crypto domain.CryptoIdentity) (blobRef string, exists bool, err error)
}

// IO is the artifact I/O engine bound to a store's Driver, StoreIndex,
// and registries. Construct once with New; the value is a thin
// handle over its dependencies and holds no mutable state.
type IO struct {
	drv          driver.Driver
	index        casIndex
	hashes       domain.HashRegistry
	transformers pipeline.TransformerRegistry
}

// New wires an IO to its dependencies. The store layer owns these
// objects and injects them so cas never reaches into *store
// internals.
func New(
	drv driver.Driver,
	index casIndex,
	hashes domain.HashRegistry,
	transformers pipeline.TransformerRegistry,
) *IO {
	return &IO{drv: drv, index: index, hashes: hashes, transformers: transformers}
}

// Result carries the outcome of Materialize: the addressing hashes, the
// original payload size, the pipeline stages recorded for the manifest,
// and either a non-nil inline body or a committed blob address. Fields are
// exported because the value crosses back into the store package (Put
// threads it into AssembleManifest and reads Addr for PersistManifest).
type Result struct {
	ContentHash  domain.ContentHash
	BlobRef      domain.BlobRef
	OriginalSize int64
	Stages       []domain.PipelineStage
	InlineBytes  []byte                 // non-nil iff the blob is inline
	Addr         domain.PhysicalAddress // zero for inline
}

// runner returns a pipeline.Runner bound to this writer's registries. A
// Runner is a cheap wrapper, built per operation rather than held.
func (e *IO) runner() *pipeline.Runner {
	return pipeline.NewRunner(e.hashes, e.transformers)
}

// Materialize hashes the payload and places it. For Inline mode it
// speculatively reads up to InlineBlobLimit+1 bytes: at or under the limit
// the bytes are embedded in the manifest; over it, the consumed head is
// spliced back and the payload streams to a Target blob. Plain stores
// always stream to Target.
//
// writeKeyID is resolved once by the caller (ADR-58) and threaded into
// both the blob pipeline here and the manifest body in AssembleManifest,
// so a blob and its manifest encrypt under the same key.
func (e *IO) Materialize(ctx context.Context, cfg config.StoreConfig, a domain.Artifact, opts domain.PutOptions, writeKeyID string) (Result, error) {
	hashAlgo := string(cfg.ContentHasher)
	inlineMode := cfg.BlobStorage == config.BlobStorageInline && cfg.InlineBlobLimit > 0

	if inlineMode {
		head, err := io.ReadAll(io.LimitReader(a.Payload, cfg.InlineBlobLimit+1))
		if err != nil {
			return Result{}, fmt.Errorf("cas: read payload head: %w", err)
		}
		if int64(len(head)) <= cfg.InlineBlobLimit {
			return e.materializeInline(hashAlgo, head)
		}
		combined := io.MultiReader(bytes.NewReader(head), a.Payload)
		return e.streamToTarget(ctx, cfg, hashAlgo, writeKeyID, combined)
	}
	return e.streamToTarget(ctx, cfg, hashAlgo, writeKeyID, a.Payload)
}

// materializeInline hashes an already-buffered payload and returns it as
// an inline blob. No driver entry, no dedup probe, and no blob_ref: the
// embedded bytes are not a physical blob, so they carry no blob address.
// Their stored-form integrity rides the manifest digest (InlineBlob is part
// of the hashed body); their identity is content_hash (ADR-66/92).
func (e *IO) materializeInline(hashAlgo string, body []byte) (Result, error) {
	h, err := e.hashes.NewHasher(hashAlgo)
	if err != nil {
		return Result{}, fmt.Errorf("cas: hasher: %w", err)
	}
	if _, err := h.Write(body); err != nil {
		return Result{}, fmt.Errorf("cas: hash inline: %w", err)
	}
	ch := domain.ContentHash(hex.EncodeToString(h.Sum(nil)))
	return Result{
		ContentHash:  ch,
		OriginalSize: int64(len(body)),
		Stages:       nil,
		InlineBytes:  body,
	}, nil
}

// streamToTarget runs the active pipeline over input, stages the result,
// then commits it to a blob slot (dedup hit drops the staging file; miss
// renames it into place).
func (e *IO) streamToTarget(ctx context.Context, cfg config.StoreConfig, hashAlgo, writeKeyID string, input io.Reader) (Result, error) {
	stagingPath := makeStagingPath()

	stream, pp, err := e.runner().BuildPut(hashAlgo, input, cfg.Pipeline, pipeline.EncodeContext{
		KeyID:          writeKeyID,
		EncryptedDedup: cfg.EncryptedDedup, // ADR-58: IV mode for the crypto stage
		SegmentSize:    cfg.SegmentSize,    // ADR-59: segmented AEAD frame size
	})
	if err != nil {
		return Result{}, fmt.Errorf("cas: build put pipeline: %w", err)
	}
	// Release stage goroutines on every exit: if drv.Put below abandons the
	// stream before EOF (staging error, ctx cancel), the zstd stages would
	// otherwise block forever on a full pipe. Harmless no-op after a clean
	// drain.
	defer pp.Close()

	if err := e.drv.Put(ctx, stagingPath, stream); err != nil {
		return Result{}, fmt.Errorf("cas: stage payload: %w", err)
	}

	contentHash, blobRef, stages := pp.Finalize()
	originalSize := pp.ContentBytesRead() // measured on the pre-pipeline input

	commitRef, addr, err := e.commitBlob(ctx, cfg, stagingPath, contentHash,
		originalSize, blobRef, domain.CryptoIdentityOf(stages))
	if err != nil {
		return Result{}, err
	}
	return Result{
		ContentHash:  contentHash,
		BlobRef:      commitRef,
		OriginalSize: originalSize,
		Stages:       stages,
		Addr:         addr,
	}, nil
}

// commitBlob is the tail of the Target write path: probe dedup, then drop
// the staging file (hit) or rename it into place (miss). Returns the
// BlobRef and the address where the blob now lives.
func (e *IO) commitBlob(
	ctx context.Context,
	cfg config.StoreConfig,
	stagingPath string,
	contentHash domain.ContentHash,
	originalSize int64,
	blobRef domain.BlobRef,
	crypto domain.CryptoIdentity,
) (domain.BlobRef, domain.PhysicalAddress, error) {
	existingRef, found, err := e.dedupProbe(ctx, contentHash, originalSize, blobRef, crypto)
	if err != nil {
		_ = e.drv.Remove(ctx, stagingPath)
		return "", domain.PhysicalAddress{}, fmt.Errorf("cas: dedup probe: %w", err)
	}
	if found {
		if err := e.drv.Remove(ctx, stagingPath); err != nil {
			return "", domain.PhysicalAddress{}, fmt.Errorf("cas: drop staging: %w", err)
		}
		addr, err := e.index.Resolve(ctx, existingRef)
		if err != nil {
			return "", domain.PhysicalAddress{}, fmt.Errorf("cas: resolve existing blob: %w", err)
		}
		return domain.BlobRef(existingRef), addr, nil
	}
	finalPath, err := layout.BlobPath(cfg.PathTopology, domain.BlobTypeRegular, string(blobRef))
	if err != nil {
		_ = e.drv.Remove(ctx, stagingPath)
		return "", domain.PhysicalAddress{}, fmt.Errorf("cas: resolve blob path: %w", err)
	}
	if err := e.drv.Rename(ctx, stagingPath, finalPath); err != nil {
		_ = e.drv.Remove(ctx, stagingPath)
		return "", domain.PhysicalAddress{}, fmt.Errorf("cas: commit blob: %w", err)
	}
	return blobRef, domain.PhysicalAddress{Path: finalPath}, nil
}

// dedupProbe decides whether a staged blob duplicates an indexed one
// (ADR-58), branching on crypto-identity:
//
//   - Plain (empty identity): probe by (ContentHash, OriginalSize). A
//     keyless transform is reproducible, so a hit is a true byte duplicate
//     — share it (preserves dedup-independent-of-compression).
//   - Encrypted (non-empty identity): probe by BlobRef. Under random-IV
//     Disabled the staged BlobRef is unique, so this never hits and we
//     never dedup; under Convergent identical ciphertext hits, the
//     intended dedup. Probing by content here would risk sharing a blob
//     encrypted under a different IV.
func (e *IO) dedupProbe(
	ctx context.Context,
	contentHash domain.ContentHash,
	originalSize int64,
	blobRef domain.BlobRef,
	crypto domain.CryptoIdentity,
) (string, bool, error) {
	if crypto == "" {
		return e.index.ExistsByContent(ctx, contentHash, originalSize, "")
	}
	if _, err := e.index.Resolve(ctx, string(blobRef)); err != nil {
		if errors.Is(err, errs.ErrArtifactNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return string(blobRef), true, nil
}

// AssembleManifest builds the manifest from the blob result, computes its
// floating ArtifactID (handle) and then its ManifestDigest. LayoutHeader
// records how this blob is laid out, independent of the current config —
// the read path trusts the header, which keeps manifests stable across
// config changes.
//
// Identity (ADR-73): in IdentityMode=Unique a fresh 16-byte nonce is mixed
// into the handle (every Put is distinct); in Coalesced the handle is
// deterministic (no nonce). The naming key is the public domain constant
// (Plain/Sealed v1). ComputeHandle must run before ComputeManifestDigest,
// because the handle is part of the body the digest hashes.
//
// dek and keyID come from the caller: for a Plain config dek is nil and
// keyID is empty; for an encrypting config the caller borrows the DEK
// under the crypto lock and wipes it after this returns. The DEK is used
// only for ComputeManifestDigest and is never retained.
func (e *IO) AssembleManifest(cfg config.StoreConfig, a domain.Artifact, opts domain.PutOptions, blob Result, dek []byte, keyID string) (domain.Manifest, []byte, error) {
	layout := domain.LayoutTarget
	if blob.InlineBytes != nil {
		layout = domain.LayoutInline
	}
	manifest := domain.Manifest{
		SessionID:      opts.SessionID,
		CreatedAt:      time.Now().UTC(),
		ContentHash:    blob.ContentHash,
		OriginalSize:   blob.OriginalSize,
		LayoutHeader:   domain.LayoutHeader{BlobStorage: layout},
		Pipeline:       blob.Stages,
		InlineBlob:     blob.InlineBytes,
		RetentionUntil: opts.RetentionUntil,
		Ext:            a.Ext,
		Usr:            a.Usr,
	}
	if blob.BlobRef != "" {
		manifest.BlobRefs = []domain.BlobRef{blob.BlobRef}
	}
	// Edges to source artifacts (ADR-92/112), copied verbatim and in order:
	// position is part of the edge's identity in the index. They go in BEFORE
	// ComputeHandle, because handle_refs is part of the body the handle and
	// then the digest hash — an artifact's sources are part of what it is.
	// Shape is validated at the encode boundary (checkRefLimits/checkHandleRefs);
	// targets are never resolved here (a declarative edge, not a join).
	if len(opts.ParentRefs) > 0 {
		manifest.HandleRefs = append([]domain.HandleRef(nil), opts.ParentRefs...)
	}

	hashAlgo := string(cfg.ContentHasher)

	nonce, err := identityNonce(cfg.IdentityMode)
	if err != nil {
		return domain.Manifest{}, nil, fmt.Errorf("cas: identity nonce: %w", err)
	}
	withHandle, err := artifact.ComputeHandle(manifest, hashAlgo, e.hashes, hashing.NamingKeyPublic, nonce)
	if err != nil {
		return domain.Manifest{}, nil, fmt.Errorf("cas: compute handle: %w", err)
	}

	_, fileBytes, sm, err := artifact.ComputeManifestDigest(
		withHandle, hashAlgo, e.hashes,
		cfg.ManifestEncoding, cfg.ManifestCrypto, dek, keyID,
	)
	if err != nil {
		// A blob (if any) is already committed; we do not roll it back. An
		// orphan blob is harmless — ref_count stays 0 and GC reaps it —
		// whereas an inverse Rename could race a parallel dedup on the same
		// content.
		return domain.Manifest{}, nil, fmt.Errorf("cas: compute manifest digest: %w", err)
	}
	return sm, fileBytes, nil
}

// WriteHeadless writes a headless blob-backed data artifact (ADR-105): input
// is materialized to a physical blob and wrapped in a CONTAINER manifest —
// both identity slots empty (no handle, no name, no identity-meta),
// blob_refs=[blob] — which is persisted and indexed, so orphan scan / GC
// treat it as live (a headless data artifact is reachable only by its digest,
// so it must be indexed or it would be reaped). Returns the ManifestDigest:
// the stable external reference an envelope's external_payload_ref carries.
//
// This is the simplest case of the pack container (one blob, no members, no
// TOC). A data artifact is never inline — it always streams to a Target blob,
// regardless of the store's inline-blob policy — because its whole purpose is
// to carry a payload too large for an inline manifest. The manifest body
// follows the store's ManifestCrypto (dek/keyID supplied by the caller, as for
// a user Put), so the blob and its manifest encrypt under the same key.
func (e *IO) WriteHeadless(ctx context.Context, cfg config.StoreConfig, input io.Reader, dek []byte, keyID string) (domain.ManifestDigest, error) {
	hashAlgo := string(cfg.ContentHasher)
	blob, err := e.streamToTarget(ctx, cfg, hashAlgo, keyID, input)
	if err != nil {
		return "", fmt.Errorf("cas: headless materialize: %w", err)
	}

	// Container slot: no handle, no name, no identity-meta; blob_refs holds
	// the single data blob. validateSlot accepts this as a container.
	m := domain.Manifest{
		CreatedAt:    time.Now().UTC(),
		ContentHash:  blob.ContentHash,
		OriginalSize: blob.OriginalSize,
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
		Pipeline:     blob.Stages,
		BlobRefs:     []domain.BlobRef{blob.BlobRef},
	}

	_, fileBytes, sm, err := artifact.ComputeManifestDigest(
		m, hashAlgo, e.hashes,
		cfg.ManifestEncoding, cfg.ManifestCrypto, dek, keyID,
	)
	if err != nil {
		// The blob is already committed; we do not roll it back (an orphan
		// blob is harmless — ref_count stays 0 and GC reaps it).
		return "", fmt.Errorf("cas: headless manifest: %w", err)
	}
	if err := e.PersistManifest(ctx, sm, fileBytes, blob.Addr); err != nil {
		return "", err
	}
	return sm.Digest, nil
}

// identityNonce returns a fresh 16-byte nonce for IdentityMode=Unique (the
// default when unset) and nil for Coalesced, so the handle is unique
// per-Put or deterministic respectively (ADR-73).
func identityNonce(mode config.IdentityMode) ([]byte, error) {
	if mode == config.IdentityModeCoalesced {
		return nil, nil
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// PersistManifest writes the manifest file and indexes it. The manifest
// file is named by its ManifestDigest (the physical form); the index row
// records both the digest and the floating ArtifactID so Load can resolve
// handle → digest. For inline manifests addr is the zero PhysicalAddress;
// IndexManifest skips the blobs-table insert but still indexes the
// manifest so Walk and GetBySession find it.
func (e *IO) PersistManifest(ctx context.Context, manifest domain.Manifest, manifestBytes []byte, addr domain.PhysicalAddress) error {
	manifestPath, err := layout.ManifestPath(manifest.Digest)
	if err != nil {
		return fmt.Errorf("cas: manifest path: %w", err)
	}
	if err := e.drv.Put(ctx, manifestPath, bytes.NewReader(manifestBytes)); err != nil {
		return fmt.Errorf("cas: write manifest: %w", err)
	}
	if err := e.index.IndexManifest(ctx, manifest, addr); err != nil {
		// Manifest is on disk but unindexed; the rebuild agent is the
		// recovery path. Surface so the caller can retry.
		return fmt.Errorf("cas: index manifest: %w", err)
	}
	return nil
}

// makeStagingPath returns a fresh unique path under the staging prefix;
// uniqueness comes from a UUIDv4.
func makeStagingPath() string {
	return domain.StagingPrefix + "/" + uuid.NewString()
}
