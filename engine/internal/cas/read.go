package cas

// read.go — the read half of the artifact I/O layer, mirroring the write
// half (Materialize/Assemble/Persist ↔ Load/OpenBlob/VerifyBlob). Pure
// mechanics over the injected Driver, StoreIndex, and registries plus the
// engine/artifact format; no *store back-reference, no event publishing
// (a verification failure is returned as ErrCorruptedBlob and the caller
// — store — decides whether to publish EventScrubFailed).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"scrinium.dev/domain"
	"scrinium.dev/engine/artifact"
	"scrinium.dev/engine/layout"
	"scrinium.dev/errs"
)

// Load reads, verifies, and decodes the manifest for the floating
// ArtifactID (handle) id.
//
// The manifest file is named by its ManifestDigest, not by the handle, so
// Load first resolves id → current digest through the index, reads the
// file at artifact.ManifestPath(digest), confirms digest == hash(file
// bytes) via artifact.VerifyManifestDigest, then decodes (Plain bypasses
// the resolver; Sealed/Paranoid consult keys). An unknown handle or a
// missing file is ErrArtifactNotFound; a hash mismatch is
// ErrCorruptedManifest.
//
// keys is the manifest key provider (store passes its KeyResolver adapted
// to domain.KeyProvider; nil means "no resolver" — Plain decodes, an
// encrypted manifest surfaces ErrKeyNotFound).
func (e *IO) Load(ctx context.Context, id domain.ArtifactID, keys domain.KeyProvider, hashAlgo string) (domain.Manifest, error) {
	if id == "" {
		return domain.Manifest{}, errs.ErrArtifactNotFound
	}
	digest, ok, err := e.index.ResolveManifestDigest(ctx, id)
	if err != nil {
		return domain.Manifest{}, fmt.Errorf("cas: resolve digest: %w", err)
	}
	if !ok {
		return domain.Manifest{}, errs.ErrArtifactNotFound
	}
	m, err := e.loadManifestByDigest(ctx, digest, keys, hashAlgo)
	if err != nil {
		return domain.Manifest{}, err
	}
	// The handle is also carried in the body; set the requested handle so
	// callers have both it and the physical digest.
	m.ArtifactID = id
	return m, nil
}

// loadManifestByDigest reads, verifies, and decodes the manifest file named
// directly by digest — no handle indirection. Load uses it after resolving
// id → digest through the index; headless resolution (ADR-105) uses it
// directly, since a headless data artifact's external reference IS its
// ManifestDigest. Plain bypasses the resolver; Sealed/Paranoid consult keys.
func (e *IO) loadManifestByDigest(ctx context.Context, digest domain.ManifestDigest, keys domain.KeyProvider, hashAlgo string) (domain.Manifest, error) {
	manifestPath, err := layout.ManifestPath(digest)
	if err != nil {
		return domain.Manifest{}, fmt.Errorf("cas: manifest path: %w", err)
	}
	rc, err := e.drv.Get(ctx, manifestPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.Manifest{}, errs.ErrArtifactNotFound
		}
		return domain.Manifest{}, fmt.Errorf("cas: read manifest: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(rc, domain.MaxManifestSize+1))
	_ = rc.Close()
	if err != nil {
		return domain.Manifest{}, fmt.Errorf("cas: read manifest body: %w", err)
	}
	if len(raw) > domain.MaxManifestSize {
		return domain.Manifest{}, errs.ErrManifestTooLarge
	}
	if err := artifact.VerifyManifestDigest(digest, raw, hashAlgo, e.hashes); err != nil {
		return domain.Manifest{}, err
	}
	m, err := artifact.DecodeEncrypted(raw, keys)
	if err != nil {
		return domain.Manifest{}, err
	}
	m.Digest = digest
	return m, nil
}

// OpenHandleByDigest resolves a headless data artifact by its ManifestDigest — the
// external_payload_ref form (ADR-105) — and returns a ReadHandle over its
// plaintext bytes. No
// handle indirection: the digest names the manifest file directly. Used to
// resolve an external system-artifact payload (e.g. a checkpoint .db).
func (e *IO) OpenHandleByDigest(ctx context.Context, digest domain.ManifestDigest, keys domain.KeyProvider, hashAlgo string) (domain.ReadHandle, error) {
	m, err := e.loadManifestByDigest(ctx, digest, keys, hashAlgo)
	if err != nil {
		return nil, err
	}
	return e.OpenHandle(ctx, m)
}

// OpenBlob returns a reader over the artifact's plaintext bytes: it opens
// the on-disk (or in-manifest) bytes and composes the inverse Pipeline
// (a no-op when the manifest has no stages). Closing the returned reader
// releases the underlying driver resource.
//
// Inline blobs are served from the manifest; Target blobs are resolved
// through the index (the read path follows where the blob was actually
// written, not what the current topology would compute) and opened
// through the Driver.
func (e *IO) OpenBlob(ctx context.Context, m domain.Manifest) (io.ReadCloser, error) {
	raw, err := e.openRawBlob(ctx, m)
	if err != nil {
		return nil, err
	}
	decoded, err := e.runner().BuildGet(m.Pipeline, raw)
	if err != nil {
		// BuildGet closed raw on its failure path.
		return nil, fmt.Errorf("cas: build pipeline: %w", err)
	}
	return decoded, nil
}

// openRawBlob returns the on-disk (ciphertext-shaped) bytes without any
// pipeline decoding. Closing the returned reader releases driver-side
// resources; for Inline it is a no-op.
func (e *IO) openRawBlob(ctx context.Context, m domain.Manifest) (io.ReadCloser, error) {
	switch m.LayoutHeader.BlobStorage {
	case domain.LayoutInline:
		return io.NopCloser(bytes.NewReader(m.InlineBlob)), nil

	case domain.LayoutTarget:
		addr, err := e.index.Resolve(ctx, string(m.PrimaryBlobRef()))
		if err != nil {
			return nil, fmt.Errorf("cas: resolve blob path: %w", err)
		}
		rc, err := e.drv.Get(ctx, addr.Path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errs.ErrCorruptedBlob
			}
			return nil, fmt.Errorf("cas: get blob: %w", err)
		}
		return rc, nil

	default:
		return nil, fmt.Errorf("cas: unknown BlobStorage %q", m.LayoutHeader.BlobStorage)
	}
}

// VerifyBlob re-hashes the artifact's plaintext bytes and compares against
// manifest.ContentHash. The algorithm is the manifest's hash_algo (ADR-93)
// (not the current config), so historical artifacts still verify. Any
// decode-side failure inside the inverse pipeline (AEAD tag mismatch,
// decompressor error) is folded into ErrCorruptedBlob; a context error is
// returned as-is. The caller decides whether to publish EventScrubFailed.
func (e *IO) VerifyBlob(ctx context.Context, m domain.Manifest) error {
	want, hasher, err := artifact.ParseContentHash(e.hashes, m.HashAlgo, m.ContentHash)
	if err != nil {
		return fmt.Errorf("cas: verify blob: %w", err)
	}

	plaintext, err := e.OpenBlob(ctx, m)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(hasher, plaintext)
	closeErr := plaintext.Close()
	if copyErr != nil {
		if errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			return copyErr
		}
		return errors.Join(errs.ErrCorruptedBlob, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("cas: close blob reader: %w", closeErr)
	}
	if !bytes.Equal(hasher.Sum(nil), want) {
		return errs.ErrCorruptedBlob
	}
	return nil
}

// LoadByDigest is loadManifestByDigest for callers outside the read path: the
// open-time reconciliation reads a manifest the index does not know, and must
// do it exactly the way a normal read does — verify the bytes against the
// name, then decode. Returns errs.ErrCorruptedManifest when the file does not
// hash to its own name, which is the single signal on which the caller is
// allowed to delete the file.
func (e *IO) LoadByDigest(ctx context.Context, digest domain.ManifestDigest, keys domain.KeyProvider, hashAlgo string) (domain.Manifest, error) {
	return e.loadManifestByDigest(ctx, digest, keys, hashAlgo)
}
