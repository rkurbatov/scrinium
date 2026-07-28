package indexsuite

import (
	"encoding/json"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- IndexManifest ---

func runIndexManifest(t *testing.T, f Factory) {
	t.Run("Blob_FreshInsert", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		m := manifestfx.Blob("art-1", "b10b0001")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatalf("IndexManifest: %v", err)
		}
		// Manifest visible, and ResolveManifestDigest returns the digest
		// the manifest was registered under.
		digest, exists, err := idx.ResolveManifestDigest(ctx, "art-1")
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Error("manifest must be visible after IndexManifest")
		}
		if digest != m.Digest {
			t.Errorf("ResolveManifestDigest: got digest %q, want %q", digest, m.Digest)
		}
		// Blob has a ref.
		n, err := idx.GetRefCount(ctx, "b10b0001")
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("ref_count: got %d, want 1", n)
		}
		// Blob is resolvable.
		if _, err := idx.Resolve(ctx, "b10b0001"); err != nil {
			t.Errorf("Resolve after Index: %v", err)
		}
	})

	t.Run("b10b0ded0f", func(t *testing.T) {
		ctx := t.Context()
		// Two distinct artifacts referencing the same blob —
		// blob row stays single, ref_count climbs to 2.
		idx := f.New(t)
		addr := manifestfx.PhysAddr("blobs/aa/bb/blob-1")
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), addr); err != nil {
			t.Fatal(err)
		}
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-2", "b10b0001"), addr); err != nil {
			t.Fatal(err)
		}
		n, err := idx.GetRefCount(ctx, "b10b0001")
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("ref_count: got %d, want 2", n)
		}
	})

	t.Run("Blob_Idempotent", func(t *testing.T) {
		ctx := t.Context()
		// Re-indexing the same artifact (same ID, same blobRef)
		// must not fail. Manifest-row uniqueness is the strict
		// invariant; ref_count behaviour on retries is an
		// implementation detail covered by the per-backend tests.
		idx := f.New(t)
		m := manifestfx.Blob("art-1", "b10b0001")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatalf("re-indexing same manifest must not fail: %v", err)
		}
		_, exists, err := idx.ResolveManifestDigest(ctx, "art-1")
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Error("manifest disappeared on second IndexManifest")
		}
	})

	t.Run("Composite_RegistersChunks", func(t *testing.T) {
		ctx := t.Context()
		// A composite manifest references previously-registered chunk
		// blobs via blob_refs (ADR-87/92 — no TOC blob). Each chunk's
		// ref_count climbs by one; the composite has no separate body blob.
		idx := f.New(t)

		chunks := []struct {
			ref  string
			hash domain.ContentHash
		}{
			{"c40a0001", manifestfx.SyntheticHash('a')},
			{"c40b0002", manifestfx.SyntheticHash('b')},
			{"c40c0003", manifestfx.SyntheticHash('c')},
		}
		// Register chunks as blobs first, each via its own
		// IndexManifest call. The manifest is artificial — what
		// matters for the composite below is that the blob row
		// exists.
		for i, c := range chunks {
			m := manifestfx.BlobWithHash(
				"chunk-mf-"+c.ref,
				c.ref,
				c.hash,
				1024,
			)
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("chunks/"+c.ref)); err != nil {
				t.Fatalf("seed chunk %d: %v", i, err)
			}
		}

		toc := domain.Manifest{
			ArtifactID:   "art-toc",
			Digest:       domain.ManifestDigest("70c0000000000000000000000000000000000000000000000000000000000001"),
			Ext:          json.RawMessage(`{"composite":true}`),
			ContentHash:  manifestfx.SyntheticHash('0'),
			BlobRefs:     []domain.BlobRef{domain.BlobRef(chunks[0].ref), domain.BlobRef(chunks[1].ref), domain.BlobRef(chunks[2].ref)},
			OriginalSize: 3072,
			CreatedAt:    time.Now(),
		}
		if err := idx.IndexManifest(ctx, toc, manifestfx.PhysAddr("composite/art-toc")); err != nil {
			t.Fatalf("IndexManifest composite: %v", err)
		}

		// Each chunk now ref-counted: 1 (from its own manifest)
		// + 1 (from the composite's blob_refs) = 2.
		for _, c := range chunks {
			n, err := idx.GetRefCount(ctx, c.ref)
			if err != nil {
				t.Fatal(err)
			}
			if n != 2 {
				t.Errorf("chunk %s ref_count: got %d, want 2", c.ref, n)
			}
		}
	})
}
