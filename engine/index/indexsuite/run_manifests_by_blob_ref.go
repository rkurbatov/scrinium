package indexsuite

import (
	"sort"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- ManifestsByBlobRef ---

func runManifestsByBlobRef(t *testing.T, f Factory) {
	t.Run("ReturnsAllConsumersOfASharedBlob", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)

		// Two artifacts dedup onto the same blob_ref: IndexManifest is
		// idempotent on the blobs row (ON CONFLICT DO NOTHING) and adds
		// a manifest_blobs edge per artifact.
		for _, id := range []string{"art-a", "art-b"} {
			m := manifestfx.Blob(id, "b10b5ded")
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
				t.Fatalf("IndexManifest %s: %v", id, err)
			}
		}
		// A third artifact on a different blob must NOT be returned.
		other := manifestfx.Blob("art-c", "b10b0aef")
		if err := idx.IndexManifest(ctx, other, manifestfx.PhysAddr("p")); err != nil {
			t.Fatalf("IndexManifest art-c: %v", err)
		}

		var got []string
		err := idx.ManifestsByBlobRef(ctx, "b10b5ded", func(m domain.Manifest) error {
			got = append(got, string(m.ArtifactID))
			return nil
		})
		if err != nil {
			t.Fatalf("ManifestsByBlobRef: %v", err)
		}
		sort.Strings(got)
		if len(got) != 2 || got[0] != "art-a" || got[1] != "art-b" {
			t.Errorf("consumers = %v, want [art-a art-b]", got)
		}
	})

	t.Run("CarriesContentHashFromBlobsRow", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		want := manifestfx.SyntheticHash('c')
		m := manifestfx.BlobWithHash("art-1", "b10b0001", want, 2048)
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		var seen domain.Manifest
		var n int
		err := idx.ManifestsByBlobRef(ctx, "b10b0001", func(m domain.Manifest) error {
			seen = m
			n++
			return nil
		})
		if err != nil {
			t.Fatalf("ManifestsByBlobRef: %v", err)
		}
		if n != 1 {
			t.Fatalf("got %d manifests, want 1", n)
		}
		if seen.ContentHash != want {
			t.Errorf("ContentHash = %q, want %q (recovered via blobs join)", seen.ContentHash, want)
		}
		if seen.OriginalSize != 2048 {
			t.Errorf("OriginalSize = %d, want 2048", seen.OriginalSize)
		}
	})

	t.Run("UnknownBlobYieldsNothing", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		var n int
		err := idx.ManifestsByBlobRef(ctx, "0b5e0000", func(domain.Manifest) error {
			n++
			return nil
		})
		if err != nil {
			t.Fatalf("ManifestsByBlobRef: %v", err)
		}
		if n != 0 {
			t.Errorf("got %d manifests for unknown blob, want 0", n)
		}
	})
}

// inlineManifest builds an Inline blob manifest (payload inside the
// manifest, no blob_ref / blobs row). Local helper — manifestfx has no
// Inline builder, and only the scrub-manifest runs need one.
func inlineManifest(id string) domain.Manifest {
	m := manifestfx.Blob(id, "")
	m.BlobRefs = nil
	m.LayoutHeader.BlobStorage = domain.LayoutInline
	m.InlineBlob = []byte("inline-payload")
	return m
}
