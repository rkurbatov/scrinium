package indexsuite

import (
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- Manifest existence (via ResolveManifestDigest found-bool) ---

func runManifestExists(t *testing.T, f Factory) {
	t.Run("Fresh_ReturnsFalse", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		_, exists, err := idx.ResolveManifestDigest(ctx, domain.ArtifactID("sha256-"+strings.Repeat("a", 64)))
		if err != nil {
			t.Fatalf("ResolveManifestDigest: %v", err)
		}
		if exists {
			t.Error("fresh index must report manifest absent")
		}
	})

	t.Run("AfterIndex_ReturnsTrue", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		m := manifestfx.Blob("art-1", "b10b0001")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		_, exists, err := idx.ResolveManifestDigest(ctx, "art-1")
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Error("manifest must be present after IndexManifest")
		}
	})

	t.Run("AfterDelete_ReturnsFalse", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		m := manifestfx.Blob("art-2", "b10b0002")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
			t.Fatal(err)
		}
		_, exists, err := idx.ResolveManifestDigest(ctx, "art-2")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("manifest must be absent after DeleteManifest")
		}
	})

	t.Run("DistinguishesIDs", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		m := manifestfx.Blob("art-known", "b10b0c0a")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		_, known, err := idx.ResolveManifestDigest(ctx, "art-known")
		if err != nil {
			t.Fatal(err)
		}
		if !known {
			t.Error("known manifest reported absent, want present")
		}
		_, unknown, err := idx.ResolveManifestDigest(ctx, "art-unknown")
		if err != nil {
			t.Fatal(err)
		}
		if unknown {
			t.Error("unknown manifest reported present, want absent")
		}
	})

	t.Run("NotConfusedByBlobRef", func(t *testing.T) {
		ctx := t.Context()
		// Existence must look in the manifests-table only,
		// not the blobs-table. Probe with a blob_ref-shaped
		// string that is NOT an ArtifactID.
		idx := f.New(t)
		m := manifestfx.Blob("art-real", "b10b4ea1")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		_, exists, err := idx.ResolveManifestDigest(ctx, "b10b4ea1")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("existence must not match blob refs")
		}
	})
}
