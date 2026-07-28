package indexsuite

import (
	"testing"

	"scrinium.dev/testutil/manifestfx"
)

// --- DeleteManifest ---

func runDeleteManifest(t *testing.T, f Factory) {
	t.Run("Blob_DropsRefCount", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		m := manifestfx.Blob("art-1", "b10b0001")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
			t.Fatalf("DeleteManifest: %v", err)
		}
		_, exists, err := idx.ResolveManifestDigest(ctx, "art-1")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("manifest still visible after DeleteManifest")
		}
		// Blob row remains as orphan with ref_count = 0 — the GC
		// state, not "missing".
		n, err := idx.GetRefCount(ctx, "b10b0001")
		if err != nil {
			t.Fatalf("blob row gone (got %v); orphans must persist for GC", err)
		}
		if n != 0 {
			t.Errorf("ref_count: got %d, want 0", n)
		}
	})

	t.Run("Idempotent", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.DeleteManifest(ctx, "0d0d0d0d"); err != nil {
			t.Errorf("delete of unknown digest must be no-op, got %v", err)
		}
	})
}
