package indexsuite

import (
	"context"
	"testing"

	"scrinium.dev/engine/index"
	"scrinium.dev/testutil/manifestfx"
)

// --- DeleteOrphanBlob ---

func runDeleteOrphanBlob(t *testing.T, f Factory) {
	// An orphan is reached through the public API: IndexManifest
	// (ref_count=1) then DeleteManifest (ref_count=0). A live blob is
	// just IndexManifest with no delete.

	seedOrphan := func(t *testing.T, idx index.StoreIndex, id, ref string) {
		t.Helper()
		ctx := context.Background()
		m := manifestfx.BlobWithHash(id, ref, manifestfx.SyntheticHash('a'), 1024)
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/"+ref)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
		if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
	}

	t.Run("RemovesOrphanRow", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		seedOrphan(t, idx, "orph-1", "b10b0f01")

		removed, err := idx.DeleteOrphanBlob(ctx, "b10b0f01")
		if err != nil {
			t.Fatalf("DeleteOrphanBlob: %v", err)
		}
		if !removed {
			t.Fatal("removed = false, want true for an orphan row")
		}
		// Gone from the orphan list now.
		var got []string
		if err := idx.ListOrphanBlobs(ctx, func(ref string) error {
			got = append(got, ref)
			return nil
		}); err != nil {
			t.Fatalf("ListOrphanBlobs: %v", err)
		}
		for _, r := range got {
			if r == "b10b0f01" {
				t.Errorf("blob-o1 still present after DeleteOrphanBlob")
			}
		}
	})

	t.Run("KeepsRevivedBlob", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		seedOrphan(t, idx, "orph-1", "b10b5ded")
		// Revive: a new artifact references the same blob → ref_count
		// back to 1. DeleteOrphanBlob must NOT remove it.
		reviver := manifestfx.BlobWithHash("reviver", "b10b5ded", manifestfx.SyntheticHash('a'), 1024)
		if err := idx.IndexManifest(ctx, reviver, manifestfx.PhysAddr("p/blob-shared")); err != nil {
			t.Fatalf("revive: %v", err)
		}

		removed, err := idx.DeleteOrphanBlob(ctx, "b10b5ded")
		if err != nil {
			t.Fatalf("DeleteOrphanBlob: %v", err)
		}
		if removed {
			t.Fatal("removed = true, want false: blob was revived (ref_count > 0)")
		}
		// Still resolvable (row intact).
		if _, err := idx.Resolve(ctx, "b10b5ded"); err != nil {
			t.Errorf("revived blob no longer resolvable: %v", err)
		}
	})

	t.Run("MissingBlobIsNoOp", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		removed, err := idx.DeleteOrphanBlob(ctx, "0e0e0e0e")
		if err != nil {
			t.Errorf("missing blob must be a no-op, got %v", err)
		}
		if removed {
			t.Error("removed = true for a nonexistent blob")
		}
	})

	t.Run("LiveBlobNotRemoved", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		// Live: indexed, never deleted → ref_count = 1.
		m := manifestfx.BlobWithHash("live-1", "b10b01fe", manifestfx.SyntheticHash('a'), 1024)
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/blob-live")); err != nil {
			t.Fatalf("seed live: %v", err)
		}
		removed, err := idx.DeleteOrphanBlob(ctx, "b10b01fe")
		if err != nil {
			t.Fatalf("DeleteOrphanBlob: %v", err)
		}
		if removed {
			t.Error("removed a live blob (ref_count > 0)")
		}
	})
}
