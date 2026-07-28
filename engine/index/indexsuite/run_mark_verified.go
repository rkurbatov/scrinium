package indexsuite

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/testutil/manifestfx"
)

// --- MarkVerified ---

func runMarkVerified(t *testing.T, f Factory) {
	t.Run("UpdatesObservableThroughListUnverified", func(t *testing.T) {
		ctx := t.Context()
		// MarkVerified updates last_verified_at on a blob.
		// Without poking the storage, we observe it through
		// ListUnverifiedBlobs: a blob freshly indexed has NULL
		// last_verified_at and is reported by every
		// ListUnverifiedBlobs call; after MarkVerified with a recent
		// timestamp, the same call with `before` set to a moment
		// before the verification stops reporting it.
		idx := f.New(t)
		m := manifestfx.Blob("art-1", "b10b0001")
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}

		// Truncate to seconds (RFC 3339 storage precision).
		verifiedAt := time.Now().UTC().Truncate(time.Second)
		if err := idx.MarkVerified(ctx, "b10b0001", verifiedAt); err != nil {
			t.Fatalf("MarkVerified: %v", err)
		}

		// `before` strictly older than verifiedAt — blob must
		// NOT appear (it has been verified more recently).
		var seen []string
		err := idx.ListUnverifiedBlobs(context.Background(), verifiedAt.Add(-time.Minute), func(ref string) error {
			seen = append(seen, ref)
			return nil
		})
		if err != nil {
			t.Fatalf("ListUnverifiedBlobs: %v", err)
		}
		for _, r := range seen {
			if r == "b10b0001" {
				t.Errorf("blob-1 still reported as unverified before %v", verifiedAt.Add(-time.Minute))
			}
		}
	})

	t.Run("MissingBlobIsNoOp", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.MarkVerified(ctx, "0e0e0e0e", time.Now()); err != nil {
			t.Errorf("missing blob must be no-op, got %v", err)
		}
	})
}
