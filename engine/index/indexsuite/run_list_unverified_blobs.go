package indexsuite

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/testutil/manifestfx"
)

// --- ListUnverifiedBlobs ---

func runListUnverifiedBlobs(t *testing.T, f Factory) {
	// IndexManifest creates blobs with no verification timestamp;
	// MarkVerified sets it. The iterator surfaces blobs whose
	// last verification (or absence thereof) places them before
	// the cutoff — never-verified rows always qualify, recently
	// verified rows are skipped.

	t.Run("CutoffBoundary", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		now := time.Now().UTC().Truncate(time.Second)

		stage := []struct {
			id, ref      string
			fillChar     byte
			verifiedAgo  time.Duration
			everVerified bool
		}{
			{"never", "b10b000e", 'a', 0, false},
			{"stale", "b10b0055", 'b', 10 * time.Minute, true},
			{"fresh", "b10b000f", 'c', time.Minute, true},
		}
		for _, s := range stage {
			m := manifestfx.BlobWithHash(s.id, s.ref, manifestfx.SyntheticHash(s.fillChar), 1024)
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/"+s.ref)); err != nil {
				t.Fatalf("seed %s: %v", s.id, err)
			}
			if s.everVerified {
				if err := idx.MarkVerified(ctx, s.ref, now.Add(-s.verifiedAgo)); err != nil {
					t.Fatalf("MarkVerified %s: %v", s.ref, err)
				}
			}
		}

		cutoff := now.Add(-5 * time.Minute)
		var got []string
		err := idx.ListUnverifiedBlobs(context.Background(), cutoff, func(ref string) error {
			got = append(got, ref)
			return nil
		})
		if err != nil {
			t.Fatalf("ListUnverifiedBlobs: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d, want 2 (never+stale)", len(got))
		}
		seen := make(map[string]bool)
		for _, ref := range got {
			seen[ref] = true
		}
		if !seen["b10b000e"] {
			t.Error("expected never-verified blob in result")
		}
		if !seen["b10b0055"] {
			t.Error("expected stale blob in result")
		}
		if seen["b10b000f"] {
			t.Error("fresh blob leaked through cutoff")
		}
	})

	t.Run("OldestFirst", func(t *testing.T) {
		ctx := t.Context()
		// Sorting order: oldest verification first. NEVER-verified
		// rows are also reported, but the relative position of
		// NEVER vs verified rows is implementation-defined when
		// last_verified_at is treated as a NULL/sentinel value;
		// this test pins down the pure-time ordering between
		// rows that have a verification timestamp.
		idx := f.New(t)
		now := time.Now().UTC().Truncate(time.Second)

		stage := []struct {
			id, ref     string
			fillChar    byte
			verifiedAgo time.Duration
		}{
			{"older", "b10b000f", 'a', 3 * time.Hour},
			{"middle", "b10b00dd", 'b', 2 * time.Hour},
			{"newer", "b10b000e", 'c', time.Hour},
		}
		for _, s := range stage {
			m := manifestfx.BlobWithHash(s.id, s.ref, manifestfx.SyntheticHash(s.fillChar), 1024)
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/"+s.ref)); err != nil {
				t.Fatalf("seed %s: %v", s.id, err)
			}
			if err := idx.MarkVerified(ctx, s.ref, now.Add(-s.verifiedAgo)); err != nil {
				t.Fatalf("MarkVerified %s: %v", s.ref, err)
			}
		}

		cutoff := now
		var got []string
		err := idx.ListUnverifiedBlobs(context.Background(), cutoff, func(ref string) error {
			got = append(got, ref)
			return nil
		})
		if err != nil {
			t.Fatalf("ListUnverifiedBlobs: %v", err)
		}
		want := []string{"b10b000f", "b10b00dd", "b10b000e"}
		if len(got) != len(want) {
			t.Fatalf("got %d, want %d", len(got), len(want))
		}
		for i, ref := range got {
			if ref != want[i] {
				t.Errorf("position %d: got %q, want %q", i, ref, want[i])
			}
		}
	})
}
