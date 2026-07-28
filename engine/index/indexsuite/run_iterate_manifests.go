package indexsuite

import (
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- IterateManifests ---
//
// IterateManifests is namespace-agnostic (ADR-99): it returns every user
// manifest the index holds, full stop. Headless pack containers and system
// artifacts never appear (covered by IndexManifest's own conformance).

func runIterateManifests(t *testing.T, f Factory) {
	t.Run("ReturnsAllUserManifests", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		// Distinct (contentHash, blobRef) per artifact keeps the
		// (content_hash, original_size) UNIQUE constraint untouched.
		stage := []struct {
			id, ref  string
			fillChar byte
		}{
			{"a1", "b10b00a1", 'a'},
			{"a2", "b10b00a2", 'b'},
			{"b1", "b10b00b1", 'c'},
		}
		for _, s := range stage {
			m := manifestfx.BlobWithHash(s.id, s.ref, manifestfx.SyntheticHash(s.fillChar), 1024)
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/"+s.ref)); err != nil {
				t.Fatalf("seed %s: %v", s.id, err)
			}
		}

		got := collectAll(t, idx)
		if len(got) != len(stage) {
			t.Fatalf("IterateManifests returned %d manifests, want %d", len(got), len(stage))
		}
		seen := make(map[domain.ArtifactID]bool, len(got))
		for _, m := range got {
			seen[m.ArtifactID] = true
		}
		for _, s := range stage {
			if !seen[domain.ArtifactID(s.id)] {
				t.Errorf("IterateManifests missing artifact %q", s.id)
			}
		}
	})

	t.Run("EmptyIndex", func(t *testing.T) {
		idx := f.New(t)
		if got := collectAll(t, idx); len(got) != 0 {
			t.Errorf("IterateManifests on empty index = %d manifests, want 0", len(got))
		}
	})
}
