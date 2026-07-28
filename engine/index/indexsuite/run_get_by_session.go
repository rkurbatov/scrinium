package indexsuite

import (
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- GetBySession ---

func runGetBySession(t *testing.T, f Factory) {
	t.Run("Hit", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		stage := []struct {
			id, ref, sess string
			fillChar      byte
		}{
			{"a1", "b10b00a1", "sess-1", 'a'},
			{"a2", "b10b00a2", "sess-1", 'b'},
			{"b1", "b10b00b1", "sess-2", 'c'},
		}
		for _, s := range stage {
			m := manifestfx.BlobWithHash(s.id, s.ref, manifestfx.SyntheticHash(s.fillChar), 1024)
			m.SessionID = domain.SessionID(s.sess)
			if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p/"+s.ref)); err != nil {
				t.Fatalf("seed %s: %v", s.id, err)
			}
		}

		ids, err := idx.GetBySession(ctx, "sess-1")
		if err != nil {
			t.Fatalf("GetBySession: %v", err)
		}
		if len(ids) != 2 {
			t.Fatalf("got %d, want 2", len(ids))
		}
		seen := make(map[domain.ArtifactID]bool)
		for _, id := range ids {
			seen[id] = true
		}
		if !seen["a1"] || !seen["a2"] {
			t.Errorf("missing expected ids: got %v", ids)
		}
	})

	t.Run("Miss", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		ids, err := idx.GetBySession(ctx, "0e0e0e0e")
		if err != nil {
			t.Fatalf("GetBySession: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("got %d, want 0", len(ids))
		}
	})
}
