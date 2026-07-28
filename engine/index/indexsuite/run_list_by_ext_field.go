package indexsuite

import (
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- ListByExtField ---

func runListByExtField(t *testing.T, f Factory) {
	t.Run("YieldsManifestForProjectedField", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		registerFixture(t, ctx, idx)

		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatalf("IndexManifest: %v", err)
		}

		var got []domain.Manifest
		err := idx.ListByExtField(ctx, fixtureName, fixtureExtField, fixtureExtValue, func(m domain.Manifest) error {
			got = append(got, m)
			return nil
		})
		if err != nil {
			t.Fatalf("ListByExtField: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("ListByExtField: got %d manifests, want 1", len(got))
		}
		if got[0].ArtifactID != "art-1" {
			t.Errorf("ListByExtField: got ArtifactID %q, want art-1", got[0].ArtifactID)
		}
	})
}
