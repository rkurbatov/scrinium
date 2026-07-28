package indexsuite

import (
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- QueryByExtField ---

func runQueryByExtField(t *testing.T, f Factory) {
	t.Run("YieldsArtifactForProjectedField", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		registerFixture(t, ctx, idx)

		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatalf("IndexManifest: %v", err)
		}

		var got []domain.ArtifactID
		err := idx.QueryByExtField(ctx, fixtureName, fixtureExtField, fixtureExtValue, func(id domain.ArtifactID) error {
			got = append(got, id)
			return nil
		})
		if err != nil {
			t.Fatalf("QueryByExtField: %v", err)
		}
		if len(got) != 1 || got[0] != "art-1" {
			t.Errorf("QueryByExtField(%q, %q, %q): got %v, want [art-1]", fixtureName, fixtureExtField, fixtureExtValue, got)
		}
	})

	t.Run("NoMatchYieldsNothing", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		registerFixture(t, ctx, idx)

		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("p")); err != nil {
			t.Fatalf("IndexManifest: %v", err)
		}

		var got []domain.ArtifactID
		err := idx.QueryByExtField(ctx, fixtureName, fixtureExtField, "no-such-value", func(id domain.ArtifactID) error {
			got = append(got, id)
			return nil
		})
		if err != nil {
			t.Fatalf("QueryByExtField (no match): %v", err)
		}
		if len(got) != 0 {
			t.Errorf("QueryByExtField with non-matching value: got %v, want none", got)
		}
	})
}
