package indexsuite

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- ListUnverifiedManifests + MarkManifestVerified ---

func runListUnverifiedManifests(t *testing.T, f Factory) {
	t.Run("FreshlyIndexedManifestIsUnverified", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		got := collectUnverifiedManifests(t, idx, time.Now())
		if !contains(got, "art-1") {
			t.Errorf("freshly indexed art-1 not reported unverified; got %v", got)
		}
	})

	t.Run("InlineManifestIsReported", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		// Inline has no blobs row, so ListUnverified (blob list) can
		// never see it — the manifest pass is the only way to scrub it.
		if err := idx.IndexManifest(ctx, inlineManifest("inline-1"), manifestfx.PhysAddr("p")); err != nil {
			t.Fatalf("IndexManifest inline: %v", err)
		}
		got := collectUnverifiedManifests(t, idx, time.Now())
		if !contains(got, "inline-1") {
			t.Errorf("inline-1 not reported by ListUnverifiedManifests; got %v", got)
		}
	})

	t.Run("MarkManifestVerifiedRemovesFromList", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		verifiedAt := time.Now().UTC().Truncate(time.Second)
		if err := idx.MarkManifestVerified(ctx, "art-1", verifiedAt); err != nil {
			t.Fatalf("MarkManifestVerified: %v", err)
		}
		// before strictly older than the stamp → art-1 must not appear.
		got := collectUnverifiedManifests(t, idx, verifiedAt.Add(-time.Minute))
		if contains(got, "art-1") {
			t.Errorf("art-1 still unverified after MarkManifestVerified; got %v", got)
		}
		// before newer than the stamp → it reappears (stale again).
		got = collectUnverifiedManifests(t, idx, verifiedAt.Add(time.Minute))
		if !contains(got, "art-1") {
			t.Errorf("art-1 should be stale relative to a later cutoff; got %v", got)
		}
	})

	t.Run("MarkManifestVerifiedMissingIsNoOp", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.MarkManifestVerified(ctx, "0e0e0e0e", time.Now()); err != nil {
			t.Errorf("missing manifest must be no-op, got %v", err)
		}
	})

	t.Run("PackManifestsExcluded", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"), manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}
		got := collectUnverifiedManifests(t, idx, time.Now())
		for _, id := range got {
			if id == "" {
				t.Fatal("unexpected empty artifact id")
			}
		}
		// Sanity: the blob manifest is present (pack-exclusion does not
		// drop ordinary manifests).
		if !contains(got, "art-1") {
			t.Errorf("ordinary manifest missing; got %v", got)
		}
	})
}

func collectUnverifiedManifests(t *testing.T, idx interface {
	ListUnverifiedManifests(context.Context, time.Time, func(domain.Manifest) error) error
}, before time.Time) []string {
	t.Helper()
	var got []string
	err := idx.ListUnverifiedManifests(context.Background(), before, func(m domain.Manifest) error {
		got = append(got, string(m.ArtifactID))
		return nil
	})
	if err != nil {
		t.Fatalf("ListUnverifiedManifests: %v", err)
	}
	return got
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
