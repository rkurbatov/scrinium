package sqlite

import (
	"strings"
	"testing"

	"scrinium.dev/domain"
)

// TestResolveManifestDigest_Hit: after IndexManifest, the floating handle
// (artifact_id) resolves to the manifest's on-disk digest
// (manifest_digest). Exercises the full write→resolve round-trip through
// the public index API on an Inline manifest (no blob row needed).
func TestResolveManifestDigest_Hit(t *testing.T) {
	ctx := t.Context()
	idx := newMemoryIndex(t)

	handle := domain.ArtifactID(strings.Repeat("a", 64))
	digest := domain.ManifestDigest(strings.Repeat("b", 64))
	m := domain.Manifest{
		ArtifactID:   handle,
		Digest:       digest,
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutInline},
	}
	if err := idx.IndexManifest(ctx, m, domain.PhysicalAddress{Path: "manifests/x"}); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	got, found, err := idx.ResolveManifestDigest(ctx, handle)
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if !found {
		t.Fatal("found=false, want true for an indexed handle")
	}
	if got != digest {
		t.Errorf("digest: got %q, want %q", got, digest)
	}
}

// TestResolveManifestDigest_Miss: an unknown handle resolves to
// ("", false, nil) — absence is reported through the boolean, not as an
// error.
func TestResolveManifestDigest_Miss(t *testing.T) {
	ctx := t.Context()
	idx := newMemoryIndex(t)

	got, found, err := idx.ResolveManifestDigest(ctx, domain.ArtifactID(strings.Repeat("c", 64)))
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if found {
		t.Errorf("found=true for an unknown handle (got digest %q)", got)
	}
	if got != "" {
		t.Errorf("digest: got %q, want empty", got)
	}
}
