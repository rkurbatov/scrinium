package sqlite

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/domain"
)

// Coverage for decision R6: the manifests_artifact index is
// deliberately non-UNIQUE (safe two-phase form migration), so (a) the
// resolve must be deterministic during the transit window — the
// freshest form (max csn) wins — and (b) duplicates outside a
// migration are surfaced by ListDuplicateHandles, the
// index.DuplicateHandleAuditor capability the Scrub Agent probes.

// setCSN stamps a manifest row's csn directly. Glass-box, like the
// other helpers here: the public API assigns csn itself, and this test
// needs two specific, ordered values.
func setCSN(t *testing.T, idx *Index, digest string, csn int64) {
	t.Helper()
	res, err := idx.db.ExecContext(context.Background(),
		`UPDATE manifests SET csn = ? WHERE manifest_digest = ?`, csn, hexKey(digest))
	if err != nil {
		t.Fatalf("setCSN: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("setCSN: %d rows affected, want 1", n)
	}
}

func TestListDuplicateHandles_CleanIndex(t *testing.T) {
	idx, _ := newDiskIndex(t)
	insertManifest(t, idx, domain.Manifest{ArtifactID: "art-a", CreatedAt: time.Now()})
	insertManifest(t, idx, domain.Manifest{ArtifactID: "art-b", CreatedAt: time.Now()})

	dups, err := idx.ListDuplicateHandles(context.Background())
	if err != nil {
		t.Fatalf("ListDuplicateHandles: %v", err)
	}
	if len(dups) != 0 {
		t.Errorf("clean index must report no duplicates, got %v", dups)
	}
}

func TestListDuplicateHandles_ReportsDuplicate(t *testing.T) {
	idx, _ := newDiskIndex(t)
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-dup", Digest: domain.ManifestDigest(rep("1")), CreatedAt: time.Now()})
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-dup", Digest: domain.ManifestDigest(rep("2")), CreatedAt: time.Now()})
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-solo", CreatedAt: time.Now()})

	dups, err := idx.ListDuplicateHandles(context.Background())
	if err != nil {
		t.Fatalf("ListDuplicateHandles: %v", err)
	}
	if len(dups) != 1 || dups[0] != "art-dup" {
		t.Errorf("got %v, want [art-dup]", dups)
	}
}

// Headless rows (empty artifact_id: pack containers, headless data
// artifacts) share the empty value by design and must never be reported
// as duplicates.
func TestListDuplicateHandles_IgnoresHeadless(t *testing.T) {
	idx, _ := newDiskIndex(t)
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "", Digest: domain.ManifestDigest(rep("3")), CreatedAt: time.Now()})
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "", Digest: domain.ManifestDigest(rep("4")), CreatedAt: time.Now()})

	dups, err := idx.ListDuplicateHandles(context.Background())
	if err != nil {
		t.Fatalf("ListDuplicateHandles: %v", err)
	}
	if len(dups) != 0 {
		t.Errorf("headless rows must not be reported, got %v", dups)
	}
}

// TestResolveManifestDigest_FreshestFormWins pins the R6 resolve rule:
// during a form-migration transit window (two rows, one handle) the row
// with the highest csn — the freshest form — resolves, deterministically.
func TestResolveManifestDigest_FreshestFormWins(t *testing.T) {
	idx, _ := newDiskIndex(t)
	oldDigest := rep("a")
	newDigest := rep("b")
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-move", Digest: domain.ManifestDigest(oldDigest), CreatedAt: time.Now()})
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-move", Digest: domain.ManifestDigest(newDigest), CreatedAt: time.Now()})
	setCSN(t, idx, oldDigest, 5)
	setCSN(t, idx, newDigest, 9)

	got, found, err := idx.ResolveManifestDigest(context.Background(), "art-move")
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if !found {
		t.Fatal("handle not found")
	}
	if string(got) != newDigest {
		t.Errorf("resolve returned %s, want the freshest form %s", got, newDigest)
	}
}

// rep builds a 64-char bare-hex digest from one hex character (ADR-93:
// digests carry no algorithm prefix, and the index stores them as raw bytes).
func rep(c string) string {
	out := ""
	for i := 0; i < 64; i++ {
		out += c
	}
	return out
}
