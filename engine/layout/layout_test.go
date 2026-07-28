package layout_test

import (
	"strings"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/layout"
)

// A well-formed 64-hex-char sha256 ref used across the table.
func ref(hexHead string) string {
	pad := 64 - len(hexHead)
	if pad < 0 {
		pad = 0
	}
	return hexHead + strings.Repeat("0", pad)
}

// --- BlobPath: Sharded ---

func TestBlobPath_ShardedBlob(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeRegular, ref("aabbccdd"))
	if err != nil {
		t.Fatalf("BlobPath: %v", err)
	}
	want := "blobs/aa/bb/" + ref("aabbccdd")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBlobPath_ShardedChunkRoot(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeChunk, ref("deadbeef"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "chunks/de/ad/") {
		t.Errorf("expected chunks/de/ad/ prefix, got %q", got)
	}
}

func TestBlobPath_ShardedPackRoot(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypePack, ref("12345678"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "packs/12/34/") {
		t.Errorf("expected packs/12/34/ prefix, got %q", got)
	}
}

// --- BlobPath: Flat ---

func TestBlobPath_Flat(t *testing.T) {
	r := ref("aabbccdd")
	got, err := layout.BlobPath(config.PathTopologyFlat, domain.BlobTypeRegular, r)
	if err != nil {
		t.Fatal(err)
	}
	if got != "blobs/"+r {
		t.Errorf("got %q, want %q", got, "blobs/"+r)
	}
}

// --- Defaults ---

func TestBlobPath_EmptyTopologyDefaultsToSharded(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopology(""), domain.BlobTypeRegular, ref("aabbccdd"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "blobs/aa/bb/") {
		t.Errorf("empty topology should fall back to Sharded, got %q", got)
	}
}

func TestBlobPath_EmptyBlobTypeMeansRegular(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopologySharded, domain.BlobType(""), ref("aabbccdd"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "blobs/") {
		t.Errorf("empty blob type should default to blobs/, got %q", got)
	}
}

// --- Case folding (part of the on-disk format contract) ---

func TestBlobPath_FoldsCaseInShards(t *testing.T) {
	got, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeRegular, "AABBCCDD"+strings.Repeat("0", 56))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "blobs/aa/bb/") {
		t.Errorf("expected lowercase shards, got %q", got)
	}
}

// --- BlobPath: error paths ---

func TestBlobPath_RejectsEmptyRef(t *testing.T) {
	if _, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeRegular, ""); err == nil {
		t.Fatal("expected error on empty ref")
	}
}

func TestBlobPath_RejectsTooShortHex(t *testing.T) {
	if _, err := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeRegular, "ab"); err == nil {
		t.Fatal("expected error on too-short hex")
	}
}

func TestBlobPath_RejectsUnknownTopology(t *testing.T) {
	if _, err := layout.BlobPath(config.PathTopology("Quantum"), domain.BlobTypeRegular, ref("aabbccdd")); err == nil {
		t.Fatal("expected error on unknown topology")
	}
}

func TestBlobPath_RejectsUnknownBlobType(t *testing.T) {
	if _, err := layout.BlobPath(config.PathTopologySharded, domain.BlobType("Frob"), ref("aabbccdd")); err == nil {
		t.Fatal("expected error on unknown blob type")
	}
}

// --- ManifestPath ---

func TestManifestPath_Sharded(t *testing.T) {
	digest := domain.ManifestDigest(ref("cafe1234"))
	got, err := layout.ManifestPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	want := "manifests/ca/fe/" + string(digest)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestManifestPath_RejectsEmpty(t *testing.T) {
	if _, err := layout.ManifestPath(""); err == nil {
		t.Fatal("expected error on empty digest")
	}
}

func TestManifestPath_RejectsShort(t *testing.T) {
	if _, err := layout.ManifestPath(domain.ManifestDigest("ab")); err == nil {
		t.Fatal("expected error on too-short digest")
	}
}

// --- RefFromBlobPath: round-trips and shapes ---

func TestRefFromBlobPath_ShardedRoundTrip(t *testing.T) {
	r := ref("aabbccdd")
	p, _ := layout.BlobPath(config.PathTopologySharded, domain.BlobTypeRegular, r)
	got, err := layout.RefFromBlobPath(p)
	if err != nil {
		t.Fatal(err)
	}
	if got != r {
		t.Errorf("round-trip: got %q, want %q", got, r)
	}
}

func TestRefFromBlobPath_FlatRoundTrip(t *testing.T) {
	r := ref("aabbccdd")
	p, _ := layout.BlobPath(config.PathTopologyFlat, domain.BlobTypeRegular, r)
	got, err := layout.RefFromBlobPath(p)
	if err != nil {
		t.Fatal(err)
	}
	if got != r {
		t.Errorf("round-trip: got %q, want %q", got, r)
	}
}

func TestRefFromBlobPath_RejectsEmpty(t *testing.T) {
	if _, err := layout.RefFromBlobPath(""); err == nil {
		t.Fatal("expected error on empty path")
	}
}

func TestRefFromBlobPath_RejectsBadShape(t *testing.T) {
	if _, err := layout.RefFromBlobPath("blobs/aa/bb/notaref"); err == nil {
		t.Fatal("expected error on segment missing algo prefix")
	}
}

func TestRefFromBlobPath_RejectsNonHex(t *testing.T) {
	if _, err := layout.RefFromBlobPath("blobs/aa/bb/sha256-zzzz"); err == nil {
		t.Fatal("expected error on non-hex tail")
	}
}

// --- DigestFromManifestPath: round-trip ---

func TestDigestFromManifestPath_RoundTrip(t *testing.T) {
	digest := domain.ManifestDigest(ref("cafe1234"))
	p, _ := layout.ManifestPath(digest)
	got, err := layout.DigestFromManifestPath(p)
	if err != nil {
		t.Fatal(err)
	}
	if got != digest {
		t.Errorf("round-trip: got %q, want %q", got, digest)
	}
}

func TestDigestFromManifestPath_RejectsBad(t *testing.T) {
	if _, err := layout.DigestFromManifestPath("manifests/aa/bb/garbage"); err == nil {
		t.Fatal("expected error on bad manifest segment")
	}
}

func TestRefFromBlobPath_FlatPathSingleSegment(t *testing.T) {
	ref := "1234" + strings.Repeat("a", 60)
	got, err := layout.RefFromBlobPath(ref)
	if err != nil {
		t.Fatal(err)
	}
	if got != ref {
		t.Errorf("got %q, want %q", got, ref)
	}
}

func TestRefFromBlobPath_RoundTripsBlobPath(t *testing.T) {
	ref := "cafebabe" + strings.Repeat("f", 56)

	for _, topo := range []config.PathTopology{config.PathTopologySharded, config.PathTopologyFlat} {
		path, err := layout.BlobPath(topo, domain.BlobTypeRegular, ref)
		if err != nil {
			t.Fatalf("BlobPath(%s): %v", topo, err)
		}
		got, err := layout.RefFromBlobPath(path)
		if err != nil {
			t.Fatalf("RefFromBlobPath(%q): %v", path, err)
		}
		if got != ref {
			t.Errorf("topology %s: got %q, want %q", topo, got, ref)
		}
	}
}

// RefFromBlobPath's other error tests cover an empty path, a no-algo-prefix
// tail, and a non-hex tail; this is the remaining gap — a tail that is valid
// hex but shorter than the four-char minimum. (The original input was
// "abc00000", a NON-hex tail, so it duplicated the non-hex case and missed
// the length guard it claimed to test; corrected to a short hex tail.)
func TestRefFromBlobPath_RejectsTooShortHex(t *testing.T) {
	if _, err := layout.RefFromBlobPath("blobs/aa/bb/abc"); err == nil {
		t.Fatal("expected error on too-short hex tail")
	}
}

// DigestFromManifestPath had a round-trip and a malformed-segment test, but no
// explicit empty-path case.
func TestDigestFromManifestPath_RejectsEmpty(t *testing.T) {
	if _, err := layout.DigestFromManifestPath(""); err == nil {
		t.Fatal("expected error on empty manifest path")
	}
}
