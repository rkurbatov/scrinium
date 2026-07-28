package sqlite

// The one invariant that keeps the binary key representation honest: every
// key column holds a BLOB, never text.
//
// It matters because a missed conversion is silent. A row written with a hex
// string still inserts, still reads back as a string, and simply never
// matches a lookup that binds bytes — a mismatch that surfaces later as
// "the artifact is gone" rather than as an error. SQLite can be asked
// directly what a value's storage class is, so the check is exact rather
// than a guess.

import (
	"context"
	"database/sql"
	"testing"

	"scrinium.dev/domain"
)

// keyColumns are the columns that must hold raw bytes. artifact_id and
// handle_ref are deliberately absent: a handle is "<algo>-<hex>", so it is
// not bare hex and stays text.
var keyColumns = []struct{ table, column string }{
	{"manifests", "manifest_digest"},
	{"manifests", "blob_ref"},
	{"blobs", "blob_ref"},
	{"blobs", "content_hash"},
	{"manifest_blobs", "manifest_digest"},
	{"manifest_blobs", "blob_ref"},
	{"proj_ext", "manifest_digest"},
}

func TestKeyColumnsAreStoredAsBlobs(t *testing.T) {
	ctx := context.Background()
	idx := newMemoryIndex(t)

	// A normal write, through the same path the engine uses: one Target
	// manifest with a blob. The projection row is staged separately below,
	// since it is written by an index extension rather than by the core.
	m := domain.Manifest{
		Digest:       domain.ManifestDigest("aabbccdd11223344"),
		ArtifactID:   domain.ArtifactID("sha256-0011223344556677"),
		ContentHash:  domain.ContentHash("99887766554433221100"),
		BlobRefs:     []domain.BlobRef{domain.BlobRef("ffeeddccbbaa9988")},
		OriginalSize: 42,
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
	}
	if err := idx.IndexManifest(ctx, m, domain.PhysicalAddress{Path: "blobs/ff/ee/ffeeddccbbaa9988"}); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	// One projection row, written through the same helper the core uses for
	// an extension's fields.
	if err := idx.inTx(ctx, func(tx *sql.Tx) error {
		return upsertProjExt(ctx, tx, hexKey(m.Digest), "fixture", "field", "value")
	}); err != nil {
		t.Fatalf("stage projection row: %v", err)
	}

	for _, kc := range keyColumns {
		t.Run(kc.table+"."+kc.column, func(t *testing.T) {
			var kind string
			// typeof() reports the storage class of the value actually
			// stored, which is what a missed conversion gets wrong.
			q := "SELECT typeof(" + kc.column + ") FROM " + kc.table +
				" WHERE " + kc.column + " IS NOT NULL LIMIT 1"
			if err := idx.db.QueryRowContext(ctx, q).Scan(&kind); err != nil {
				t.Fatalf("no row to inspect in %s (%v) — the fixture must exercise this table", kc.table, err)
			}
			if kind != "blob" {
				t.Fatalf("%s.%s stored as %q, want \"blob\": a bind site still passes the hex string",
					kc.table, kc.column, kind)
			}
		})
	}
}

// TestKeyRoundTrip: what goes in as hex comes back as the same hex. Without
// this, "stored as blob" could be satisfied by storing the wrong bytes.
func TestKeyRoundTrip(t *testing.T) {
	ctx := context.Background()
	idx := newMemoryIndex(t)

	const digest = "0123456789abcdef"
	const ref = "fedcba9876543210"
	m := domain.Manifest{
		Digest:       domain.ManifestDigest(digest),
		ArtifactID:   domain.ArtifactID("sha256-abcabcabcabcabca"),
		ContentHash:  domain.ContentHash("1111222233334444"),
		BlobRefs:     []domain.BlobRef{domain.BlobRef(ref)},
		OriginalSize: 7,
		LayoutHeader: domain.LayoutHeader{BlobStorage: domain.LayoutTarget},
	}
	if err := idx.IndexManifest(ctx, m, domain.PhysicalAddress{Path: "blobs/fe/dc/" + ref}); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	got, ok, err := idx.ResolveManifestDigest(ctx, m.ArtifactID)
	if err != nil || !ok {
		t.Fatalf("ResolveManifestDigest: %v ok=%v", err, ok)
	}
	if string(got) != digest {
		t.Errorf("digest round-trip: got %q, want %q", got, digest)
	}

	addr, err := idx.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve by blob ref: %v", err)
	}
	if addr.Path == "" {
		t.Error("Resolve returned an empty address: the ref did not match what was stored")
	}

	exists, err := idx.ManifestExistsByDigest(ctx, m.Digest)
	if err != nil || !exists {
		t.Fatalf("ManifestExistsByDigest: %v exists=%v", err, exists)
	}
}
