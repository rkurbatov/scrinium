package sqlite

import (
	"context"
	"testing"

	"scrinium.dev/testutil/manifestfx"
)

// TestCSN_IndexManifestStamps checks IndexManifest advances Token by one
// and stamps the issued csn onto the manifest row, in the write
// transaction (ADR-106).
func TestCSN_IndexManifestStamps(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	m := manifestfx.Blob("art-1", "b10b0001")
	if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	tok, err := readToken(ctx, idx.db)
	if err != nil {
		t.Fatalf("readToken: %v", err)
	}
	if tok != 1 {
		t.Errorf("Token after one IndexManifest = %d, want 1", tok)
	}

	var rowCSN uint64
	if err := idx.db.QueryRowContext(ctx,
		`SELECT csn FROM manifests WHERE manifest_digest = ?`, hexKey(m.Digest),
	).Scan(&rowCSN); err != nil {
		t.Fatalf("read manifests.csn: %v", err)
	}
	if rowCSN != tok {
		t.Errorf("manifests.csn = %d, want %d (the issued Token)", rowCSN, tok)
	}
}

// TestCSN_IndexManifestAdvancesToken checks each IndexManifest issues the
// next monotonic csn — two distinct artifacts move Token to 2.
func TestCSN_IndexManifestAdvancesToken(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"),
		manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest #1: %v", err)
	}
	if err := idx.IndexManifest(ctx, manifestfx.Blob("art-2", "b10b0002"),
		manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
		t.Fatalf("IndexManifest #2: %v", err)
	}

	tok, err := readToken(ctx, idx.db)
	if err != nil {
		t.Fatalf("readToken: %v", err)
	}
	if tok != 2 {
		t.Errorf("Token after two IndexManifest = %d, want 2", tok)
	}
}

// TestCSN_DeleteManifestStampsPrune checks DeleteManifest advances Token,
// records the prune watermark, and removes the row (ADR-106 D2-A: the
// deleted digest is no longer enumerable by csn — prune_csn drives Gapped).
func TestCSN_DeleteManifestStampsPrune(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	m := manifestfx.Blob("art-1", "b10b0001")
	if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}

	var csn, prune uint64
	if err := idx.db.QueryRowContext(ctx,
		`SELECT csn, prune_csn FROM index_seq WHERE id = 0`,
	).Scan(&csn, &prune); err != nil {
		t.Fatalf("read index_seq: %v", err)
	}
	// Index (csn 1) then Delete (csn 2): Token advances, prune marks the delete.
	if csn != 2 {
		t.Errorf("Token after Index+Delete = %d, want 2", csn)
	}
	if prune != 2 {
		t.Errorf("prune_csn after Delete = %d, want 2", prune)
	}

	var n int
	if err := idx.db.QueryRowContext(ctx,
		`SELECT count(*) FROM manifests WHERE manifest_digest = ?`, hexKey(m.Digest),
	).Scan(&n); err != nil {
		t.Fatalf("count manifests: %v", err)
	}
	if n != 0 {
		t.Errorf("manifest row after Delete = %d, want 0", n)
	}
}

// TestCSN_DeleteAbsentNoBump checks deleting a never-indexed digest is a
// no-op for the counter: DeleteManifest returns early on a missing row, so
// neither Token nor the prune watermark moves.
func TestCSN_DeleteAbsentNoBump(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	if err := idx.DeleteManifest(ctx, "deadbeef"); err != nil {
		t.Fatalf("DeleteManifest(absent): %v", err)
	}

	var csn, prune uint64
	if err := idx.db.QueryRowContext(ctx,
		`SELECT csn, prune_csn FROM index_seq WHERE id = 0`,
	).Scan(&csn, &prune); err != nil {
		t.Fatalf("read index_seq: %v", err)
	}
	if csn != 0 || prune != 0 {
		t.Errorf("counter moved on absent delete: csn=%d prune=%d, want 0, 0", csn, prune)
	}
}
