package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
)

// MarkVerified and MarkVerified-related listing
// behaviour live in the conformance suite at
// internal/testutil/indexsuite. This file is for sqlite-specific
// behaviour: WriteCheckpoint (the optional index.CheckpointWriter
// capability, which sqlite implements via VACUUM INTO — it needs an
// on-disk source and so does not map to in-memory backends, and other
// backends such as Postgres need not implement it at all).

// --- WriteCheckpoint ---

func TestWriteCheckpoint_CreatesCheckpoint(t *testing.T) {
	idx, _ := newDiskIndex(t)
	// Seed some data so the checkpoint is a meaningful copy.
	insertBlob(t, idx, "b10b0001", strings.Repeat("a", 64), 1024,
		domain.PhysicalAddress{Path: "p"}, 1)
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-1",
		BlobRefs:   []domain.BlobRef{"b10b0001"}, CreatedAt: time.Now(),
	})

	dest := filepath.Join(t.TempDir(), "snap.db")
	if err := idx.WriteCheckpoint(context.Background(), dest); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// File exists.
	st, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("stat checkpoint: %v", err)
	}
	if st.Size() == 0 {
		t.Error("checkpoint file is empty")
	}

	// Checkpoint is a self-contained, openable database with the
	// same data.
	snap, err := NewStore(context.Background(), dest)
	if err != nil {
		t.Fatalf("NewStore checkpoint: %v", err)
	}

	if got := countRows(t, snap, "blobs"); got != 1 {
		t.Errorf("checkpoint blobs: got %d, want 1", got)
	}
	if got := countRows(t, snap, "manifests"); got != 1 {
		t.Errorf("checkpoint manifests: got %d, want 1", got)
	}
}

func TestWriteCheckpoint_RejectsExistingFile(t *testing.T) {
	idx, _ := newDiskIndex(t)
	dest := filepath.Join(t.TempDir(), "exists.db")
	if err := os.WriteFile(dest, []byte("placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := idx.WriteCheckpoint(context.Background(), dest)
	if err == nil {
		t.Fatal("expected error on existing destination")
	}
}

func TestWriteCheckpoint_RejectsMemoryDest(t *testing.T) {
	idx := newMemoryIndex(t)
	err := idx.WriteCheckpoint(context.Background(), ":memory:")
	if err == nil {
		t.Fatal("expected error on :memory: destination")
	}
}

func TestWriteCheckpoint_RejectsEmptyPath(t *testing.T) {
	idx := newMemoryIndex(t)
	err := idx.WriteCheckpoint(context.Background(), "")
	if err == nil {
		t.Fatal("expected error on empty path")
	}
}

func TestWriteCheckpoint_CreatesParentDir(t *testing.T) {
	idx, _ := newDiskIndex(t)
	dest := filepath.Join(t.TempDir(), "deep", "nested", "snap.db")
	if err := idx.WriteCheckpoint(context.Background(), dest); err != nil {
		t.Fatalf("WriteCheckpoint with nested parent: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("checkpoint not created: %v", err)
	}
}

// --- RestoreCheckpoint ---

func TestRestoreCheckpoint_RoundTrip(t *testing.T) {
	// Seed a source index, checkpoint it, restore into a fresh target.
	src, _ := newDiskIndex(t)
	insertBlob(t, src, "b10b0001", strings.Repeat("a", 64), 1024,
		domain.PhysicalAddress{Path: "p"}, 1)
	insertManifest(t, src, domain.Manifest{
		ArtifactID: "art-1",
		BlobRefs:   []domain.BlobRef{"b10b0001"}, CreatedAt: time.Now(),
	})

	cp := filepath.Join(t.TempDir(), "cp.db")
	if err := src.WriteCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	dst, _ := newDiskIndex(t)
	if err := dst.RestoreCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("RestoreCheckpoint: %v", err)
	}
	if got := countRows(t, dst, "blobs"); got != 1 {
		t.Errorf("restored blobs: got %d, want 1", got)
	}
	if got := countRows(t, dst, "manifests"); got != 1 {
		t.Errorf("restored manifests: got %d, want 1", got)
	}

	// The pinned connection must return to the pool clean: a follow-up query
	// on dst must not see a lingering "ckpt" attachment.
	if got := countRows(t, dst, "manifests"); got != 1 {
		t.Errorf("post-restore query: got %d, want 1", got)
	}
}

func TestRestoreCheckpoint_Idempotent(t *testing.T) {
	src, _ := newDiskIndex(t)
	insertBlob(t, src, "b10b0001", strings.Repeat("b", 64), 512,
		domain.PhysicalAddress{Path: "p"}, 1)
	cp := filepath.Join(t.TempDir(), "cp.db")
	if err := src.WriteCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	dst, _ := newDiskIndex(t)
	for i := 0; i < 2; i++ {
		if err := dst.RestoreCheckpoint(context.Background(), cp); err != nil {
			t.Fatalf("RestoreCheckpoint pass %d: %v", i, err)
		}
	}
	if got := countRows(t, dst, "blobs"); got != 1 {
		t.Errorf("after double restore: got %d blobs, want 1", got)
	}
}

func TestRestoreCheckpoint_RejectsMissingSource(t *testing.T) {
	dst, _ := newDiskIndex(t)
	err := dst.RestoreCheckpoint(context.Background(), filepath.Join(t.TempDir(), "nope.db"))
	if err == nil {
		t.Fatal("expected error for missing source")
	}
}

func TestRestoreCheckpoint_RejectsEmptyAndMemory(t *testing.T) {
	dst, _ := newDiskIndex(t)
	if err := dst.RestoreCheckpoint(context.Background(), ""); err == nil {
		t.Error("expected error for empty srcPath")
	}
	if err := dst.RestoreCheckpoint(context.Background(), ":memory:"); err == nil {
		t.Error("expected error for :memory: source")
	}
}

// --- Compile-time interface conformance ---

func TestIndex_ImplementsStoreIndex(t *testing.T) {
	// The compile-time check var _ index.StoreIndex = (*Index)(nil)
	// in sqlite.go is the real guarantee; this test just confirms
	// it at runtime so a regression shows up in test output, not
	// just a build error.
	var _ index.StoreIndex = (*Index)(nil)
	// WriteCheckpoint moved off the mandatory StoreIndex into the optional
	// CheckpointWriter capability; sqlite implements it.
	var _ index.CheckpointWriter = (*Index)(nil)
	// RestoreCheckpoint is the read side (CheckpointRestorer); sqlite implements it.
	var _ index.CheckpointRestorer = (*Index)(nil)
	idx := newMemoryIndex(t)
	var asInterface index.StoreIndex = idx
	if asInterface == nil {
		t.Fatal("Index does not satisfy index.StoreIndex")
	}
}
