package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/engine/index"
	"scrinium.dev/testutil/manifestfx"
)

// TestSync_TokenMonotonic checks Token starts at zero and advances on each
// committed mutation, including a delete (ADR-106).
func TestSync_TokenMonotonic(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	t0, err := idx.Token(ctx)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if t0 != 0 {
		t.Errorf("fresh Token = %d, want 0", t0)
	}

	m := manifestfx.Blob("art-1", "b10b0001")
	if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest #1: %v", err)
	}
	t1, _ := idx.Token(ctx)

	if err := idx.IndexManifest(ctx, manifestfx.Blob("art-2", "b10b0002"),
		manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
		t.Fatalf("IndexManifest #2: %v", err)
	}
	t2, _ := idx.Token(ctx)

	if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	t3, _ := idx.Token(ctx)

	if !(t0 < t1 && t1 < t2 && t2 < t3) {
		t.Errorf("Token not strictly monotonic: %d, %d, %d, %d", t0, t1, t2, t3)
	}
}

// TestSync_SinceReturnsChangesInOrder checks Since enumerates changed
// manifests with csn greater than the cursor, in csn order, with Next set to
// the last returned csn and Gapped clear when nothing was pruned.
func TestSync_SinceReturnsChangesInOrder(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	m1 := manifestfx.Blob("art-1", "b10b0001")
	m2 := manifestfx.Blob("art-2", "b10b0002")
	if err := idx.IndexManifest(ctx, m1, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexManifest(ctx, m2, manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
		t.Fatal(err)
	}

	d, err := idx.Since(ctx, 0)
	if err != nil {
		t.Fatalf("Since(0): %v", err)
	}
	if len(d.Changes) != 2 {
		t.Fatalf("Since(0) changes = %d, want 2", len(d.Changes))
	}
	if d.Changes[0].CSN >= d.Changes[1].CSN {
		t.Errorf("changes not in csn order: %d then %d", d.Changes[0].CSN, d.Changes[1].CSN)
	}
	if d.Changes[0].Digest != m1.Digest || d.Changes[1].Digest != m2.Digest {
		t.Errorf("digests = %q, %q; want %q, %q",
			d.Changes[0].Digest, d.Changes[1].Digest, m1.Digest, m2.Digest)
	}
	if d.Next != d.Changes[1].CSN {
		t.Errorf("Next = %d, want %d (last csn)", d.Next, d.Changes[1].CSN)
	}
	if d.Gapped {
		t.Error("Gapped = true with no deletion")
	}

	// Resuming from Next yields nothing, and Next does not regress.
	d2, err := idx.Since(ctx, d.Next)
	if err != nil {
		t.Fatalf("Since(Next): %v", err)
	}
	if len(d2.Changes) != 0 {
		t.Errorf("Since(Next) changes = %d, want 0", len(d2.Changes))
	}
	if d2.Next != d.Next {
		t.Errorf("Since(Next).Next = %d, want %d", d2.Next, d.Next)
	}
}

// TestSync_SinceFiltersDeletedAndFlagsGap checks a hard-deleted digest is not
// enumerated by Since, and that a cursor below the prune watermark is Gapped
// while a cursor at the current Token is not (ADR-106 D2-A).
func TestSync_SinceFiltersDeletedAndFlagsGap(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	m1 := manifestfx.Blob("art-1", "b10b0001")
	m2 := manifestfx.Blob("art-2", "b10b0002")
	if err := idx.IndexManifest(ctx, m1, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexManifest(ctx, m2, manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
		t.Fatal(err)
	}
	if err := idx.DeleteManifest(ctx, m1.Digest); err != nil {
		t.Fatal(err)
	}

	d, err := idx.Since(ctx, 0)
	if err != nil {
		t.Fatalf("Since(0): %v", err)
	}
	for _, c := range d.Changes {
		if c.Digest == m1.Digest {
			t.Error("Since enumerated a hard-deleted digest")
		}
	}
	if len(d.Changes) != 1 || d.Changes[0].Digest != m2.Digest {
		t.Errorf("Since(0) changes = %+v, want only art-2", d.Changes)
	}
	if !d.Gapped {
		t.Error("Gapped = false at cursor 0, want true (below prune watermark)")
	}

	// A cursor at the current Token is past the prune point → not gapped.
	tok, err := idx.Token(ctx)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	dt, err := idx.Since(ctx, tok)
	if err != nil {
		t.Fatalf("Since(Token): %v", err)
	}
	if dt.Gapped {
		t.Errorf("Gapped = true at cursor=Token (%d), want false", tok)
	}
}

// TestSync_WaitFastPath checks Wait returns immediately when the index has
// already moved past `after`.
func TestSync_WaitFastPath(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx := context.Background()

	if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"),
		manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}
	got, err := idx.Wait(ctx, 0)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got != 1 {
		t.Errorf("Wait(0) fast path = %d, want 1", got)
	}
}

// TestSync_WaitWakesOnChange checks Wait blocks until a concurrent write
// advances Token. Uses a disk index: a :memory: pool gives each connection a
// separate database, so the poller and the writer would not share state.
func TestSync_WaitWakesOnChange(t *testing.T) {
	idx, _ := newDiskIndex(t)
	ctx := context.Background()

	after, err := idx.Token(ctx)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	var (
		got     index.Token
		waitErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		got, waitErr = idx.Wait(wctx, after)
	}()

	// Let the waiter enter its poll loop, then write.
	time.Sleep(20 * time.Millisecond)
	if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"),
		manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
		t.Fatalf("IndexManifest: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return after a change")
	}
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}
	if got <= after {
		t.Errorf("Wait returned %d, want > %d", got, after)
	}
}

// TestSync_WaitRespectsContext checks Wait returns ctx.Err() when the context
// is cancelled while blocking.
func TestSync_WaitRespectsContext(t *testing.T) {
	idx := newMemoryIndex(t)
	ctx, cancel := context.WithCancel(context.Background())

	after, err := idx.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	done := make(chan error, 1)
	go func() { _, e := idx.Wait(ctx, after); done <- e }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Errorf("Wait error = %v, want context.Canceled", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after ctx cancel")
	}
}
