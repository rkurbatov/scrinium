package indexsuite

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/engine/index"
	"scrinium.dev/testutil/manifestfx"
)

// runSyncSource exercises the optional synchronization capability (ADR-106).
// The capability is by-assertion (INV-106-1): a single-client backend does
// not implement index.SyncSource, and the suite skips rather than fails.
//
// All assertions are value-agnostic — they compare Tokens for ordering, never
// against absolute numbers. SQLite issues a 1,2,3 counter; another backend
// might map a sequence or txid. The "from the beginning" cursor is captured
// via Token() before the writes, not assumed to be the zero value.
func runSyncSource(t *testing.T, f Factory) {
	if _, ok := f.New(t).(index.SyncSource); !ok {
		t.Skipf("%s does not implement index.SyncSource (single-client backend)", f.Name)
	}

	t.Run("TokenMonotonic", func(t *testing.T) {
		ctx := context.Background()
		idx := f.New(t)
		ss := idx.(index.SyncSource)

		m := manifestfx.Blob("art-1", "b10b0001")

		t0, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatalf("IndexManifest #1: %v", err)
		}
		t1, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-2", "b10b0002"),
			manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
			t.Fatalf("IndexManifest #2: %v", err)
		}
		t2, _ := ss.Token(ctx)
		if err := idx.DeleteManifest(ctx, m.Digest); err != nil {
			t.Fatalf("DeleteManifest: %v", err)
		}
		t3, _ := ss.Token(ctx)

		if !(t0 < t1 && t1 < t2 && t2 < t3) {
			t.Errorf("Token not strictly monotonic across Index/Index/Delete: %d, %d, %d, %d",
				t0, t1, t2, t3)
		}
	})

	t.Run("SinceReturnsChangesInOrder", func(t *testing.T) {
		ctx := context.Background()
		idx := f.New(t)
		ss := idx.(index.SyncSource)

		base, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		m1 := manifestfx.Blob("art-1", "b10b0001")
		m2 := manifestfx.Blob("art-2", "b10b0002")
		if err := idx.IndexManifest(ctx, m1, manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatal(err)
		}
		if err := idx.IndexManifest(ctx, m2, manifestfx.PhysAddr("blobs/cc/dd/blob-2")); err != nil {
			t.Fatal(err)
		}

		d, err := ss.Since(ctx, base)
		if err != nil {
			t.Fatalf("Since(base): %v", err)
		}
		if len(d.Changes) != 2 {
			t.Fatalf("Since(base) changes = %d, want 2", len(d.Changes))
		}
		if !(d.Changes[0].CSN < d.Changes[1].CSN) {
			t.Errorf("changes not ordered by csn: %d then %d", d.Changes[0].CSN, d.Changes[1].CSN)
		}
		if d.Changes[0].Digest != m1.Digest || d.Changes[1].Digest != m2.Digest {
			t.Errorf("digests = %q, %q; want %q, %q",
				d.Changes[0].Digest, d.Changes[1].Digest, m1.Digest, m2.Digest)
		}
		if d.Next != d.Changes[1].CSN {
			t.Errorf("Next = %d, want %d (last returned csn)", d.Next, d.Changes[1].CSN)
		}
		if d.Gapped {
			t.Error("Gapped = true with no deletion")
		}

		// Resuming from Next yields nothing and does not regress the cursor.
		d2, err := ss.Since(ctx, d.Next)
		if err != nil {
			t.Fatalf("Since(Next): %v", err)
		}
		if len(d2.Changes) != 0 {
			t.Errorf("Since(Next) changes = %d, want 0", len(d2.Changes))
		}
		if d2.Next != d.Next {
			t.Errorf("Since(Next).Next = %d, want %d", d2.Next, d.Next)
		}
	})

	// SinceFiltersDeletedAndFlagsGap encodes the ADR-106 D2-A contract: a hard
	// deletion removes the row, so the deleted digest is not enumerable; the
	// deletion is reported through Gapped for a cursor taken before it.
	t.Run("SinceFiltersDeletedAndFlagsGap", func(t *testing.T) {
		ctx := context.Background()
		idx := f.New(t)
		ss := idx.(index.SyncSource)

		base, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
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

		d, err := ss.Since(ctx, base)
		if err != nil {
			t.Fatalf("Since(base): %v", err)
		}
		foundM2 := false
		for _, c := range d.Changes {
			if c.Digest == m1.Digest {
				t.Error("Since enumerated a hard-deleted digest")
			}
			if c.Digest == m2.Digest {
				foundM2 = true
			}
		}
		if !foundM2 {
			t.Error("Since did not enumerate the surviving manifest")
		}
		if !d.Gapped {
			t.Error("Gapped = false for a cursor below the prune watermark, want true")
		}

		// A cursor at the current Token is past the deletion → not gapped.
		cur, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		dc, err := ss.Since(ctx, cur)
		if err != nil {
			t.Fatalf("Since(Token): %v", err)
		}
		if dc.Gapped {
			t.Errorf("Gapped = true at cursor=Token (%d), want false", cur)
		}
	})

	// Wait is covered portably here by its fast path and ctx handling. The
	// end-to-end "a blocked Wait wakes on another writer's commit" needs an
	// index observable across connections, which the shared factory does not
	// promise (SQLite's conformance factory is in-memory); that path is
	// validated per-backend on a shared-state index.
	t.Run("Wait", func(t *testing.T) {
		ctx := context.Background()
		idx := f.New(t)
		sw, ok := idx.(index.SyncWaiter)
		if !ok {
			t.Skipf("%s does not implement index.SyncWaiter", f.Name)
		}
		ss := idx.(index.SyncSource)

		// Fast path: once the index has moved past `after`, Wait returns the
		// new Token without blocking.
		before, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		if err := idx.IndexManifest(ctx, manifestfx.Blob("art-1", "b10b0001"),
			manifestfx.PhysAddr("blobs/aa/bb/blob-1")); err != nil {
			t.Fatalf("IndexManifest: %v", err)
		}
		got, err := sw.Wait(ctx, before)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		cur, _ := ss.Token(ctx)
		if got != cur || !(got > before) {
			t.Errorf("Wait fast path = %d, want current Token %d (> %d)", got, cur, before)
		}

		// Respects ctx: with nothing advancing the index, Wait blocks until
		// the context is cancelled.
		wctx, cancel := context.WithCancel(ctx)
		at, err := ss.Token(ctx)
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		done := make(chan error, 1)
		go func() { _, e := sw.Wait(wctx, at); done <- e }()
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
	})
}
