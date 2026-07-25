package storesuite

// Forced re-initialisation: what it destroys, what it deliberately keeps, and
// the two combinations it refuses.
//
// The load-bearing case is TestForceReinit_KeepsPayloadReclaimable. A re-init
// ends in the same bootstrap every open does, Orphan Scan included, and that
// scan deletes any blob file whose ref does not resolve — so "payload survives
// for GC" is only true if the blob ROWS survive too. An implementation that
// wiped the index wholesale would pass every other test in this file and
// silently delete the bytes it promised to keep.

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver/localfs"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

// seeded is a location that already holds a store with one artifact in it, plus
// the pieces needed to re-init over it: the same driver and the same index
// handle a real host would still be holding.
type seeded struct {
	drv  *localfs.Driver
	idx  index.StoreIndex
	id   domain.ArtifactID
	body string
}

func seedStore(t *testing.T) seeded {
	t.Helper()
	ctx := context.Background()

	s, drv, idx := storefx.InitShared(t)
	const body = "a book nobody will read again"
	id, err := s.Put(ctx, domain.Artifact{Payload: strings.NewReader(body)})
	if err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if n := storefx.OnDiskAt(drv.Root()).BlobCount(); n == 0 {
		t.Fatal("seed wrote no blob file — the payload assertions would be vacuous")
	}
	return seeded{drv: drv, idx: idx, id: id, body: body}
}

// reinit runs InitStore over a seeded location with the given extra options.
func reinit(t *testing.T, s seeded, extra ...store.StoreOption) (store.Store, error) {
	t.Helper()
	opts := append([]store.StoreOption{
		store.WithStoreIndex(s.idx),
		store.WithHashRegistry(storefx.Hashes()),
	}, extra...)
	st, _, err := store.InitStore(context.Background(), s.drv, opts...)
	return st, err
}

// countFiles reports the regular files under <root>/<prefix>. A missing
// directory counts as zero, which is the answer a purge should produce.
func countFiles(root, prefix string) int {
	n := 0
	_ = filepath.WalkDir(filepath.Join(root, prefix), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// A forced re-init leaves nothing of the old store's identity or content: the
// artifact is unreachable, the walk is empty, the manifests are off the disk —
// and the new store works.
func TestForceReinit_DestroysTheOldStore(t *testing.T) {
	ctx := context.Background()
	s := seedStore(t)

	if n := countFiles(s.drv.Root(), "manifests"); n == 0 {
		t.Fatal("seed wrote no manifest file — the assertion below would be vacuous")
	}

	st, err := reinit(t, s, store.WithForceReinit())
	if err != nil {
		t.Fatalf("force-reinit: %v", err)
	}
	if st.State() != domain.StateUnlocked {
		t.Errorf("state after force-reinit: got %v, want %v", st.State(), domain.StateUnlocked)
	}

	if _, err := st.Get(ctx, s.id); err == nil {
		t.Error("the old artifact is still readable after a forced re-init")
	}

	seen := 0
	if err := st.Walk(ctx, func(domain.Manifest) error {
		seen++
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if seen != 0 {
		t.Errorf("Walk after force-reinit yielded %d manifests, want 0", seen)
	}

	if n := countFiles(s.drv.Root(), "manifests"); n != 0 {
		t.Errorf("%d manifest files survived a forced re-init, want 0", n)
	}

	// The new store is a store, not a shell.
	const fresh = "the first book of the new store"
	id, err := st.Put(ctx, domain.Artifact{Payload: strings.NewReader(fresh)})
	if err != nil {
		t.Fatalf("Put after force-reinit: %v", err)
	}
	rh, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after force-reinit: %v", err)
	}
	defer func() { _ = rh.Close() }()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != fresh {
		t.Errorf("round-trip after force-reinit = %q, want %q", got, fresh)
	}
}

// Without a purge the payload stays — and stays RECLAIMABLE, which means the
// blob rows are still there with their counts at zero. Both halves are asserted:
// the file (it survived the bootstrap Orphan Scan) and the row (GC can still see
// what to reclaim).
func TestForceReinit_KeepsPayloadReclaimable(t *testing.T) {
	ctx := context.Background()
	s := seedStore(t)

	before := storefx.OnDiskAt(s.drv.Root()).BlobFiles()

	if _, err := reinit(t, s, store.WithForceReinit()); err != nil {
		t.Fatalf("force-reinit: %v", err)
	}

	after := storefx.OnDiskAt(s.drv.Root()).BlobFiles()
	if len(after) != len(before) {
		t.Fatalf("blob files after force-reinit: %d, want %d (the Orphan Scan ate the payload "+
			"the re-init promised to keep)", len(after), len(before))
	}

	// Each survivor must still resolve — that is precisely why the Orphan Scan
	// left it alone — and must be orphaned, or GC will never take it.
	//
	// The refs are collected before anything is resolved, for the same reason
	// the engine's own tombstone sweep does it: a query issued inside the
	// iteration callback needs a second connection, and against an in-memory
	// SQLite a second connection is a second, empty database.
	var orphans []string
	if err := s.idx.ListOrphanBlobs(ctx, func(ref string) error {
		orphans = append(orphans, ref)
		return nil
	}); err != nil {
		t.Fatalf("ListOrphanBlobs: %v", err)
	}
	if len(orphans) == 0 {
		t.Fatal("no orphan blob rows after force-reinit: the kept payload is unreclaimable")
	}
	for _, ref := range orphans {
		if _, err := s.idx.Resolve(ctx, ref); err != nil {
			t.Errorf("kept blob %s does not resolve: %v", ref, err)
		}
	}
}

// A purge takes the payload with everything else. The location holds a fresh
// store and nothing more.
func TestForceReinit_Purge_RemovesPayload(t *testing.T) {
	ctx := context.Background()
	s := seedStore(t)

	if _, err := reinit(t, s, store.WithForceReinit(), store.WithPurgeOnReinit()); err != nil {
		t.Fatalf("purging force-reinit: %v", err)
	}

	if n := storefx.OnDiskAt(s.drv.Root()).BlobCount(); n != 0 {
		t.Errorf("%d blob files survived a purging re-init, want 0", n)
	}
	if n := countFiles(s.drv.Root(), "manifests"); n != 0 {
		t.Errorf("%d manifest files survived a purging re-init, want 0", n)
	}

	rows := 0
	if err := s.idx.ListOrphanBlobs(ctx, func(string) error {
		rows++
		return nil
	}); err != nil {
		t.Fatalf("ListOrphanBlobs: %v", err)
	}
	if rows != 0 {
		t.Errorf("%d blob rows survived a purging re-init, want 0", rows)
	}
}

// The refusals. Each leaves the store where it was — which is what makes them
// safe to hit by accident.
func TestForceReinit_Refusals(t *testing.T) {
	t.Run("purge without force", func(t *testing.T) {
		s := seedStore(t)
		_, err := reinit(t, s, store.WithPurgeOnReinit())
		if err == nil {
			t.Fatal("expected a refusal: a purge with nothing authorised to destroy")
		}
		if !strings.Contains(err.Error(), "WithForceReinit") {
			t.Errorf("error %v should name the option that was missing", err)
		}
		// The store it refused to purge is still there. Probed through the
		// descriptor rather than by opening: an open would run an Orphan Scan
		// against a fresh index and delete the very payload this is checking on.
		if _, err := descriptor.Read(context.Background(), s.drv, storefx.Hashes()); err != nil {
			t.Errorf("the refused purge damaged the store: %v", err)
		}
	})

	t.Run("no force at all", func(t *testing.T) {
		s := seedStore(t)
		_, err := reinit(t, s)
		if !errors.Is(err, errs.ErrStoreAlreadyExists) {
			t.Fatalf("got %v, want %v", err, errs.ErrStoreAlreadyExists)
		}
	})

	t.Run("index that cannot be emptied", func(t *testing.T) {
		s := seedStore(t)
		// Embedding the interface satisfies StoreIndex while hiding ResetIndex:
		// exactly the shape of a backend that does not implement the optional
		// capability.
		blind := noResetIndex{StoreIndex: s.idx}
		_, _, err := store.InitStore(context.Background(), s.drv,
			store.WithStoreIndex(blind),
			store.WithHashRegistry(storefx.Hashes()),
			store.WithForceReinit(),
		)
		if err == nil {
			t.Fatal("expected a refusal: the index cannot be emptied")
		}
		if !errors.Is(err, errs.ErrNotImplemented) {
			t.Errorf("got %v, want it to wrap %v", err, errs.ErrNotImplemented)
		}
		// Nothing was destroyed on the way to the refusal.
		if n := countFiles(s.drv.Root(), "manifests"); n == 0 {
			t.Error("the refused re-init removed manifests before refusing")
		}
	})
}

// noResetIndex is a StoreIndex without the Resetter capability.
type noResetIndex struct{ index.StoreIndex }

// A forced re-init on a location that never held a store destroys nothing and
// succeeds — "make me a store here, whatever was here" is satisfied by nothing
// having been here.
func TestForceReinit_FreshLocation_IsPlainInit(t *testing.T) {
	drv := driverfx.LocalFS(t)
	st, _, err := store.InitStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithForceReinit(),
	)
	if err != nil {
		t.Fatalf("force-reinit on a fresh location: %v", err)
	}
	if st.State() != domain.StateUnlocked {
		t.Errorf("state: got %v, want %v", st.State(), domain.StateUnlocked)
	}
}
