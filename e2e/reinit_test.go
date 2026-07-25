//go:build e2e

package e2e

// Forced re-initialisation through the facade: the whole stack, the way a host
// actually reaches it — a real localfs location, a real sqlite index in a file
// under it, extensions installed, and the destructive options arriving as build
// options rather than as engine internals.
//
// This is the level at which the interesting failure would have hidden. The
// engine's own tests hand InitStore an index they constructed; here the index is
// dialled by the assembler, opened before the store, and populated by the
// extensions during assembly — so a re-init that forgot any of those layers
// would come out looking fine here and only here.

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	scrinium "scrinium.dev"
	"scrinium.dev/domain"
	"scrinium.dev/errs"

	_ "scrinium.dev/engine/driver/localfs"
	_ "scrinium.dev/engine/index/sqlite"
)

// seedLocation builds a store at a fresh location, puts one artifact in it and
// closes it, returning the directory and the artifact's handle.
func seedLocation(t *testing.T) (dir string, id domain.ArtifactID) {
	t.Helper()
	ctx := context.Background()
	dir = t.TempDir()

	c, err := scrinium.Open(ctx, "file://"+dir)
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	id, err = c.Put(ctx, scrinium.Artifact{Payload: strings.NewReader("a seeded book")})
	if err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if countUnder(dir, "blobs") == 0 {
		t.Fatal("seed wrote no blob file — the payload assertions would be vacuous")
	}
	return dir, id
}

// countUnder counts regular files under <dir>/<prefix>; a missing directory is
// zero, which is what a purge should leave behind.
func countUnder(dir, prefix string) int {
	n := 0
	_ = filepath.WalkDir(filepath.Join(dir, prefix), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// An init over an existing store is refused, and the store is untouched.
func TestReinit_InitOverExistingStore_Refused(t *testing.T) {
	ctx := context.Background()
	dir, id := seedLocation(t)

	_, err := scrinium.Open(ctx, "file://"+dir, scrinium.WithMode(scrinium.ModeInit))
	if !errors.Is(err, errs.ErrStoreAlreadyExists) {
		t.Fatalf("init over an existing store: got %v, want %v", err, errs.ErrStoreAlreadyExists)
	}

	// Untouched means readable: reopen and read the seeded artifact back.
	c, err := scrinium.Open(ctx, "file://"+dir, scrinium.WithMode(scrinium.ModeOpen))
	if err != nil {
		t.Fatalf("reopen after the refusal: %v", err)
	}
	defer func() { _ = c.Close() }()
	rh, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("the refused init damaged the store: %v", err)
	}
	_ = rh.Close()
}

// Force destroys the store and keeps the bytes; the fresh store is usable and
// carries a new identity.
func TestReinit_Force_DestroysStoreKeepsPayload(t *testing.T) {
	ctx := context.Background()
	dir, id := seedLocation(t)
	blobsBefore := countUnder(dir, "blobs")

	c, err := scrinium.Open(ctx, "file://"+dir,
		scrinium.WithMode(scrinium.ModeInit),
		scrinium.WithForceReinit(),
	)
	if err != nil {
		t.Fatalf("forced re-init: %v", err)
	}
	defer func() { _ = c.Close() }()

	if !c.Info.Created {
		t.Error("Info.Created is false after a forced re-init — this assembly did create the store")
	}
	if _, err := c.Get(ctx, id); err == nil {
		t.Error("the old artifact is still readable after a forced re-init")
	}

	seen := 0
	if err := c.Walk(ctx, func(domain.Manifest) error {
		seen++
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if seen != 0 {
		t.Errorf("Walk after a forced re-init yielded %d manifests, want 0", seen)
	}
	if n := countUnder(dir, "manifests"); n != 0 {
		t.Errorf("%d manifest files survived a forced re-init, want 0", n)
	}

	// The payload the re-init promised to keep is still on disk. It has to
	// survive the Orphan Scan that ran inside the very call above, which it can
	// only do because the blob rows survived with it.
	if n := countUnder(dir, "blobs"); n != blobsBefore {
		t.Errorf("blob files after a forced re-init: %d, want %d", n, blobsBefore)
	}

	const fresh = "the first book of the new store"
	newID, err := c.Put(ctx, scrinium.Artifact{Payload: strings.NewReader(fresh)})
	if err != nil {
		t.Fatalf("Put after a forced re-init: %v", err)
	}
	rh, err := c.Get(ctx, newID)
	if err != nil {
		t.Fatalf("Get after a forced re-init: %v", err)
	}
	defer func() { _ = rh.Close() }()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != fresh {
		t.Errorf("round-trip = %q, want %q", got, fresh)
	}
}

// A purging re-init leaves the location bare: no manifests, no payload, and an
// index file holding a store with nothing in it.
func TestReinit_Purge_LeavesLocationBare(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedLocation(t)

	c, err := scrinium.Open(ctx, "file://"+dir,
		scrinium.WithMode(scrinium.ModeInit),
		scrinium.WithForceReinit(),
		scrinium.WithPurgeOnReinit(),
	)
	if err != nil {
		t.Fatalf("purging re-init: %v", err)
	}
	defer func() { _ = c.Close() }()

	for _, prefix := range []string{"blobs", "chunks", "packs", "manifests"} {
		if n := countUnder(dir, prefix); n != 0 {
			t.Errorf("%d files survived a purging re-init under %s/, want 0", n, prefix)
		}
	}

	// The store still works, and the location is a store rather than a bare
	// directory: the descriptor and config were rewritten by the init.
	if _, err := c.Put(ctx, scrinium.Artifact{Payload: strings.NewReader("after the purge")}); err != nil {
		t.Fatalf("Put after a purging re-init: %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) == 0 {
		t.Errorf("location looks empty after a re-init that should have rebuilt it: %v", err)
	}
}

// Survives a restart: the store a purging re-init produced reopens, and what was
// destroyed stays destroyed. The reopen matters because it is the first time the
// index is read by a process that did not just reset it.
func TestReinit_Purge_ReopensClean(t *testing.T) {
	ctx := context.Background()
	dir, id := seedLocation(t)

	c, err := scrinium.Open(ctx, "file://"+dir,
		scrinium.WithMode(scrinium.ModeInit),
		scrinium.WithForceReinit(),
		scrinium.WithPurgeOnReinit(),
	)
	if err != nil {
		t.Fatalf("purging re-init: %v", err)
	}
	kept, err := c.Put(ctx, scrinium.Artifact{Payload: strings.NewReader("written after the purge")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2, err := scrinium.Open(ctx, "file://"+dir, scrinium.WithMode(scrinium.ModeOpen))
	if err != nil {
		t.Fatalf("reopen after a purging re-init: %v", err)
	}
	defer func() { _ = c2.Close() }()

	if _, err := c2.Get(ctx, kept); err != nil {
		t.Errorf("the artifact written after the purge did not survive a reopen: %v", err)
	}
	if _, err := c2.Get(ctx, id); err == nil {
		t.Error("the purged artifact came back after a reopen")
	}
}

// The two refusals the facade adds on top of the engine's, both about a
// destructive flag that would otherwise sit armed and inert.
func TestReinit_OptionRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("force without ModeInit", func(t *testing.T) {
		dir, _ := seedLocation(t)
		_, err := scrinium.Open(ctx, "file://"+dir, scrinium.WithForceReinit())
		if err == nil {
			t.Fatal("expected a refusal: force under the default open-or-init mode")
		}
		if !strings.Contains(err.Error(), "ModeInit") {
			t.Errorf("error %v should name the mode it requires", err)
		}
	})

	t.Run("purge without force", func(t *testing.T) {
		dir, id := seedLocation(t)
		_, err := scrinium.Open(ctx, "file://"+dir,
			scrinium.WithMode(scrinium.ModeInit),
			scrinium.WithPurgeOnReinit(),
		)
		if err == nil {
			t.Fatal("expected a refusal: a purge with nothing authorised to destroy")
		}
		if !strings.Contains(err.Error(), "WithForceReinit") {
			t.Errorf("error %v should name the option that was missing", err)
		}
		// And it refused before touching anything.
		c, err := scrinium.Open(ctx, "file://"+dir, scrinium.WithMode(scrinium.ModeOpen))
		if err != nil {
			t.Fatalf("reopen after the refusal: %v", err)
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Get(ctx, id); err != nil {
			t.Errorf("the refused purge damaged the store: %v", err)
		}
	})
}
