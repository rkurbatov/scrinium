package store

// reinit.go — what a forced re-initialisation actually destroys, and the probe
// that decides whether it runs at all.
//
// # Why this is a file of its own
//
// Because destruction deserves to be read in one sitting. InitStore's other
// phases build things; this one is the only place in the engine that removes
// data a user put there on purpose, and it is worth being able to see the whole
// of what it touches without paging through key derivation.
//
// # The two levels, and why they differ the way they do
//
// WithForceReinit is a STRUCTURAL wipe: the descriptor replicas, every system
// artifact, every manifest, every index row. What survives is blob payload —
// the files under blobs/, chunks/, packs/ — together with their index rows,
// whose ref_counts drop to zero. The new store therefore begins on top of the
// old bytes: GC will reclaim them at its own pace, and until it does, dedup in
// a Plain store may legitimately reuse them.
//
// Keeping the blob ROWS is what makes keeping the blob FILES coherent, and this
// was the trap in the previous implementation. A blob file whose ref does not
// resolve is, by the Orphan Scan's definition, an orphan — and the Orphan Scan
// runs during the bootstrap at the end of every InitStore. So an implementation
// that wiped the index and left the files "for GC" was really scheduling their
// deletion for forty lines later, while an implementation that dropped the rows
// but kept the files would have made them invisible to GC forever. Zeroed
// ref_counts on surviving rows is the only shape in which the promise is true.
//
// WithPurgeOnReinit adds the payload: blob rows go with everything else, the
// files under every blob root are removed, and tombstone markers left by a GC
// that had marked but not yet swept are removed too. Afterwards the location
// holds a fresh store and nothing else.
//
// # What this does NOT do
//
// It does not tombstone anything, and it does not coordinate with other hosts.
// A forced re-init assumes the location is not in use: it is an administrative
// act on a store somebody has decided to stop having.
//
// It also does not reach past the driver's own view of the location. Anything a
// driver hides from iteration — for localfs, in-flight temp files — is left
// where it is, which is correct: those belong to a writer, and if a writer is
// live, this operation was a mistake no cleanup can fix.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/named"
	"scrinium.dev/engine/layout"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/errs"
)

// prepareInitLocation probes the Location for an existing store and applies the
// force-reinit policy.
//
// With no descriptor present it is a no-op: that is the ordinary fresh-location
// path, and WithForceReinit on a fresh location destroys nothing rather than
// erroring — "make me a store here, I do not care what was here" is satisfied
// by a location where nothing was.
//
// A descriptor that is present refuses with errs.ErrStoreAlreadyExists unless
// force is set; one that is present but unreadable refuses with
// errs.ErrStoreCorrupted unless force is set. Both refusals leave the location
// untouched, which is the property that makes them safe to retry.
func prepareInitLocation(
	ctx context.Context,
	drv driver.Driver,
	idx index.StoreIndex,
	o storeOptions,
	log *slog.Logger,
	wrap func(string, error) error,
) error {
	existing, probeErr := descriptor.Read(ctx, drv, o.hashRegistry)
	switch {
	case probeErr == nil:
		if !o.forceReinit {
			return fmt.Errorf("%w: storeId=%s", errs.ErrStoreAlreadyExists, existing.StoreID)
		}
		log.LogAttrs(ctx, slog.LevelWarn, "force-reinit: destroying the existing store",
			slog.String("store_id", existing.StoreID),
			slog.Bool("purge_payload", o.purgeOnReinit))
		return wipeForReinit(ctx, drv, idx, o.purgeOnReinit, log, wrap)

	case errors.Is(probeErr, errs.ErrArtifactNotFound):
		// Fresh Location, the normal path.
		return nil

	default:
		// The descriptor is there and cannot be read. Refuse without force: the
		// user has to decide whether they really mean to clobber whatever this
		// is, and an unreadable descriptor is exactly the state in which a
		// corpus might still be recoverable by other means.
		if !o.forceReinit {
			return fmt.Errorf("%w: descriptor present but unreadable: %v",
				errs.ErrStoreCorrupted, probeErr)
		}
		log.LogAttrs(ctx, slog.LevelWarn, "force-reinit: destroying a store with an unreadable descriptor",
			slog.String("error", probeErr.Error()),
			slog.Bool("purge_payload", o.purgeOnReinit))
		return wipeForReinit(ctx, drv, idx, o.purgeOnReinit, log, wrap)
	}
}

// reinitReport counts what a wipe removed, for the one log line that follows
// it. The counts are for a human reading a terminal after an irreversible
// operation: they are the difference between "it did the thing" and "it found
// nothing and said so cheerfully".
type reinitReport struct {
	Manifests  int
	System     int
	Staging    int
	Payload    int
	Tombstones int
}

// sweepTarget is one driver prefix to clear and the counter its removals land
// in, so the sweep loop reads as a list of what is being destroyed rather than
// as five near-identical blocks.
type sweepTarget struct {
	prefix string
	count  *int
}

// wipeForReinit turns the location into an empty one, to the depth purge asks
// for.
//
// Order is load-bearing, and the rule behind it is: leave every intermediate
// state self-healing. Tombstone markers go first because finding them needs the
// index that is about to be emptied. The index goes second, because a crash
// between the index reset and the file sweep leaves files nothing points at —
// which the next bootstrap's Orphan Scan removes on its own — whereas the
// reverse order would leave rows pointing at nothing, which only the rebuild
// agent can repair.
func wipeForReinit(
	ctx context.Context,
	drv driver.Driver,
	idx index.StoreIndex,
	purge bool,
	log *slog.Logger,
	wrap func(string, error) error,
) error {
	resetter, ok := idx.(index.Resetter)
	if !ok {
		// Refuse loudly rather than wipe partially. See index.Resetter for why
		// there is no portable fallback: one built from the mandatory methods
		// cannot reach handle-less rows, and would leave a store that looks
		// fresh while carrying unreachable, unreclaimable state.
		return wrap("force-reinit", fmt.Errorf(
			"%w: the configured StoreIndex cannot be emptied (it does not implement index.Resetter), "+
				"so a forced re-initialisation would leave the previous store's rows behind",
			errs.ErrNotImplemented))
	}

	var rep reinitReport

	if purge {
		n, err := sweepBlobTombstones(ctx, drv, idx)
		if err != nil {
			return wrap("force-reinit: tombstone sweep", err)
		}
		rep.Tombstones = n
	}

	if err := resetter.ResetIndex(ctx, index.ResetOptions{KeepBlobs: !purge}); err != nil {
		return wrap("force-reinit: reset index", err)
	}

	// Structural state: every manifest file, every system artifact (both
	// descriptor replicas among them, plus config versions, agent cursors and
	// checkpoint pointers), and any staging leftovers.
	targets := []sweepTarget{
		{prefix: "manifests", count: &rep.Manifests},
		{prefix: named.Root, count: &rep.System},
		{prefix: domain.StagingPrefix, count: &rep.Staging},
	}
	if purge {
		for _, root := range layout.BlobRoots() {
			targets = append(targets, sweepTarget{prefix: root, count: &rep.Payload})
		}
	}

	for _, s := range targets {
		n, err := sweepPrefix(ctx, drv, s.prefix)
		*s.count += n
		if err != nil {
			return wrap("force-reinit: sweep "+s.prefix, err)
		}
		// Tidy the shard directories the sweep emptied. A failure here is
		// cosmetic — empty directories cost nothing but tidiness — so it is
		// reported and not fatal.
		if err := drv.PruneEmptyDirs(ctx, s.prefix); err != nil {
			log.LogAttrs(ctx, slog.LevelWarn, "force-reinit: could not prune empty directories",
				slog.String("prefix", s.prefix), slog.String("error", err.Error()))
		}
	}

	log.LogAttrs(ctx, slog.LevelWarn, "force-reinit: location wiped",
		slog.Bool("purge_payload", purge),
		slog.Int("manifests_removed", rep.Manifests),
		slog.Int("system_artifacts_removed", rep.System),
		slog.Int("staging_removed", rep.Staging),
		slog.Int("payload_removed", rep.Payload),
		slog.Int("tombstones_removed", rep.Tombstones))
	return nil
}

// sweepPrefix removes every object the driver reports under prefix and returns
// how many went. A prefix that does not exist is an empty walk, so this is safe
// on a location that never had the directory.
//
// Paths are collected before anything is removed. Iterating and deleting in the
// same pass would ask a driver to enumerate a tree while it changes underneath
// — which localfs happens to survive and an object store need not.
func sweepPrefix(ctx context.Context, drv driver.Driver, prefix string) (int, error) {
	var paths []string
	if err := drv.ListObjectsWithModTime(ctx, prefix, time.Time{},
		func(om driver.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			paths = append(paths, om.Path)
			return nil
		}); err != nil {
		return 0, err
	}

	removed := 0
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if err := drv.Remove(ctx, p); err != nil {
			return removed, fmt.Errorf("remove %q: %w", p, err)
		}
		removed++
	}
	return removed, nil
}

// sweepBlobTombstones removes the tombstone markers of blobs a GC cycle had
// marked but not yet swept.
//
// They need their own pass because a marker is not an object as far as the
// driver's iteration is concerned — localfs filters marker files out of every
// listing — so the prefix sweep cannot see them, and the marker path itself is
// a driver-internal detail a caller may not construct. What a caller can do is
// name the ORIGINAL path and let the driver find its marker, which is what this
// does.
//
// Only orphans can carry one: a blob is marked after its ref_count reaches zero,
// and a Revive renames the marker away. Refs are collected before any resolve,
// because resolving inside the iteration callback would nest a query inside the
// index's own open cursor.
func sweepBlobTombstones(ctx context.Context, drv driver.Driver, idx index.StoreIndex) (int, error) {
	var refs []string
	if err := idx.ListOrphanBlobs(ctx, func(ref string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		refs = append(refs, ref)
		return nil
	}); err != nil {
		return 0, err
	}

	removed := 0
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		addr, err := idx.Resolve(ctx, ref)
		if err != nil {
			// The row went between the listing and here, or the index cannot
			// answer. Either way there is no path to hand the driver.
			continue
		}
		marked, _, err := drv.TombstoneInfo(ctx, addr.Path)
		if err != nil || !marked {
			// An unreadable marker state is skipped rather than fatal: the file
			// this is about is being destroyed anyway, and stopping a purge
			// half-way over a marker probe is the worse outcome.
			continue
		}
		if err := drv.RemoveTombstone(ctx, addr.Path); err != nil {
			return removed, fmt.Errorf("remove tombstone %q: %w", addr.Path, err)
		}
		removed++
	}
	return removed, nil
}
