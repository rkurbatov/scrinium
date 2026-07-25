package view

import (
	"context"
	"fmt"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// backfill walks the source and classifies each manifest. The
// caller holds no lock; backfill is single-threaded by virtue of
// running before New returns.
//
// Two paths exist:
//
//   - Fast path (an MetadataSource is configured). Source.Walk
//     gives us the stripped manifests; we top them up by
//     calling metadataSource.Metadata(id) — an O(log N) lookup against
//     a local index custom index that persisted the ext block at
//     write time. No round-trip to Source.Get per manifest.
//
//   - Slow path (no MetadataSource). We round-trip Source.Get for
//     every manifest to recover Ext. This is N+1 by
//     construction; acceptable for tests with FakeSource
//     (which keeps full manifests in memory anyway, so Get is
//     cheap) and for backfills small enough that latency
//     doesn't matter. Daemons with large stores configure
//     WithMetadataSource (or WithFSPathIndex) to take the fast path.
func (v *View) backfill(ctx context.Context) error {
	cb := func(m domain.Manifest) error {
		if !v.passesFilter(m) {
			return nil
		}
		v.populateExt(ctx, &m)
		var events []event.Event
		v.indexArtifact(m, true /*duringBackfill*/, &events)
		// backfill holds no lock and runs single-threaded before New
		// returns, so emitting inline cannot deadlock; route through emit
		// anyway to keep the publish path uniform.
		v.emit(events)
		return nil
	}
	if err := v.src.Walk(ctx, cb); err != nil {
		return fmt.Errorf("%w: %w", errs.ErrSourceUnavailable, err)
	}
	return nil
}

// populateExt fills m.Ext (and a couple of cheap neighbouring
// fields) from the configured fast path or, failing that, from
// Source.Get. Mutates m in place.
//
// Errors from either path are intentionally swallowed — backfill
// must finish even if one manifest's ext fetch fails (the
// resolver will then treat it as orphaned, the standard
// missing-path behaviour). A noisy "best effort" log line could
// be added behind the bus once we wire it.
func (v *View) populateExt(ctx context.Context, m *domain.Manifest) {
	// Fast path: bulk MetadataSource lookup, no Source round-trip.
	if v.opts.metadataSource != nil {
		raw, ok, err := v.opts.metadataSource.Metadata(m.ArtifactID)
		if err == nil && ok && len(raw) > 0 {
			m.Ext = raw
			return
		}
		// Miss or error — fall through to Source.Get. A miss is
		// legitimate (artifact written before the custom index was
		// registered, or custom index doesn't index this schema);
		// for those rare cases the slow path is correct.
	}

	// Slow path: round-trip Source.Get for the full manifest.
	// Walk-style sources (store.Store-backed) usually return a
	// stripped manifest from the index — Ext, layout, inline
	// blob, etc. are absent. Resolvers like vfsmeta need Ext to
	// produce a path.
	//
	// FakeSource and similar in-memory test stubs already return
	// complete manifests; doing a Get on top is a cheap no-op.
	rh, err := v.src.Get(ctx, m.ArtifactID)
	if err != nil {
		return
	}
	full := rh.Manifest()
	rh.Close()
	// Prefer the canonical Ext block; fall back to the legacy
	// Metadata field for Sealed/Paranoid manifests whose crypto
	// path has not migrated yet.
	if extBytes := full.Ext; len(extBytes) > 0 {
		m.Ext = extBytes
	}
	if full.ContentHash != "" {
		m.ContentHash = full.ContentHash
	}
	if full.OriginalSize != 0 {
		m.OriginalSize = full.OriginalSize
	}
}

// passesFilter checks the configured Filter against a
// manifest. All non-zero conditions combine by AND. Prefix is
// applied to the resolver path — see resolvePathForManifest below.
func (v *View) passesFilter(m domain.Manifest) bool {
	f := v.opts.filter
	if f.SessionID != "" && m.SessionID != f.SessionID {
		return false
	}
	if f.Prefix != "" {
		path, ok := v.resolvePathForManifest(m)
		if !ok {
			return false
		}
		if !strings.HasPrefix(path, f.Prefix) {
			return false
		}
	}
	return true
}

// resolvePathForManifest applies the orphaning view's placement
// function, then the fallback policy, returning (path, true) when the
// artifact has any such representation (real or synthetic), or
// ("", false) when it does not (FallbackOrphaned + no placement). Used
// only for Filter.Prefix; the orphaning view is found in the active set
// so the View names none.
func (v *View) resolvePathForManifest(m domain.Manifest) (string, bool) {
	for _, d := range v.defs {
		if d.orphans && d.path != nil {
			if path, ok := d.path(m); ok {
				return path, true
			}
			break
		}
	}
	if v.opts.fallback == FallbackSynthetic {
		return v.syntheticPath(m), true
	}
	return "", false
}

// syntheticPath builds a synthetic by-path entry for artifacts
// without a resolver path. Format mirrors docs/4 §14.4.4:
//
//	<sid-shard>/<id-short>.bin   — session present
//	_anonymous/<id-short>.bin    — no session
func (v *View) syntheticPath(m domain.Manifest) string {
	idShort := shortID(m.ArtifactID)
	if m.SessionID != "" {
		return sessionShard(m.SessionID) + "/" + idShort + ".bin"
	}
	return "_anonymous/" + idShort + ".bin"
}

// indexArtifact registers an artifact in every applicable tree by
// iterating the active view set. duringBackfill is retained for symmetry
// (Stats are maintained identically on Add and backfill).
//
// The function does NOT take the View lock — callers (backfill,
// Add) handle locking themselves. backfill runs single-threaded;
// Add takes the write lock around the call.
//
// Collision events raised while placing the artifact are appended to
// *events instead of being published inline; the caller flushes them with
// emit after releasing the lock (see emit's contract).
func (v *View) indexArtifact(m domain.Manifest, duringBackfill bool, events *[]event.Event) {
	rec := &artifactRecord{manifest: m, paths: make(map[RootView]string)}

	for _, d := range v.defs {
		var (
			path string
			ok   bool
		)
		if d.path != nil {
			path, ok = d.path(m)
		}
		// Whether the capture declared this artifact a directory. The tree
		// still decides the node's kind (children or a declaration); this is
		// the half it cannot infer for an empty captured directory (ADR-116).
		declaredDir := d.isDir != nil && d.isDir(m)
		if !ok {
			// No placement in this view. Orphaning views (by-path)
			// send the artifact to the orphan tree, or to a synthetic
			// by-path entry under FallbackSynthetic; other views skip.
			if !d.orphans {
				continue
			}
			if v.opts.fallback == FallbackSynthetic {
				sp := v.syntheticPath(m)
				rec.paths[d.root] = sp
				if d.collide {
					v.applyCollisionInsert(d.root, sp, m, rec, declaredDir, events)
				} else {
					v.insertArtifact(v.trees[d.root], sp, m, declaredDir)
				}
			} else {
				op := byArtifactPath(m.ArtifactID)
				rec.paths[RootByOrphaned] = op
				// The orphan tree is id-shaped: a synthetic leaf per artifact,
				// where a directory declaration has no meaning.
				v.insertArtifact(v.trees[RootByOrphaned], op, m, false)
				v.Stats.OrphanedCount++
			}
			continue
		}

		rec.paths[d.root] = path
		if d.collide {
			v.applyCollisionInsert(d.root, path, m, rec, declaredDir, events)
		} else {
			v.insertArtifact(v.trees[d.root], path, m, declaredDir)
		}
		if d.countKey != nil {
			if key, kok := d.countKey(m); kok {
				v.seenKeys[d.root][key] = struct{}{}
			}
		}
	}

	v.artifacts[m.ArtifactID] = rec

	// Stats. TotalNodes counts artifacts (not virtual dirs).
	v.Stats.TotalNodes++
	v.Stats.TotalBytes += m.OriginalSize
	// Distinct-cardinality counters from the per-view seen-key sets.
	// by-session is an intrinsic view and keeps a named counter; every
	// other counting view (including any extension-provided one) lands
	// in ViewCounts under its own root, so this stays generic.
	v.Stats.SessionCount = int64(len(v.seenKeys[RootBySession]))
	for root, keys := range v.seenKeys {
		if root == RootBySession {
			continue
		}
		if v.Stats.ViewCounts == nil {
			v.Stats.ViewCounts = make(map[RootView]int64)
		}
		v.Stats.ViewCounts[root] = int64(len(keys))
	}
	_ = duringBackfill
}
