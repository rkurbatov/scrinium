package view

import (
	"context"
	"fmt"
	"sort"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/event"
	"scrinium.dev/projection/internal/source"
)

// --- Construction ---

// New constructs a View by walking source and populating
// every tree. Backfill is synchronous: New returns only after
// the source has been fully traversed.
//
// Default options:
//   - root view: the first available root (intrinsic or provided)
//   - fallback: FallbackOrphaned
//   - filter: empty
//
// EventBus is optional via WithEventBus; without it the View
// silently produces no events.
func New(ctx context.Context, src source.Provider, opts ...Option) (*View, error) {
	if src == nil {
		return nil, fmt.Errorf("projection.New: source is nil")
	}

	o := viewOptions{
		fallback: FallbackOrphaned,
	}
	for _, opt := range opts {
		opt(&o)
	}

	v := &View{
		Source:    source.KindStore,
		CreatedAt: time.Now().UTC(),

		src:      src,
		bus:      o.bus,
		opts:     o,
		tokenSrc: o.tokenSrc,
		waiter:   o.waiter,
	}

	// Incremental convergence (ADR-107): if the wired token source can also
	// enumerate changes, a stale read upserts just those instead of re-walking.
	if ds, ok := o.tokenSrc.(source.DeltaSource); ok {
		v.delta = ds
	}

	// Build the active view set (intrinsic core + extension-provided) and
	// initialise the per-tree state (trees, records, collision/count books).
	v.defs = v.buildViewDefs()
	v.initTrees()

	// Resolve the root view from the initialised set — the tree keys are the
	// active roots. Unset → first available; a named root that does not exist
	// is an error. The projection itself names none of the roots.
	if v.opts.rootView == "" {
		rs := make([]RootView, 0, len(v.trees))
		for r := range v.trees {
			rs = append(rs, r)
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
		if len(rs) > 0 {
			v.opts.rootView = rs[0]
		}
	} else if _, ok := v.trees[v.opts.rootView]; !ok {
		return nil, fmt.Errorf("projection.New: root view %q is not available", v.opts.rootView)
	}

	// Snapshot the backend's change-sequence BEFORE backfill so lastToken is a
	// lower bound on what the trees reflect: a writer racing the walk only
	// pushes Token past lastToken, which a later read treats as staleness and
	// re-derives (ADR-107). Reading after backfill could record a token for a
	// write the walk missed, hiding it. Snapshot mode (no source) leaves it 0.
	if v.tokenSrc != nil {
		tok, err := v.tokenSrc.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("projection.New: read sync token: %w", err)
		}
		v.lastToken.Store(tok)
	}

	startedAt := time.Now()
	if err := v.backfill(ctx); err != nil {
		return nil, err
	}
	v.publish(event.EventViewRebuilt, event.RebuiltPayload{
		Duration:  time.Since(startedAt),
		NodeCount: v.Stats.TotalNodes,
	})

	// Eager refresh: if a Waiter is wired, watch the backend in the background
	// and converge without waiting for a read (ADR-107). No-op otherwise.
	v.startWatcher()

	return v, nil
}

// buildViewDefs assembles the active view set: the intrinsic core
// definitions (derived from core manifest fields) augmented by the views
// active extensions provide via WithProvidedViews (ADR-98). The View is
// agnostic to what each provided view computes — by-path (fspath) and
// by-namespace (the namespace extension) arrive through the provided rail
// like any other, each carrying its own layout, collision and count policy.
func (v *View) buildViewDefs() []viewDef {
	defs := []viewDef{
		{root: RootByArtifact, path: pathByArtifactDef},
		{root: RootByDate, path: pathByDateDef},
		{root: RootBySession, path: pathBySessionDef, countKey: sessionCountKey},
	}
	for _, p := range v.opts.provided {
		defs = append(defs, viewDef{
			root:     p.Root,
			path:     p.Path,
			collide:  p.Collide,
			orphans:  p.Orphans,
			countKey: p.CountKey,
			isDir:    p.IsDir,
		})
	}
	return defs
}

// initTrees initialises the rebuildable per-tree state from v.defs: an empty
// tree per active root (RootByOrphaned plus each def's root), with the
// collision and counting bookkeeping each def needs. New calls it once at
// construction; a lazy refresh calls it on a shadow View before re-walking
// the source (ADR-107). It assumes v.defs is already built.
func (v *View) initTrees() {
	v.trees = make(map[RootView]map[string]*viewNode)
	v.artifacts = make(map[domain.ArtifactID]*artifactRecord)
	v.pathOwner = make(map[RootView]map[string]domain.ArtifactID)
	v.pathLosers = make(map[RootView]map[string][]loserEntry)
	v.collide = make(map[RootView]bool)
	v.seenKeys = make(map[RootView]map[string]struct{})
	v.Stats = Stats{}

	roots := map[RootView]bool{RootByOrphaned: true}
	for _, d := range v.defs {
		roots[d.root] = true
		if d.collide {
			v.collide[d.root] = true
			v.pathOwner[d.root] = make(map[string]domain.ArtifactID)
			v.pathLosers[d.root] = make(map[string][]loserEntry)
		}
		if d.countKey != nil {
			v.seenKeys[d.root] = make(map[string]struct{})
		}
	}
	for r := range roots {
		v.trees[r] = map[string]*viewNode{"": newDirNode("", "", v.CreatedAt)}
	}
}

// Intrinsic def path/count functions. These derive placement purely from
// core manifest fields, so they carry no extension knowledge.

func pathByArtifactDef(m domain.Manifest) (string, bool) { return byArtifactPath(m.ArtifactID), true }

func pathByDateDef(m domain.Manifest) (string, bool) { return byDatePath(m), true }

func pathBySessionDef(m domain.Manifest) (string, bool) {
	if m.SessionID == "" {
		return "", false
	}
	return bySessionPath(m), true
}

func sessionCountKey(m domain.Manifest) (string, bool) {
	if m.SessionID == "" {
		return "", false
	}
	return string(m.SessionID), true
}
