package view

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/event"
	"scrinium.dev/projection/internal/source"
)

type View struct {
	// Public, read-only after New returns.
	Source    source.Kind
	CreatedAt time.Time
	Stats     Stats

	// Internal state, guarded by mu.
	mu sync.RWMutex

	// trees holds one tree per active RootView (the intrinsic core
	// set plus every extension-provided view, see defs). Each tree
	// maps a full path (no leading slash, "" is the tree root) to
	// the canonical viewNode. RootByOrphaned is always present as the
	// sink for artifacts an orphaning view (by-path) could not place.
	trees map[RootView]map[string]*viewNode

	// defs is the active view set: the intrinsic definitions the View
	// builds from core manifest fields, augmented by the views active
	// extensions provide (ADR-98). indexArtifact iterates it; the View
	// attaches no meaning to any individual root.
	defs []viewDef

	// Per-artifact tracking. Used by Remove/Move to fan out a
	// deletion or move across every tree without re-deriving the
	// paths. paths maps each RootView the artifact appears in to its
	// path there (including RootByOrphaned when orphaned).
	artifacts map[domain.ArtifactID]*artifactRecord

	// Path-collision bookkeeping, keyed by RootView then path, for
	// the collidable views (those whose path keys are not artifact-
	// unique — by-path). pathOwner records the ArtifactID currently
	// holding each path; pathLosers stores every other ArtifactID
	// claiming it, sorted by CreatedAt descending so the freshest
	// loser is at index 0. On Remove of the owner we promote
	// pathLosers[root][path][0]; on a new insert against an existing
	// owner we compare CreatedAt to decide winner vs loser. collide
	// marks which roots run this arbitration.
	pathOwner  map[RootView]map[string]domain.ArtifactID
	pathLosers map[RootView]map[string][]loserEntry
	collide    map[RootView]bool

	// seenKeys tracks, per counting view, the distinct cardinality
	// keys observed (session ids, nsids), so Stats counters stay
	// accurate without the View knowing the concept. Monotonic:
	// entries are never removed (matching the existing Stats
	// semantics for SessionCount/NamespaceCount).
	seenKeys map[RootView]map[string]struct{}

	src    source.Provider
	bus    event.EventBus // nil = events not published
	opts   viewOptions
	closed atomic.Bool

	// Synchronization seam (ADR-107). tokenSrc is the backend change-sequence
	// source; nil ⇒ snapshot (the View does not track other writers). waiter
	// is the optional push source for eager refresh. lastToken is the Token
	// observed at the last (re)build — a lower bound on what the trees
	// reflect, compared on read to decide staleness.
	tokenSrc  source.TokenSource
	waiter    source.Waiter
	lastToken atomic.Uint64

	// delta is the optional incremental capability (ADR-107): when the wired
	// tokenSrc also implements DeltaSource, a stale read upserts just the
	// changed manifests instead of re-walking the whole source. nil ⇒ every
	// refresh is a full re-derive.
	delta source.DeltaSource

	// refreshMu serialises lazy rebuilds (ADR-107): a stale read triggers at
	// most one re-backfill at a time; concurrent triggers collapse onto it.
	refreshMu sync.Mutex

	// Eager-refresh watcher (ADR-107), present only when a Waiter is wired.
	// watchCancel stops the Wait loop; watchDone closes when it has exited, so
	// Close blocks until the goroutine is gone.
	watchCancel context.CancelFunc
	watchDone   chan struct{}
}

// viewDef describes one logical tree. Intrinsic defs (by-artifact,
// by-date, by-session) are built by the View from core manifest fields;
// extension-provided defs (by-path, by-namespace, …) augment the set via
// ProvidedView. The View is agnostic to what each def means.
type viewDef struct {
	root RootView
	// path maps a manifest to its placement in this tree. ok=false (or a
	// nil path) means the manifest has no place here: it is routed to the
	// orphan tree when orphans is set, otherwise skipped.
	path func(domain.Manifest) (string, bool)
	// collide marks a tree whose path keys are NOT artifact-unique (the
	// by-path logical namespace): inserts run collision arbitration.
	collide bool
	// orphans routes a path()=!ok manifest to the orphan tree (or a
	// synthetic path under FallbackSynthetic); other defs skip a miss.
	orphans bool
	// countKey, when non-nil, supplies this view's distinct-cardinality
	// key so the View maintains the matching Stats counter.
	countKey func(domain.Manifest) (string, bool)
	// isDir, when non-nil, declares a manifest to be a directory in this
	// tree (ADR-116): the half the tree cannot infer, since a captured
	// empty directory has no children.
	isDir func(domain.Manifest) bool
}

// viewNode is the internal node representation. The public Node
// is built from these fields when read.
type viewNode struct {
	fs       FilesystemFacet
	artifact *ArtifactFacet // nil for virtual directories
	children []string       // sorted last-segment names; nil for files
}

// artifactRecord is the cross-tree record of an artifact: the
// manifest plus the path under which it appears in every tree.
// paths maps each RootView the artifact occupies to its path there;
// a root absent from the map means the artifact is not in that tree
// (e.g. no by-path entry when the resolver missed and it was
// orphaned — then RootByOrphaned is present instead).
type artifactRecord struct {
	manifest domain.Manifest
	paths    map[RootView]string
}

// loserEntry records a losing artifact in a path collision —
// the ArtifactID and its CreatedAt for re-election ordering.
type loserEntry struct {
	id        domain.ArtifactID
	createdAt time.Time
}
