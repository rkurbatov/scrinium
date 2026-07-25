package projection

import (
	"scrinium.dev/projection/internal/source"
	"scrinium.dev/projection/internal/view"
)

// Re-exported read-model types. External code (daemons, CLI flag
// parsers, the stats renderer) depends on these projection-level
// names; the view package that defines them is a projection internal
// and is never named outside the projection tree.
type (
	// RootView selects which materialised tree backs a lookup. The
	// intrinsic trees are by-date, by-session, by-artifact, and
	// orphaned; extensions contribute further roots at runtime.
	RootView = view.RootView

	// Node is one position in a materialised tree: its filesystem facet, the
	// artifact behind it when there is one (nil for a directory the tree
	// derived from paths rather than captured), and its storage facet under a
	// multistore source.
	Node = view.Node

	// FilesystemFacet is the schema-agnostic half of a node — name, path,
	// directory flag, size, mtime. POSIX attributes live in the vfsmeta
	// schema, not here.
	FilesystemFacet = view.FilesystemFacet

	// ArtifactFacet is the artifact behind a node, Ext block included.
	ArtifactFacet = view.ArtifactFacet

	// Seq is a fallible sequence of nodes (the iter.Seq2 pattern) — what a tree
	// walk returns. Re-exported because a consumer outside the projection tree
	// (the Extractor, ADR-97) walks a resolved view and cannot name the
	// internal package that defines it.
	Seq = view.Seq

	// Stats is a snapshot of projection counters.
	Stats = view.Stats

	// SearchResult is one hit from Reader.Search.
	SearchResult = view.SearchResult

	// RelatedArtifact is one entry from Reader.RelatedByBlobRef.
	RelatedArtifact = view.RelatedArtifact

	// Locations bundles every tree-placement of one artifact.
	Locations = view.Locations

	// TokenSource is the pull half of the synchronization seam (ADR-107): the
	// backend's current change-sequence, adapted from the index's SyncSource
	// capability by the composition root. A nil SyncSource in Config gives the
	// View snapshot semantics.
	TokenSource = source.TokenSource

	// Waiter is the optional push half (ADR-107), adapted from the index's
	// SyncWaiter. With it the View can refresh eagerly instead of polling.
	Waiter = source.Waiter

	// Delta is a batch of resolved manifest changes for incremental
	// convergence (ADR-107), returned by a DeltaSource. The composition root
	// builds it from the index's digest-level Since plus its manifest
	// resolver; the projection re-exports the type so the assembler (outside
	// the projection's internal tree) can name it.
	Delta = source.Delta

	// DeltaSource is the incremental pull capability (ADR-107): a TokenSource
	// that also enumerates changed manifests since a cursor. When the wired
	// SyncSource implements it, the View upserts just the changes instead of
	// re-walking; otherwise it falls back to a full re-derive.
	DeltaSource = source.DeltaSource
)

// RootView values, re-exported so flag parsers and configs can name the
// intrinsic enum without reaching into the view package. Extension-
// contributed roots (by-path, by-namespace, …) are not re-exported — the
// projection names none of them; they are discovered at runtime.
const (
	RootBySession  = view.RootBySession
	RootByDate     = view.RootByDate
	RootByArtifact = view.RootByArtifact
	RootByOrphaned = view.RootByOrphaned
)
