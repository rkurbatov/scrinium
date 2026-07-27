package customindex

import (
	"encoding/json"

	"scrinium.dev/domain"
)

// ViewProvider is the optional capability by which an index extension
// contributes one or more named projection views (ADR-98). It is
// discovered by type-assertion at the assembly layer — exactly like
// Resolver / Indexer / Accessor — and is asserted off the registered
// CustomIndex. The projection library never sees this type: discovery
// lives at the composition root, which alone may import both the
// extensions and the projection (ADR-89; Principle 12 — the projection
// stays acyclic and extension-agnostic).
type ViewProvider interface {
	// ProvidedViews returns the view(s) this extension backs. Each names
	// a RootView and supplies the seam pieces the projection needs to
	// materialise that tree. Empty slice = the extension backs no view
	// (it occupies only the index axis).
	ProvidedViews() []ProvidedView
}

// ProvidedView describes one projection view an extension backs (ADR-98).
// The composition root collects these across installed extensions, unions
// them with the native (intrinsic) views, and feeds the projection. Root
// must be unique across installed extensions; the root rejects a collision.
//
// The extension owns the view's full layout: Path maps a manifest directly
// to its placement in the tree, so the projection never knows the view's
// addressing scheme (a verbatim logical path for by-path, an id-sharded
// path under a registry label for by-namespace, …). This keeps the
// projection extension-agnostic (ADR-89, Principle 12): it materialises
// trees from opaque Path/CountKey functions without naming any view.
type ProvidedView struct {
	// Root is the RootView this extension backs, e.g. "by-path" (fspath)
	// or "by-namespace" (namespace). Unique across installed extensions.
	Root string

	// Path maps a manifest to its full placement path in this tree during
	// backfill: (path, true) admits the artifact, ("", false) when the
	// manifest is opaque to this view (routed to the orphan tree when
	// Orphans is set). Pure — the same manifest must always produce the
	// same result. The extension applies whatever layout it wants here
	// (verbatim key, label + id-shard, …); the projection does not look
	// inside.
	Path func(m domain.Manifest) (path string, ok bool)

	// Collide marks a tree whose Path keys are NOT artifact-unique (e.g.
	// by-path's logical paths): the projection runs collision arbitration
	// (freshest CreatedAt wins, tie → larger ArtifactID) for it. id-shaped
	// layouts (by-namespace) leave this false.
	Collide bool

	// Orphans routes a Path()=!ok manifest to the orphan tree (by-path);
	// other views simply skip a miss.
	Orphans bool

	// IsDir, when non-nil, declares that a manifest describes a DIRECTORY in
	// this tree (ADR-116). It does not decide what kind of node the path
	// becomes — that stays a property of the tree, which reads "a node is a
	// directory if it has children or was declared one" — it reports what the
	// capture said about this artifact.
	//
	// It exists for the one case the tree cannot infer: a captured EMPTY
	// directory has no children, so nothing but the declaration distinguishes
	// it from a file. An artifact that both carries bytes and has descendants
	// (an archive whose members hang under its path) needs no declaration —
	// its children speak for themselves, and it keeps its artifact facet
	// either way.
	//
	// nil ⇒ nothing is declared; a node is a directory only if it has
	// children, which is the behaviour every view had before this field.
	IsDir func(m domain.Manifest) bool

	// CountKey, when non-nil, supplies this view's distinct-cardinality
	// key (e.g. the namespace label) so the projection can maintain the
	// view's count without knowing the concept. nil ⇒ not counted.
	CountKey func(m domain.Manifest) (key string, ok bool)

	// Metadata optionally provides bulk ext-block lookup so the backfill
	// fetches an artifact's custom-index block in one round-trip instead
	// of re-reading the manifest. Satisfied by an index that already
	// stores ext blocks (as fspathindex does). nil ⇒ the backfill reads via
	// the store. A miss falls back transparently — performance hint, not
	// a correctness requirement.
	Metadata interface {
		Metadata(id domain.ArtifactID) (json.RawMessage, bool, error)
	}
}
