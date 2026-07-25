package projection

import "scrinium.dev/domain"

// Config is the build-time configuration for Build. It is the
// projection-layer view of the host's settings: the composition root
// (e.g. internal/assembly) maps its own config representation onto
// this struct. Unlike a serialization config it carries no tags and
// no store/policy concerns — only what the View and FSOps need.
//
// Zero values mean "engine default": an empty RootView selects the
// first available root, a zero ScratchQuota is unlimited, zero
// DefaultUID/GID fall back to the running process.
type Config struct {
	// RootView selects the tree presented at the mount root by its
	// name; an empty RootView selects the engine's default root. Valid
	// names are the intrinsic views plus whatever the active extensions
	// provide (see ProvidedViews).
	RootView string

	// ProvidedViews are the views the host's extensions back, collected
	// at the composition root from each extension's ViewProvider
	// capability (ADR-98) and forwarded here verbatim. The projection
	// unions them with its intrinsic views and materialises each from the
	// supplied Path/CountKey without knowing the view's concept — it
	// names no extension view (ADR-89, Principle 10). Empty ⇒ only the
	// intrinsic views exist.
	ProvidedViews []ProvidedView

	// ByPathFallback controls what the orphaning root view does with
	// manifests it cannot place: "orphaned" or "synthetic".
	ByPathFallback string

	// Editing controls in-place edits: "off" (strict CAS), "on", or
	// "custom" (consult the Allow* flags). Empty = off.
	Editing        string
	AllowRename    *bool
	AllowSetattr   *bool
	AllowTruncate  *bool
	AllowAppend    *bool
	AllowDirRename *bool

	// ScratchDir is the staging area for in-flight FSOps writes,
	// already resolved by the caller (empty = none; the engine
	// tolerates this for read-mostly use). Ignored when ReadOnly.
	ScratchDir string

	// ScratchQuota caps the scratch area in bytes. Zero = unlimited.
	ScratchQuota int64

	// ReadOnly disables writes through FSOps.
	ReadOnly bool

	// Default POSIX bits for artifacts written without explicit ones.
	// Zero UID/GID fall back to the running process.
	DefaultMode uint32
	DefaultUID  uint32
	DefaultGID  uint32

	// MountSession tags writes from this projection instance.
	MountSession domain.SessionID

	// SyncSource is the pull half of the synchronization seam (ADR-107): the
	// backend's change-sequence source, adapted by the composition root from
	// the index's SyncSource capability. nil ⇒ the View is a snapshot as of
	// Build and does not observe other writers (INV-107-6).
	SyncSource TokenSource

	// SyncWaiter is the optional push half (ADR-107): when set, the View can
	// block for changes instead of polling. nil ⇒ the View refreshes lazily
	// on read.
	SyncWaiter Waiter
}

// ProvidedView is the projection-layer description of one view an
// extension backs (ADR-98). The composition root adapts each
// engine-level provided view onto this type and lists them in
// Config.ProvidedViews; Build forwards them to the read-side View. The
// type carries the view's whole layout as opaque functions so the
// projection materialises the tree without knowing its addressing
// scheme — it never names the view (ADR-89, Principle 10). It is a
// distinct type from the engine's so the projection takes no dependency
// on the extension/engine layer.
type ProvidedView struct {
	// Root is the view's name (its RootView). Unique across the set.
	Root string

	// Path maps a manifest to its placement path in this tree; ("", false)
	// when the manifest is opaque to the view (orphaned when Orphans).
	Path func(m domain.Manifest) (path string, ok bool)

	// Collide marks a tree whose Path keys are not artifact-unique, so the
	// View runs collision arbitration for it (freshest wins).
	Collide bool

	// Orphans routes a Path()=!ok manifest to the orphan tree; otherwise a
	// miss is skipped.
	Orphans bool

	// CountKey, when non-nil, supplies the view's distinct-cardinality key
	// so the View can maintain the view's count. nil ⇒ not counted.
	CountKey func(m domain.Manifest) (key string, ok bool)

	// IsDir, when non-nil, declares that a manifest describes a directory in
	// this tree (ADR-116) — needed for a captured EMPTY directory, which has no
	// children to speak for it. It does not decide the node's kind: a node is a
	// directory if it has children or was declared one.
	IsDir func(m domain.Manifest) bool
}
