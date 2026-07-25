package fspath

import (
	"syscall"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/engine/customindex"
)

// ProvidedViews implements customindex.ViewProvider (ADR-98): the fspath
// extension backs the by-path view. The resolver is vfsmeta.Resolver
// (manifest → path from the vfsmeta payload); the registered index itself
// doubles as the bulk Metadata source the backfill consults. fspath thus
// occupies the view capability alongside the index axis (CustomIndex with
// the Indexer + Accessor capabilities) — which is what "its view together
// with the index" means.
//
// The Root string must match the projection's RootView name for by-path;
// it is kept as a literal here so the extension takes no dependency on the
// projection library (the dependency only ever runs the other way, and
// projection must not import extensions — ADR-89).
func (e *CustomIndex) ProvidedViews() []customindex.ProvidedView {
	return []customindex.ProvidedView{{
		Root:     "by-path",
		Path:     vfsmeta.Resolver,
		Collide:  true,
		Orphans:  true,
		IsDir:    capturedAsDir,
		Metadata: e,
	}}
}

// capturedAsDir reports whether the capture declared this artifact a directory
// (ADR-114/116). The type bits of the captured POSIX mode are the answer, and
// this is the only place they are read on the projection's behalf: the view
// stays schema-agnostic and receives a plain yes or no from its provider.
//
// An artifact with no filesystem block, or one whose mode carries no type bits,
// declares nothing — such a node is a directory only if it has children.
func capturedAsDir(m domain.Manifest) bool {
	fs, ok, err := vfsmeta.Decode(m.Ext)
	if err != nil || !ok {
		return false
	}
	return fs.Mode&syscall.S_IFMT == syscall.S_IFDIR
}

// Compile-time conformance: fspathindex backs a projection view.
var _ customindex.ViewProvider = (*CustomIndex)(nil)
