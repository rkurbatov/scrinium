package view

import (
	"os"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/event"
	"scrinium.dev/projection/pathx"
)

// Close marks the View closed. Idempotent. Subsequent reads
// return os.ErrClosed.
func (v *View) Close() error {
	if v.closed.Swap(true) {
		return nil // already closed
	}
	v.stopWatcher()
	return nil
}

// Add registers a new manifest, mirroring backfill's per-manifest
// path. Used by FSOps after Store.Put. Concurrent with reads;
// holds the write lock.
//
// Returns os.ErrClosed if the View is closed. Otherwise nil —
// classification cannot fail for a valid manifest (the input
// itself is what the source produced).
func (v *View) Add(m domain.Manifest) error {
	if v.closed.Load() {
		return os.ErrClosed
	}
	if !v.passesFilter(m) {
		return nil
	}
	// emit runs after v.mu is released (LIFO defers: the Unlock below is
	// registered later, so it runs first). Publishing the collision events
	// outside the lock keeps a synchronous subscriber that reads the View
	// from deadlocking.
	var events []event.Event
	defer func() { v.emit(events) }()
	v.mu.Lock()
	defer v.mu.Unlock()

	// Idempotent: an Add for an already-known ArtifactID is a no-op.
	if _, exists := v.artifacts[m.ArtifactID]; exists {
		return nil
	}
	v.indexArtifact(m, false, &events)
	return nil
}

// Remove drops every entry of the artifact from every tree.
// Handles by-path collision re-election when the removed
// artifact was the current owner of a path.
//
// Idempotent: Remove for an unknown ArtifactID is a no-op.
func (v *View) Remove(id domain.ArtifactID) error {
	if v.closed.Load() {
		return os.ErrClosed
	}
	v.mu.Lock()
	defer v.mu.Unlock()

	rec, ok := v.artifacts[id]
	if !ok {
		return nil
	}
	v.removeArtifactFromTrees(id, rec)
	return nil
}

// removeArtifactFromTrees does the actual fan-out delete. Caller
// holds the write lock.
func (v *View) removeArtifactFromTrees(id domain.ArtifactID, rec *artifactRecord) {
	orphaned := rec.paths[RootByOrphaned] != ""
	for root, path := range rec.paths {
		if path == "" {
			continue
		}
		if v.collide[root] {
			v.removeFromCollisionTree(root, id, rec)
		} else {
			v.removeFile(v.trees[root], path)
		}
	}

	delete(v.artifacts, id)
	v.Stats.TotalNodes--
	v.Stats.TotalBytes -= rec.manifest.OriginalSize
	if orphaned {
		v.Stats.OrphanedCount--
	}
	// SessionCount and ViewCounts: we do not decrement (see seenKeys —
	// distinct keys are tracked monotonically). Those distinct-key
	// counters stay monotonic across the View's lifetime — callers use
	// them for pacing, not for exact accounting.
}

// removeFromCollisionTree drops an artifact from a collidable tree
// (keyed by root). If it was the current owner of a path, the freshest
// loser (if any) is promoted to owner.
func (v *View) removeFromCollisionTree(root RootView, id domain.ArtifactID, rec *artifactRecord) {
	path := rec.paths[root]
	owners := v.pathOwner[root]
	tree := v.trees[root]
	owner, claimed := owners[path]
	if claimed && owner == id {
		// Drop the artifact and try to promote a loser. A node with children
		// survives as a directory instead of being deleted (ADR-116): the path
		// is a directory regardless of who owns its bytes, and removing it
		// would orphan the subtree.
		if !v.detachArtifact(tree, path) {
			v.removeFile(tree, path)
		}
		delete(owners, path)
		losers := v.pathLosers[root][path]
		if len(losers) > 0 {
			promoted := losers[0]
			v.pathLosers[root][path] = losers[1:]
			if len(v.pathLosers[root][path]) == 0 {
				delete(v.pathLosers[root], path)
			}
			if promotedRec, ok := v.artifacts[promoted.id]; ok {
				owners[path] = promoted.id
				v.insertFile(tree, path, promotedRec.manifest)
			}
		}
	} else {
		// Removed artifact was a loser, not owner.
		v.removeLoser(root, path, id)
	}
}

// Move atomically replaces an old artifact with a new one — used
// by FSOps to emulate rename. The old artifact's entry in the
// root view is dropped (with collision re-election), and the new
// manifest is added through the standard Add path.
//
// oldPath/newPath are passed for documentation and future use
// (FSOps wants to log the user-level rename); the actual location
// in the root view comes from the new manifest's provided-view
// Path() result.
func (v *View) Move(oldPath, newPath string, m domain.Manifest) error {
	if v.closed.Load() {
		return os.ErrClosed
	}
	// See Add: collision events are flushed after the lock is released.
	var events []event.Event
	defer func() { v.emit(events) }()
	v.mu.Lock()
	defer v.mu.Unlock()

	// We do not require oldPath to currently exist — the FSOps
	// orchestration may have already done the Store.Delete and
	// only failed to find the manifest. Move is idempotent on the
	// "old" side: remove if present, add new.

	// Find the old artifact by oldPath in the configured root view;
	// if found, remove. Reading a nil inner map (no such view active)
	// yields ok=false.
	if oldOwner, ok := v.pathOwner[v.opts.rootView][oldPath]; ok {
		if rec, found := v.artifacts[oldOwner]; found {
			v.removeArtifactFromTrees(oldOwner, rec)
		}
	}

	// Add the new manifest, applying filter.
	if !v.passesFilter(m) {
		return nil
	}
	if _, exists := v.artifacts[m.ArtifactID]; exists {
		return nil
	}
	v.indexArtifact(m, false, &events)
	_ = newPath
	return nil
}

// --- Internal helpers ---

// insertFile places a plain leaf: the shape the intrinsic id-shaped trees
// (by-artifact, by-date, by-session) always want, where a path can never be a
// directory an artifact occupies. Anything that can land on a directory goes
// through insertArtifact.
//
// FilesystemFacet carries only the schema-agnostic fields: Name, Path, Size,
// ModTime, IsDir. POSIX attributes (mode/uid/gid) live in vfsmeta.FileSystem
// inside Manifest.Ext and are materialised by FSOps at the transport boundary.
// ModTime is seeded from m.CreatedAt; FSOps overrides with vfsmeta.ModTime when
// non-zero.
func (v *View) insertFile(tree map[string]*viewNode, path string, m domain.Manifest) {
	v.insertArtifact(tree, path, m, false)
}

// insertArtifact places an artifact at path WITHOUT destroying what is already
// there (ADR-116). It replaces insertFile, which built a leaf and overwrote the
// node — losing the children of a directory an artifact happens to occupy.
//
// Three facts about a node are now independent: it may carry an artifact, it may
// have children, and it is a DIRECTORY if it has children or the view's provider
// declared it one. "Directory" is the answer the filesystem projection must give
// for a path, not a claim that no artifact stands behind it.
//
// declaredDir comes from the provided view (customindex.ProvidedView.IsDir) and
// matters for exactly one case: a captured empty directory, which has no
// children to speak for it.
func (v *View) insertArtifact(tree map[string]*viewNode, path string, m domain.Manifest, declaredDir bool) {
	v.ensureDirs(tree, pathx.Parent(path))
	name := pathx.LastSegment(path)

	if existing, ok := tree[path]; ok {
		// Somebody is already here: an intermediate directory synthesised from a
		// descendant's path, or a previous artifact. Keep the structure and
		// attach the artifact to it.
		existing.artifact = artifactFacetFrom(m)
		existing.fs.IsDir = existing.fs.IsDir || len(existing.children) > 0 || declaredDir
		if !existing.fs.IsDir {
			existing.fs.Size = m.OriginalSize
		}
		existing.fs.ModTime = m.CreatedAt
		return
	}

	tree[path] = &viewNode{
		fs: FilesystemFacet{
			Name:    name,
			Path:    path,
			IsDir:   declaredDir,
			Size:    m.OriginalSize,
			ModTime: m.CreatedAt,
		},
		artifact: artifactFacetFrom(m),
	}
	if pn, ok := tree[pathx.Parent(path)]; ok {
		pn.children = insertSorted(pn.children, name)
	}
}

// detachArtifact strips the artifact from a node but keeps the node when it
// still has children (ADR-116): the path is a directory regardless of who owns
// its bytes, and deleting it would orphan the subtree. Reports whether the node
// survived; a childless node is left to removeFile.
func (v *View) detachArtifact(tree map[string]*viewNode, path string) bool {
	n, ok := tree[path]
	if !ok || len(n.children) == 0 {
		return false
	}
	n.artifact = nil
	n.fs.IsDir = true
	n.fs.Size = 0
	return true
}

// removeFile deletes the node at path. Empty parent directories
// are recursively pruned to keep List tidy. The tree root ""
// always survives.
func (v *View) removeFile(tree map[string]*viewNode, path string) {
	if _, ok := tree[path]; !ok {
		return
	}
	delete(tree, path)
	parent := pathx.Parent(path)
	name := pathx.LastSegment(path)
	if pn, ok := tree[parent]; ok {
		pn.children = removeSorted(pn.children, name)
		// Prune empty virtual directory cascading upwards.
		for parent != "" && len(pn.children) == 0 && pn.artifact == nil {
			delete(tree, parent)
			grand := pathx.Parent(parent)
			gname := pathx.LastSegment(parent)
			parent = grand
			pn, ok = tree[grand]
			if !ok {
				break
			}
			pn.children = removeSorted(pn.children, gname)
		}
	}
}

// ensureDirs walks path top-down and inserts virtual directory
// nodes for every component that does not yet exist.
func (v *View) ensureDirs(tree map[string]*viewNode, path string) {
	if path == "" {
		return
	}
	segments := strings.Split(path, "/")
	cur := ""
	for i, seg := range segments {
		next := seg
		if cur != "" {
			next = cur + "/" + seg
		}
		if _, ok := tree[next]; !ok {
			tree[next] = newDirNode(seg, next, v.CreatedAt)
			parent := ""
			if i > 0 {
				parent = cur
			}
			if pn, ok := tree[parent]; ok {
				pn.children = insertSorted(pn.children, seg)
			}
		}
		cur = next
	}
}
