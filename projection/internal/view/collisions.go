package view

import (
	"sort"

	"scrinium.dev/domain"
	"scrinium.dev/event"
)

// applyCollisionInsert handles arbitration for a collidable tree
// (path keys not artifact-unique, e.g. by-path), keyed by root. Three
// cases:
//
//  1. path is unclaimed → insert as winner.
//  2. path is claimed; newcomer is fresher → newcomer wins, previous
//     owner becomes loser, event.EventPathCollision emitted.
//  3. path is claimed; newcomer is older → newcomer joins the losers
//     list, no node, event.EventPathCollision emitted.
//
// The caller records rec.paths[root] before calling (a loser still has
// its path recorded for Remove). The "fresher" rule is CreatedAt
// descending; on tie the lexicographically larger ArtifactID wins.
//
// Collision events are appended to *events rather than published inline:
// applyCollisionInsert runs under the View write lock (via indexArtifact
// from Add/Move/applyDelta), and the synchronous bus would deadlock a
// subscriber that reads the View. The caller flushes *events with emit
// after releasing the lock.
func (v *View) applyCollisionInsert(root RootView, path string, m domain.Manifest, rec *artifactRecord, declaredDir bool, events *[]event.Event) {
	owners := v.pathOwner[root]
	tree := v.trees[root]

	currentOwner, claimed := owners[path]
	if !claimed {
		owners[path] = m.ArtifactID
		v.insertArtifact(tree, path, m, declaredDir)
		return
	}

	currentRec := v.artifacts[currentOwner]
	if currentRec == nil {
		// Should not happen: owner without artifact record.
		// Recover by treating as unclaimed.
		owners[path] = m.ArtifactID
		v.insertArtifact(tree, path, m, declaredDir)
		return
	}

	if isFresherWinner(m, currentRec.manifest) {
		// Newcomer wins. Demote previous owner. The node is NOT removed and
		// re-created: it may have children (a directory an artifact occupies,
		// an archive with members under its path), and removing it would orphan
		// them. insertArtifact replaces the artifact facet in place (ADR-116).
		owners[path] = m.ArtifactID
		v.insertArtifact(tree, path, m, declaredDir)
		v.pushLoser(root, path, currentRec.manifest)
		*events = append(*events, event.Event{
			Type: event.EventPathCollision,
			Payload: event.PathCollisionPayload{
				Path:   path,
				Winner: m.ArtifactID,
				Loser:  currentOwner,
			},
		})
		v.Stats.CollisionCount++
		return
	}

	// Newcomer loses.
	v.pushLoser(root, path, m)
	*events = append(*events, event.Event{
		Type: event.EventPathCollision,
		Payload: event.PathCollisionPayload{
			Path:   path,
			Winner: currentOwner,
			Loser:  m.ArtifactID,
		},
	})
	v.Stats.CollisionCount++
}

// isFresherWinner reports whether candidate beats incumbent for
// the by-path slot. CreatedAt later wins; on tie lexicographically
// larger ArtifactID wins.
func isFresherWinner(candidate, incumbent domain.Manifest) bool {
	if candidate.CreatedAt.After(incumbent.CreatedAt) {
		return true
	}
	if candidate.CreatedAt.Equal(incumbent.CreatedAt) {
		return string(candidate.ArtifactID) > string(incumbent.ArtifactID)
	}
	return false
}

// pushLoser inserts an entry into pathLosers[root][path], keeping the
// slice sorted by CreatedAt descending (and ArtifactID descending on
// tie). The inner slice is allocated lazily.
func (v *View) pushLoser(root RootView, path string, m domain.Manifest) {
	byPath := v.pathLosers[root]
	losers := byPath[path]
	entry := loserEntry{id: m.ArtifactID, createdAt: m.CreatedAt}
	idx := sort.Search(len(losers), func(i int) bool {
		// sort descending: we want the position of the first entry
		// "older or equal" to the new one. That position is the
		// insertion point.
		l := losers[i]
		if l.createdAt.After(entry.createdAt) {
			return false
		}
		if l.createdAt.Equal(entry.createdAt) {
			return string(l.id) <= string(entry.id)
		}
		return true
	})
	losers = append(losers, loserEntry{})
	copy(losers[idx+1:], losers[idx:])
	losers[idx] = entry
	byPath[path] = losers
}

// removeLoser drops the entry with the given id from
// pathLosers[root][path]; no-op if not present.
func (v *View) removeLoser(root RootView, path string, id domain.ArtifactID) {
	byPath := v.pathLosers[root]
	losers := byPath[path]
	for i, l := range losers {
		if l.id == id {
			byPath[path] = append(losers[:i], losers[i+1:]...)
			if len(byPath[path]) == 0 {
				delete(byPath, path)
			}
			return
		}
	}
}
