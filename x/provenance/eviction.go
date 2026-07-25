package provenance

import (
	"context"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
)

// The index side of eviction (ADR-113): recognising a receipt, and computing
// EFFECTIVE reproducibility once sources start disappearing.

// Receipt returns the receipt explaining this artifact's eviction, if any. A
// traversal that hit an unresolvable handle asks this: the answer tells a
// deliberate decision from data loss.
func (e *Index) Receipt(ctx context.Context, id domain.ArtifactID) (domain.ArtifactID, bool, error) {
	if err := e.ready(); err != nil {
		return "", false, err
	}
	var receipt domain.ArtifactID
	err := e.sub.Scan(tableByParent, join(string(id), e.evictRel())+sep, func(key string, _ []byte) error {
		if _, _, child, ok := split3(key); ok {
			receipt = domain.ArtifactID(child)
		}
		return customindex.ErrStopScan
	})
	if err != nil {
		return "", false, fmt.Errorf("provenance: receipt of %s: %w", id, err)
	}
	return receipt, receipt != "", nil
}

// HasReceipt is Receipt as a single bit — what the delete guard asks.
func (e *Index) HasReceipt(ctx context.Context, id domain.ArtifactID) (bool, error) {
	_, has, err := e.Receipt(ctx, id)
	return has, err
}

// IsReceipt reports whether this artifact IS a receipt — it carries an eviction
// edge — which the guard needs, since receipts are not deletable. Stops at the
// first such edge.
func (e *Index) IsReceipt(ctx context.Context, id domain.ArtifactID) (bool, error) {
	if err := e.ready(); err != nil {
		return false, err
	}
	evictRel, found := e.evictRel(), false
	err := e.eachParent(id, func(ed Edge) error {
		if ed.Rel == evictRel {
			found = true
			return customindex.ErrStopScan
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("provenance: check whether %s is a receipt: %w", id, err)
	}
	return found, nil
}

// Cleanable reports whether an artifact may be removed as a cache — whether it is
// EFFECTIVELY reproducible, not merely declared so (ADR-113 П-11).
//
// The declared flag stays honest about the operation and never changes, but it
// stops being sufficient the moment a source is evicted: recognised text is a
// cache while its scan is on disk and becomes data the moment the scan is gone.
// So the real question is recursive — an artifact is available if it is alive, or
// reproducible with all of ITS inputs available — and this walks it.
//
// The existence probe comes from the caller: the extension has no store handle
// and should not acquire one.
//
// It errs conservatively in one known case. An artifact that was itself evicted
// leaves no record row behind (Unindex removes it with the manifest), so its
// declared reproducibility is no longer readable and it counts as unavailable —
// even when a living grandparent could in principle regenerate it. The answer is
// then "not cleanable" for its descendants, which keeps data rather than losing
// it. Retaining a tombstone row instead would make the live state differ from the
// state a rebuild reconstructs from manifests, and that invariant is worth more
// than the sharper answer.
func (e *Index) Cleanable(
	ctx context.Context,
	id domain.ArtifactID,
	alive func(context.Context, domain.ArtifactID) (bool, error),
) (bool, error) {
	if err := e.ready(); err != nil {
		return false, err
	}
	if alive == nil {
		return false, fmt.Errorf("provenance: Cleanable needs an existence probe")
	}

	rec, ok, err := e.record(id)
	if err != nil {
		return false, err
	}
	if !ok || !rec.repro {
		return false, nil // an origin, or declared non-reproducible
	}
	return e.inputsAvailable(ctx, id, alive, map[domain.ArtifactID]bool{})
}

// inputsAvailable reports whether every input of id can be reached, directly or
// by recomputation.
func (e *Index) inputsAvailable(
	ctx context.Context,
	id domain.ArtifactID,
	alive func(context.Context, domain.ArtifactID) (bool, error),
	memo map[domain.ArtifactID]bool,
) (bool, error) {
	edges, err := e.Parents(ctx, id)
	if err != nil {
		return false, err
	}
	evictRel := e.evictRel()
	for _, ed := range edges {
		if ed.Rel == evictRel {
			continue // a receipt's own edge is not an input
		}
		ok, err := e.available(ctx, domain.ArtifactID(ed.Ref), alive, memo)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (e *Index) available(
	ctx context.Context,
	id domain.ArtifactID,
	alive func(context.Context, domain.ArtifactID) (bool, error),
	memo map[domain.ArtifactID]bool,
) (bool, error) {
	if v, seen := memo[id]; seen {
		return v, nil
	}
	memo[id] = false // guards against a cycle in a corrupt graph

	live, err := alive(ctx, id)
	if err != nil {
		return false, err
	}
	if live {
		memo[id] = true
		return true, nil
	}
	// Gone: recoverable only if it was reproducible and its own inputs are
	// available. See the conservative case in Cleanable's comment.
	rec, ok, err := e.record(id)
	if err != nil {
		return false, err
	}
	if !ok || !rec.repro {
		return false, nil
	}
	res, err := e.inputsAvailable(ctx, id, alive, memo)
	if err != nil {
		return false, err
	}
	memo[id] = res
	return res, nil
}

// recordRow is a parsed records row: what the index knows about an artifact's
// production once its manifest is out of reach.
type recordRow struct {
	pkey, outcome, inputsKey string
	repro                    bool
}

func (e *Index) record(id domain.ArtifactID) (recordRow, bool, error) {
	raw, ok, err := e.sub.Get(tableRecords, string(id))
	if err != nil {
		return recordRow{}, false, fmt.Errorf("provenance: record of %s: %w", id, err)
	}
	if !ok {
		return recordRow{}, false, nil
	}
	pkey, outcome, inputsKey, repro := splitRecord(string(raw))
	return recordRow{pkey: pkey, outcome: outcome, inputsKey: inputsKey, repro: repro}, true, nil
}
