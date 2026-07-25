package provenance

import (
	"context"
	"fmt"
	"strconv"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
)

// Edge is one incoming edge of a production record as stored: which artifact was
// consumed, as what, and at which position of the record.
type Edge struct {
	Ref domain.HandleRef
	Rel string
	Pos int
}

// HoleQuery describes work that is owed. Exactly one of Is/Has must be given:
// they select candidates differently because pipelines have two shapes — chained
// (chunks come from the text, so "is a text, has no chunks") and star
// (everything hangs off the original, so "has a text, has no thumbnail").
type HoleQuery struct {
	// Is selects candidates by the kind they were produced as.
	Is string
	// Has selects candidates by a kind of derivative they already have.
	Has string
	// Missing is the kind of derivative they lack — the work that is owed. Only a
	// SUCCESSFUL derivative fills a hole.
	Missing string
	// MaxFailures drops a candidate once it has this many recorded failed
	// attempts at Missing (0 — no limit). Quarantine is this threshold, not a
	// state: nothing anywhere says "quarantined".
	MaxFailures int
}

func (q HoleQuery) validate() error {
	switch {
	case q.Missing == "":
		return fmt.Errorf("provenance: HoleQuery needs the missing relation kind")
	case q.Is == "" && q.Has == "":
		return fmt.Errorf("provenance: HoleQuery needs either Is or Has")
	case q.Is != "" && q.Has != "":
		return fmt.Errorf("provenance: HoleQuery takes Is or Has, not both")
	}
	return nil
}

// --- Traversal ---

// Parents returns the artifacts this one was produced from, in the order of the
// record. The order is data: for an assembly it is the order of the parts. It
// comes out of the scan already ordered — positions are zero-padded so that
// lexicographic key order is numeric order — so nothing is sorted here.
func (e *Index) Parents(ctx context.Context, id domain.ArtifactID) ([]Edge, error) {
	if err := e.ready(); err != nil {
		return nil, err
	}
	var edges []Edge
	err := e.eachParent(id, func(ed Edge) error {
		edges = append(edges, ed)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("provenance: parents of %s: %w", id, err)
	}
	return edges, nil
}

// eachParent streams the incoming edges of id in record order. It is the single
// place the byChild layout is parsed; a callback returning ErrStopScan ends the
// walk, which is how the cheap probes avoid materialising a slice.
func (e *Index) eachParent(id domain.ArtifactID, cb func(Edge) error) error {
	return e.sub.Scan(tableByChild, string(id)+sep, func(key string, value []byte) error {
		_, posStr, ok := cut2(key)
		if !ok {
			return nil
		}
		parent, rel, ok := cut2(string(value))
		if !ok {
			return nil
		}
		pos, _ := strconv.Atoi(posStr)
		return cb(Edge{Ref: domain.HandleRef(parent), Rel: rel, Pos: pos})
	})
}

// Children returns everything recorded as produced from this artifact, failed
// attempts included: hiding them would make the graph lie. For "what actually
// came out" use Results. rel="" means every kind.
func (e *Index) Children(ctx context.Context, id domain.ArtifactID, rel string) ([]domain.ArtifactID, error) {
	if err := e.ready(); err != nil {
		return nil, err
	}
	var out []domain.ArtifactID
	err := e.scanChildren(id, rel, func(child string, _ Outcome) error {
		out = append(out, domain.ArtifactID(child))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("provenance: children of %s: %w", id, err)
	}
	return out, nil
}

// Results returns the derivatives that actually came out: children of the given
// kind whose attempt succeeded (rel="" — of any kind).
func (e *Index) Results(ctx context.Context, id domain.ArtifactID, rel string) ([]domain.ArtifactID, error) {
	if err := e.ready(); err != nil {
		return nil, err
	}
	var out []domain.ArtifactID
	err := e.scanChildren(id, rel, func(child string, outcome Outcome) error {
		if outcome == OutcomeOK {
			out = append(out, domain.ArtifactID(child))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("provenance: results of %s: %w", id, err)
	}
	return out, nil
}

// HasChildOf reports whether the artifact has at least one recorded child of the
// given kind, successful or not (rel="" — any kind). This is what the delete
// guard asks: a failed attempt still references its source, so it still pins it.
func (e *Index) HasChildOf(ctx context.Context, id domain.ArtifactID, rel string) (bool, error) {
	return e.probeChildren(id, rel, func(Outcome) bool { return true })
}

// HasResultOf reports whether the artifact already has a SUCCESSFUL derivative
// of the given kind. This is what planning asks: failed attempts do not fill a
// hole, or a stage that keeps breaking would look complete.
func (e *Index) HasResultOf(ctx context.Context, id domain.ArtifactID, rel string) (bool, error) {
	return e.probeChildren(id, rel, func(o Outcome) bool { return o == OutcomeOK })
}

// probeChildren stops at the first child whose outcome satisfies want.
func (e *Index) probeChildren(id domain.ArtifactID, rel string, want func(Outcome) bool) (bool, error) {
	if err := e.ready(); err != nil {
		return false, err
	}
	found := false
	err := e.scanChildren(id, rel, func(_ string, outcome Outcome) error {
		if !want(outcome) {
			return nil
		}
		found = true
		return customindex.ErrStopScan
	})
	if err != nil {
		return false, fmt.Errorf("provenance: probe children of %s: %w", id, err)
	}
	return found, nil
}

// scanChildren streams (child, outcome) for one parent, optionally narrowed to a
// kind. It is the single place the byParent layout is parsed.
func (e *Index) scanChildren(id domain.ArtifactID, rel string, cb func(child string, outcome Outcome) error) error {
	prefix := string(id) + sep
	if rel != "" {
		prefix += rel + sep
	}
	return e.sub.Scan(tableByParent, prefix, func(key string, value []byte) error {
		_, _, child, ok := split3(key)
		if !ok {
			return nil
		}
		return cb(child, outcomeOf(value))
	})
}

// WalkUp visits the ancestors of id, breadth-first, up to depth levels
// (depth<=0 — no limit), skipping artifacts already seen so a diamond is visited
// once and a cycle cannot spin. An error from the callback stops the walk and
// propagates unchanged.
func (e *Index) WalkUp(ctx context.Context, id domain.ArtifactID, depth int, cb func(domain.ArtifactID, int) error) error {
	return e.walk(ctx, id, depth, cb, func(ctx context.Context, cur domain.ArtifactID) ([]domain.ArtifactID, error) {
		edges, err := e.Parents(ctx, cur)
		if err != nil {
			return nil, err
		}
		next := make([]domain.ArtifactID, 0, len(edges))
		for _, ed := range edges {
			next = append(next, domain.ArtifactID(ed.Ref))
		}
		return next, nil
	})
}

// WalkDown visits the descendants of id, breadth-first — the enumeration a
// cascade needs when a source has been replaced and everything made from it must
// be recomputed.
func (e *Index) WalkDown(ctx context.Context, id domain.ArtifactID, depth int, cb func(domain.ArtifactID, int) error) error {
	return e.walk(ctx, id, depth, cb, func(ctx context.Context, cur domain.ArtifactID) ([]domain.ArtifactID, error) {
		return e.Children(ctx, cur, "")
	})
}

func (e *Index) walk(
	ctx context.Context,
	from domain.ArtifactID,
	depth int,
	cb func(domain.ArtifactID, int) error,
	step func(context.Context, domain.ArtifactID) ([]domain.ArtifactID, error),
) error {
	if err := e.ready(); err != nil {
		return err
	}
	seen := map[domain.ArtifactID]struct{}{from: {}}
	frontier := []domain.ArtifactID{from}

	for level := 1; len(frontier) > 0 && (depth <= 0 || level <= depth); level++ {
		var nextFrontier []domain.ArtifactID
		for _, cur := range frontier {
			next, err := step(ctx, cur)
			if err != nil {
				return err
			}
			for _, n := range next {
				if _, dup := seen[n]; dup {
					continue
				}
				seen[n] = struct{}{}
				if err := cb(n, level); err != nil {
					return err
				}
				nextFrontier = append(nextFrontier, n)
			}
		}
		frontier = nextFrontier
	}
	return nil
}

// --- Planning ---

// Holes streams the artifacts that owe work. It is the whole state a pipeline
// stage needs: no queue, no cursor, no lease — the answer is derived each time.
func (e *Index) Holes(ctx context.Context, q HoleQuery, cb func(domain.ArtifactID) error) error {
	if err := e.ready(); err != nil {
		return err
	}
	if err := q.validate(); err != nil {
		return err
	}

	candidates, err := e.candidates(q)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		id := domain.ArtifactID(c)

		// An evicted artifact keeps the rows where it is a parent — they were
		// written by its children's manifests, which are alive. Without this check
		// a star-shaped query would keep offering work on bytes that are gone, and
		// the stage would fail and mis-tally the failure (ADR-113 П-10).
		if evicted, err := e.HasReceipt(ctx, id); err != nil {
			return err
		} else if evicted {
			continue
		}

		has, err := e.HasResultOf(ctx, id, q.Missing)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		if q.MaxFailures > 0 {
			n, err := e.Failures(ctx, id, q.Missing, "")
			if err != nil {
				return err
			}
			if n >= q.MaxFailures {
				continue
			}
		}
		if err := cb(id); err != nil {
			return err
		}
	}
	return nil
}

// candidates resolves the Is/Has side of a hole query. byRel is keyed kind-first
// precisely so this half is a prefix scan rather than a walk of the whole graph;
// whether the candidate is the child or the parent of that edge is what
// distinguishes a chained pipeline from a star.
//
// The candidate set is materialised before the per-candidate probes run, and
// deliberately so: each probe is itself a scan, and opening one inside an open
// scan of the same substrate is not something the contract promises. The cost is
// memory linear in the number of candidates of one kind.
func (e *Index) candidates(q HoleQuery) ([]string, error) {
	kind, childSide := q.Has, false
	if q.Is != "" {
		kind, childSide = q.Is, true
	}
	seen := map[string]struct{}{}
	var out []string
	err := e.sub.Scan(tableByRel, kind+sep, func(key string, value []byte) error {
		_, parent, child, ok := split3(key)
		if !ok {
			return nil
		}
		if len(value) != 0 && Outcome(value) != OutcomeOK {
			return nil // a failed attempt is not evidence a stage completed
		}
		who := parent
		if childSide {
			who = child
		}
		if _, dup := seen[who]; dup {
			return nil
		}
		seen[who] = struct{}{}
		out = append(out, who)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("provenance: scan %q candidates: %w", kind, err)
	}
	return out, nil
}

// Done reports whether this exact work — the same operation with the same
// parameters over the same inputs, in order — has already produced an artifact.
func (e *Index) Done(ctx context.Context, pkey string, inputs []domain.HandleRef) (domain.ArtifactID, bool, error) {
	if err := e.ready(); err != nil {
		return "", false, err
	}
	if pkey == "" {
		// No work identity, nothing to recognise: a record without an operation
		// never claimed to be repeatable work.
		return "", false, nil
	}
	v, ok, err := e.sub.Get(tableOps, join(opsOK, pkey, inputsKeyOf(inputs)))
	if err != nil {
		return "", false, fmt.Errorf("provenance: probe work %s: %w", pkey, err)
	}
	if !ok {
		return "", false, nil
	}
	return domain.ArtifactID(v), true, nil
}

// Failures counts recorded failed attempts against a source: attempts to produce
// rel from id, optionally narrowed to one parameter set (pkey="" — any).
//
// The count is derived by scanning the failure rows rather than kept in the
// substrate's counter (Substrate.Inc), and that is a deliberate trade. A counter
// would answer in one read, but it would be a second source of truth able to
// drift from the records it summarises; a scan is always exact and needs no
// repair. If this ever becomes hot, Inc is the sanctioned upgrade — it is
// transactional inside Index, where the failures are written.
func (e *Index) Failures(ctx context.Context, id domain.ArtifactID, rel, pkey string) (int, error) {
	if err := e.ready(); err != nil {
		return 0, err
	}
	prefix := join(opsFail, string(id), rel) + sep
	if pkey != "" {
		prefix += pkey + sep
	}
	n := 0
	err := e.sub.Scan(tableOps, prefix, func(string, []byte) error { n++; return nil })
	if err != nil {
		return 0, fmt.Errorf("provenance: failures of %s: %w", id, err)
	}
	return n, nil
}

// --- Currency ---

// Head resolves a chain of replacements to its end; an artifact nobody replaced
// is its own head. Eviction edges are not followed — a receipt is not a
// successor. A fork returns ErrForked with the candidates: detecting it is
// mechanical, choosing between them is not this extension's policy.
func (e *Index) Head(ctx context.Context, id domain.ArtifactID) (domain.ArtifactID, error) {
	if err := e.ready(); err != nil {
		return "", err
	}
	cur := id
	seen := map[domain.ArtifactID]struct{}{id: {}}

	for {
		var next []domain.ArtifactID
		err := e.sub.Scan(tableHeads, string(cur)+sep, func(key string, _ []byte) error {
			if _, succ, ok := cut2(key); ok {
				next = append(next, domain.ArtifactID(succ))
			}
			return nil
		})
		if err != nil {
			return "", fmt.Errorf("provenance: head of %s: %w", id, err)
		}
		switch len(next) {
		case 0:
			return cur, nil
		case 1:
			cur = next[0]
			if _, loop := seen[cur]; loop {
				return "", fmt.Errorf("provenance: supersede cycle at %s", cur)
			}
			seen[cur] = struct{}{}
		default:
			return "", fmt.Errorf("%s superseded by %v: %w", cur, next, ErrForked)
		}
	}
}
