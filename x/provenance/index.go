package provenance

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/present"
)

const (
	// Name is the custom-index identifier and the namespace its own tables
	// live under in the substrate.
	Name = "scrinium.provenance"

	// indexSchemaVersion is the layout version of the own tables below.
	indexSchemaVersion = 1

	// sep separates key components. It cannot occur in an artifact handle
	// and is rejected in a relation kind, so a composite key parses
	// unambiguously and a prefix scan cannot spill into a neighbouring key.
	sep = "\x00"

	// The own tables. Every one of them is a pure function of the manifests
	// seen, so a rebuild replays them and none is a source of truth.
	tableByParent = "byParent" // parent ‖ rel ‖ child → position ‖ outcome
	tableByChild  = "byChild"  // child ‖ position → parent ‖ rel
	tableByRel    = "byRel"    // rel ‖ parent ‖ child → outcome
	tableRecords  = "records"  // child → pkey ‖ outcome ‖ inputs-key
	tableOps      = "ops"      // work identity → result; failure tallies
	tableHeads    = "heads"    // superseded ‖ successor → marker

	opsOK   = "ok"
	opsFail = "fail"

	// mark is a non-empty value for rows that carry no payload: the substrate
	// stores values NOT NULL, so a set-membership row still needs a byte.
	mark = "1"

	// posWidth zero-pads a position so lexicographic key order is numeric
	// order: the edge array is capped at 65535, five digits cover it.
	posWidth = 5
)

// Edge is one incoming edge of a production record as stored: which artifact
// was consumed, as what, and at which position of the record.
type Edge struct {
	Ref domain.HandleRef
	Rel string
	Pos int
}

// HoleQuery describes work that is owed. A hole is not a queue entry: nothing
// is enqueued, leased or acknowledged, and a crash leaves nothing to clean up —
// the hole is simply still there on the next pass, and stops being reported the
// moment the derivative exists.
//
// Candidates are named in one of two ways, and exactly one must be given,
// because the two describe different pipeline shapes:
//
//   - Is — the artifact was itself produced as this kind. This is the chained
//     shape: chunks derive from the text, which derives from the scan, so
//     "is a text, has no chunks" is the question stage three asks.
//   - Has — the artifact has a derivative of this kind. This is the star
//     shape, where everything hangs off the original: "has a text, has no
//     thumbnail".
type HoleQuery struct {
	// Is selects candidates by the kind they were produced as.
	Is string
	// Has selects candidates by a kind of derivative they already have.
	Has string
	// Missing is the kind of derivative they lack — the work that is owed.
	// Only a SUCCESSFUL derivative fills a hole.
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

// Index is the provenance custom index. It occupies the Indexer capability
// (write side) and exposes its own read surface; it projects nothing into the
// shared equality tables, because not one of its questions is an equality
// match — "has no derivative of this kind" is an anti-join, and an ordered walk
// of a graph is not a lookup.
type Index struct {
	cfg Config

	// sub is captured at Setup and used by the read side for the lifetime of
	// the StoreIndex; the backend swaps the underlying executor from
	// transaction mode to database mode after registration commits.
	sub customindex.Substrate
}

// NewIndex returns a fresh provenance index. Register it through the store
// index's custom-index registry, or install the whole extension.
func NewIndex(cfg Config) *Index { return &Index{cfg: cfg} }

func (e *Index) Name() string         { return Name }
func (e *Index) SchemaVersion() int   { return indexSchemaVersion }
func (e *Index) Close() error         { e.sub = nil; return nil }
func (e *Index) supersedeRel() string { return e.cfg.supersedeRel() }

// Setup captures the substrate. The tables are implicit key-value spaces, so
// there is nothing to create; an unknown stored version is refused rather than
// guessed at.
func (e *Index) Setup(ctx context.Context, sub customindex.Substrate, oldVersion int) error {
	switch oldVersion {
	case 0, indexSchemaVersion:
	default:
		return fmt.Errorf("provenance index: unsupported old schema version: %d", oldVersion)
	}
	e.sub = sub
	return nil
}

// --- Indexer (write side) ---

// Index writes the graph rows for one manifest inside the index-write
// transaction. It is a pure function of the manifest: the edges come from the
// core half of the record (HandleRefs), the meaning from this extension's Ext
// block, and nothing is read from anywhere else — which is what lets a rebuild
// reproduce the same rows from the manifests alone.
//
// A manifest with no provenance block is skipped: an artifact that came from
// outside the system has no production record, and that is not a defect.
func (e *Index) Index(ctx context.Context, sub customindex.Substrate, m domain.Manifest) ([]customindex.Projection, error) {
	block, ok, err := Decode(m.Ext)
	if err != nil {
		return nil, fmt.Errorf("provenance index: decode %q: %w", m.ArtifactID, err)
	}
	if !ok {
		return nil, nil
	}
	if err := block.Validate(len(m.HandleRefs)); err != nil {
		// A stored manifest whose meaning does not line up with its edges is
		// corrupt or foreign; refuse it loudly rather than build half a graph.
		return nil, fmt.Errorf("provenance index: %q: %w", m.ArtifactID, err)
	}

	child := string(m.ArtifactID)
	if child == "" {
		return nil, nil // container or system manifest: no child identity
	}
	inputsKey := inputsKeyOf(m.HandleRefs)

	for i, ref := range m.HandleRefs {
		rel := block.Rel[i]
		if strings.Contains(rel, sep) {
			return nil, fmt.Errorf("provenance index: %q: relation kind contains a separator byte", m.ArtifactID)
		}
		parent, pos := string(ref), padPos(i)

		// The outcome travels with the edge, because a failed attempt is a
		// real edge — it consumed its input and belongs in the graph — but it
		// is not a result: the work is still owed. Keeping it in the value
		// lets traversal show everything while planning counts only results.
		if err := sub.Put(tableByParent, join(parent, rel, child), []byte(join(pos, string(block.Outcome)))); err != nil {
			return nil, err
		}
		if err := sub.Put(tableByChild, join(child, pos), []byte(join(parent, rel))); err != nil {
			return nil, err
		}
		if err := sub.Put(tableByRel, join(rel, parent, child), []byte(block.Outcome)); err != nil {
			return nil, err
		}
		if rel == e.supersedeRel() {
			if err := sub.Put(tableHeads, join(parent, child), []byte(mark)); err != nil {
				return nil, err
			}
		}
		if block.Outcome != OutcomeOK && block.PKey != "" {
			// A failure is tallied against each input it was attempted on,
			// under the kind being produced, so a planner can skip a source
			// that keeps breaking without keeping any state of its own.
			if err := sub.Put(tableOps, join(opsFail, parent, rel, block.PKey, child), []byte(mark)); err != nil {
				return nil, err
			}
		}
	}

	// The per-child record is what makes deletion self-sufficient: at delete
	// time the manifest body is gone (only its identity is passed), so the
	// index must be able to recover from its own tables everything it wrote.
	if err := sub.Put(tableRecords, child, []byte(join(block.PKey, string(block.Outcome), inputsKey))); err != nil {
		return nil, err
	}
	if block.Outcome == OutcomeOK && block.PKey != "" {
		if err := sub.Put(tableOps, join(opsOK, block.PKey, inputsKey), []byte(child)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Unindex removes every row Index wrote for this artifact. The manifest passed
// on delete carries identity only — its body is already gone — so the rows are
// recovered from the index's own tables rather than recomputed from Ext.
// Symmetric and idempotent, so a replay after crash recovery converges.
func (e *Index) Unindex(ctx context.Context, sub customindex.Substrate, m domain.Manifest) error {
	child := string(m.ArtifactID)
	if child == "" {
		return nil
	}
	raw, ok, err := sub.Get(tableRecords, child)
	if err != nil {
		return err
	}
	if !ok {
		return nil // nothing of ours was ever written for this artifact
	}
	pkey, outcome, inputsKey := splitRecord(string(raw))

	// Walk the edges from the child side: byChild is keyed by this artifact,
	// so it is the one table reachable without the manifest body.
	type edgeRow struct{ pos, parent, rel string }
	var edges []edgeRow
	err = sub.Scan(tableByChild, child+sep, func(key string, value []byte) error {
		_, pos, ok := cut2(key)
		if !ok {
			return nil
		}
		parent, rel, ok := cut2(string(value))
		if !ok {
			return nil
		}
		edges = append(edges, edgeRow{pos: pos, parent: parent, rel: rel})
		return nil
	})
	if err != nil {
		return fmt.Errorf("provenance: unindex %s: %w", child, err)
	}

	for _, ed := range edges {
		for _, del := range []struct{ table, key string }{
			{tableByParent, join(ed.parent, ed.rel, child)},
			{tableByChild, join(child, ed.pos)},
			{tableByRel, join(ed.rel, ed.parent, child)},
		} {
			if err := sub.Delete(del.table, del.key); err != nil {
				return err
			}
		}
		if ed.rel == e.supersedeRel() {
			if err := sub.Delete(tableHeads, join(ed.parent, child)); err != nil {
				return err
			}
		}
		if outcome != string(OutcomeOK) && pkey != "" {
			if err := sub.Delete(tableOps, join(opsFail, ed.parent, ed.rel, pkey, child)); err != nil {
				return err
			}
		}
	}

	if outcome == string(OutcomeOK) && pkey != "" {
		if err := sub.Delete(tableOps, join(opsOK, pkey, inputsKey)); err != nil {
			return err
		}
	}
	return sub.Delete(tableRecords, child)
}

// --- Read side ---

// Parents returns the artifacts this one was produced from, in the order of the
// record. The order is data: for an assembly it is the order of the parts.
func (e *Index) Parents(ctx context.Context, id domain.ArtifactID) ([]Edge, error) {
	if err := e.ready(); err != nil {
		return nil, err
	}
	var edges []Edge
	err := e.sub.Scan(tableByChild, string(id)+sep, func(key string, value []byte) error {
		_, posStr, ok := cut2(key)
		if !ok {
			return nil
		}
		parent, rel, ok := cut2(string(value))
		if !ok {
			return nil
		}
		pos, _ := strconv.Atoi(posStr)
		edges = append(edges, Edge{Ref: domain.HandleRef(parent), Rel: rel, Pos: pos})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("provenance: parents of %s: %w", id, err)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].Pos < edges[j].Pos })
	return edges, nil
}

// Children returns everything recorded as produced from this artifact,
// including failed attempts: a failure is part of a source's provenance, and
// hiding it would make the graph lie. For "what actually came out of it" use
// Results. rel="" means every kind.
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

// HasChildOf reports whether the artifact has at least one recorded child of
// the given kind, successful or not (rel="" — any kind). This is what the
// delete guard asks: a failed attempt still references its source, so it still
// pins it.
func (e *Index) HasChildOf(ctx context.Context, id domain.ArtifactID, rel string) (bool, error) {
	return e.probeChildren(id, rel, func(Outcome) bool { return true })
}

// HasResultOf reports whether the artifact already has a SUCCESSFUL derivative
// of the given kind. This is what planning asks: failed attempts do not fill a
// hole, or a stage that keeps breaking would look complete.
func (e *Index) HasResultOf(ctx context.Context, id domain.ArtifactID, rel string) (bool, error) {
	return e.probeChildren(id, rel, func(o Outcome) bool { return o == OutcomeOK })
}

// HasChildren satisfies the wrapper's delete guard: any derivative at all pins
// its source.
func (e *Index) HasChildren(ctx context.Context, id domain.ArtifactID) (bool, error) {
	return e.HasChildOf(ctx, id, "")
}

// WalkUp visits the ancestors of id, breadth-first, up to depth levels
// (depth<=0 — no limit), skipping artifacts already seen so a diamond is
// visited once and a cycle cannot spin.
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
// cascade needs when a source has been replaced and everything made from it
// must be recomputed.
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

// Holes streams the artifacts that owe work, per the query. It is the whole
// state a pipeline stage needs: no queue, no cursor, no lease — the answer is
// derived from the graph each time it is asked.
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

// candidates resolves the Is/Has side of a hole query. byRel is keyed
// kind-first precisely so this half is a prefix scan rather than a walk of the
// whole graph; whether the candidate is the child or the parent of that edge is
// what distinguishes a chained pipeline from a star.
func (e *Index) candidates(q HoleQuery) ([]string, error) {
	kind, childSide := q.Has, false
	if q.Is != "" {
		kind, childSide = q.Is, true
	}
	seen := map[string]struct{}{}
	var out []string
	err := e.sub.Scan(tableByRel, kind+sep, func(key string, value []byte) error {
		parts := strings.Split(key, sep)
		if len(parts) != 3 {
			return nil
		}
		if len(value) != 0 && Outcome(value) != OutcomeOK {
			return nil // a failed attempt is not evidence a stage completed
		}
		who := parts[1] // parent
		if childSide {
			who = parts[2]
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
	v, ok, err := e.sub.Get(tableOps, join(opsOK, pkey, inputsKeyOf(inputs)))
	if err != nil {
		return "", false, fmt.Errorf("provenance: probe work %s: %w", pkey, err)
	}
	if !ok {
		return "", false, nil
	}
	return domain.ArtifactID(v), true, nil
}

// Failures counts recorded failed attempts against a source: attempts to
// produce rel from id, optionally narrowed to one parameter set (pkey="" —
// any). The count is derived from the failure records themselves, so it cannot
// drift from them.
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

// Head resolves a chain of replacements to its current end: the artifact that
// supersedes this one, and so on, until nothing supersedes it. An artifact that
// was never superseded is its own head.
//
// A fork — two artifacts claiming to replace the same one — returns ErrForked
// with the candidates. Detecting it is mechanical; choosing between them is a
// policy this extension does not own.
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

// --- SchemaPresenter ---

// PresentedSchemas makes the index the presenter of its own Ext schema: the
// owner of a schema is the one that knows how to show it.
func (e *Index) PresentedSchemas() []present.Schema { return presentedSchemas() }

// --- helpers ---

func (e *Index) ready() error {
	if e.sub == nil {
		return fmt.Errorf("provenance: index not registered")
	}
	return nil
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

// scanChildren streams (child, outcome) for one parent, optionally narrowed to
// a kind. It is the single place the byParent layout is parsed.
func (e *Index) scanChildren(id domain.ArtifactID, rel string, cb func(child string, outcome Outcome) error) error {
	prefix := string(id) + sep
	if rel != "" {
		prefix += rel + sep
	}
	return e.sub.Scan(tableByParent, prefix, func(key string, value []byte) error {
		parts := strings.Split(key, sep)
		if len(parts) != 3 {
			return nil
		}
		return cb(parts[2], outcomeOf(value))
	})
}

// outcomeOf reads the outcome out of a byParent value ("position ‖ outcome").
func outcomeOf(value []byte) Outcome {
	_, out, ok := cut2(string(value))
	if !ok || out == "" {
		return OutcomeOK
	}
	return Outcome(out)
}

// splitRecord parses a records row ("pkey ‖ outcome ‖ inputs-key").
func splitRecord(v string) (pkey, outcome, inputsKey string) {
	parts := strings.SplitN(v, sep, 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

func join(parts ...string) string { return strings.Join(parts, sep) }

func padPos(i int) string {
	s := strconv.Itoa(i)
	if len(s) >= posWidth {
		return s
	}
	return strings.Repeat("0", posWidth-len(s)) + s
}

// cut2 splits a two-component key on the first separator.
func cut2(s string) (string, string, bool) {
	i := strings.Index(s, sep)
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+len(sep):], true
}

func inputsKeyOf(refs []domain.HandleRef) string {
	strs := make([]string, len(refs))
	for i, r := range refs {
		strs[i] = string(r)
	}
	return InputsKey(strs)
}

// Compile-time conformance: a CustomIndex occupying the Indexer capability and
// presenting its own schema; it exposes no standard Accessor, because none of
// its questions is a key lookup.
var (
	_ customindex.CustomIndex = (*Index)(nil)
	_ customindex.Indexer     = (*Index)(nil)
	_ derivativeLookup        = (*Index)(nil)
)
