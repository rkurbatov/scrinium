package provenance

import (
	"context"
	"fmt"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/present"
)

const (
	// Name is the custom-index identifier and the namespace its own tables live
	// under in the substrate. It is distinct from Key (the Ext schema key, also
	// the extension's registration name): one names a place in the index, the
	// other a block in a manifest.
	Name = "scrinium.provenance"

	// indexSchemaVersion is the layout version of the tables in keys.go.
	indexSchemaVersion = 1
)

// Index is the provenance custom index: the write side (the Indexer capability)
// plus a read surface of its own. The queries live in query.go, eviction in
// eviction.go; this file is the contract and the indexing itself.
//
// It projects nothing into the shared equality tables, because not one of its
// questions is an equality match — "has no derivative of this kind" is an
// anti-join, and an ordered walk of a graph is not a lookup.
type Index struct {
	cfg Config

	// sub is captured at Setup and used by the read side for the lifetime of the
	// StoreIndex; the backend swaps the underlying executor from transaction mode
	// to database mode after registration commits.
	sub customindex.Substrate
}

// NewIndex returns a fresh provenance index. Register it through the store
// index's custom-index registry, or install the whole extension (extension.go).
// The configuration given here is the one every other part of the extension
// reads — see Config.
func NewIndex(cfg Config) *Index { return &Index{cfg: cfg} }

func (e *Index) Name() string         { return Name }
func (e *Index) SchemaVersion() int   { return indexSchemaVersion }
func (e *Index) Close() error         { e.sub = nil; return nil }
func (e *Index) supersedeRel() string { return e.cfg.supersedeRel() }
func (e *Index) evictRel() string     { return e.cfg.evictRel() }

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

func (e *Index) ready() error {
	if e.sub == nil {
		return fmt.Errorf("provenance: index not registered")
	}
	return nil
}

// PresentedSchemas makes the index the presenter of its own Ext schema: the
// owner of a schema is the one that knows how to show it.
func (e *Index) PresentedSchemas() []present.Schema { return presentedSchemas() }

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
			return nil, fmt.Errorf("provenance index: %q: %w: relation kind contains a separator byte",
				m.ArtifactID, ErrBadProduction)
		}
		parent, pos := string(ref), padPos(i)

		// The outcome travels with the edge, because a failed attempt is a real
		// edge — it consumed its input and belongs in the graph — but it is not a
		// result: the work is still owed. Keeping it in the value lets traversal
		// show everything while planning counts only results.
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
			// A failure is tallied against each input it was attempted on, under
			// the kind being produced, so a planner can skip a source that keeps
			// breaking without keeping any state of its own.
			if err := sub.Put(tableOps, join(opsFail, parent, rel, block.PKey, child), []byte(mark)); err != nil {
				return nil, err
			}
		}
	}

	// The per-child record makes deletion self-sufficient: at delete time the
	// manifest body is gone (only its identity is passed), so the index must be
	// able to recover from its own tables everything it wrote. Reproducibility is
	// stored for the same reason — after an eviction the manifest that declared
	// it no longer exists (ADR-113 П-11).
	if err := sub.Put(tableRecords, child,
		[]byte(join(block.PKey, string(block.Outcome), inputsKey, boolStr(block.Repro)))); err != nil {
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
	pkey, outcome, inputsKey, _ := splitRecord(string(raw))

	// Walk the edges from the child side: byChild is keyed by this artifact, so
	// it is the one table reachable without the manifest body.
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

// Compile-time conformance: a CustomIndex occupying the Indexer capability and
// presenting its own schema; it exposes no standard Accessor, because none of
// its questions is a key lookup.
var (
	_ customindex.CustomIndex = (*Index)(nil)
	_ customindex.Indexer     = (*Index)(nil)
	_ derivativeLookup        = (*Index)(nil)
)
