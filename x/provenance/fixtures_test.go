package provenance

import (
	"encoding/hex"
	"sort"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
	"scrinium.dev/testutil/substratefx"
)

// Shared fixtures for the package's white-box tests. The substrate fake is the
// common one (testutil/substratefx) rather than a local copy: its Scan honours
// the contract's lexicographic order, which this index depends on for record
// order and for every prefix probe.

func newIndex(t *testing.T, cfg Config) (*Index, *substratefx.Substrate) {
	t.Helper()
	idx, sub := NewIndex(cfg), substratefx.Memory(t)
	if err := idx.Setup(t.Context(), sub, 0); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return idx, sub
}

// aid builds a readable artifact id from one letter.
func aid(c string) domain.ArtifactID { return domain.ArtifactID(strings.Repeat(c, 8)) }

// production builds a manifest as the wrapper would have written it: edges in the
// core half, meaning in Ext. The base comes from manifestfx, so the fixture
// carries a digest and blob ref like a real indexed manifest does.
func production(child domain.ArtifactID, op string, outcome Outcome, repro bool, inputs ...Input) domain.Manifest {
	m := manifestfx.Blob(string(child), fxBlobRef(string(child)))

	block := Block{V: SchemaVersion, Op: op, Outcome: outcome, Repro: repro}
	refs := make([]domain.HandleRef, 0, len(inputs))
	for _, in := range inputs {
		block.Rel = append(block.Rel, in.Rel)
		refs = append(refs, in.Ref)
	}
	if op != "" {
		block.PKey, _ = ParamsKey(op, nil)
	}
	ext, err := stamp(m.Ext, block)
	if err != nil {
		panic(err)
	}
	m.HandleRefs = refs
	m.Ext = ext
	return m
}

// record is production with reproducibility declared — the common case for a
// machine-made derivative.
func record(child domain.ArtifactID, op string, outcome Outcome, inputs ...Input) domain.Manifest {
	return production(child, op, outcome, true, inputs...)
}

// receiptManifest is what the evictor's Put produces: a non-reproducible
// judgement whose only edge is the eviction edge to the artifact it explains.
func receiptManifest(rid, evicted domain.ArtifactID, cfg Config) domain.Manifest {
	return production(rid, EvictOp, OutcomeOK, false,
		Input{Ref: domain.HandleRef(evicted), Rel: cfg.evictRel()})
}

func indexAll(t *testing.T, idx *Index, sub *substratefx.Substrate, ms ...domain.Manifest) {
	t.Helper()
	for _, m := range ms {
		if _, err := idx.Index(t.Context(), sub, m); err != nil {
			t.Fatalf("Index %s: %v", m.ArtifactID, err)
		}
	}
}

func sortedIDs(list []domain.ArtifactID) []string {
	out := make([]string, len(list))
	for i, v := range list {
		out[i] = string(v)
	}
	sort.Strings(out)
	return out
}

func mustPKey(t *testing.T, op string) string {
	t.Helper()
	k, err := ParamsKey(op, nil)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// manifestfxBlobWithForeignExt is an origin: a valid indexed manifest carrying
// someone else's Ext schema and no production record.
func manifestfxBlobWithForeignExt(id domain.ArtifactID) domain.Manifest {
	m := manifestfx.Blob(string(id), fxBlobRef(string(id)))
	m.Ext = []byte(`{"vfsmeta":{"path":"books/foo.pdf"}}`)
	return m
}

// fxBlobRef derives a bare-hex blob ref from a fixture id: refs carry no
// prefix (ADR-93) and the index stores them as raw bytes.
func fxBlobRef(id string) string { return hex.EncodeToString([]byte(id)) }
