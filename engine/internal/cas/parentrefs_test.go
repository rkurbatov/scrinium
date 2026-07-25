package cas_test

import (
	"context"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/artifact"
	"scrinium.dev/testutil/artifactfx"
)

// WithParentRefs is the public entrance into the artifact→artifact DAG
// (ADR-112). The refs must survive into the manifest verbatim and in order —
// position is part of an edge's identity in the index — and they must be part
// of the hashed body, so an artifact's declared sources are part of what it is.
func TestAssembleManifest_ParentRefsLandInManifest(t *testing.T) {
	w, cfg := harness(t)
	ctx := context.Background()

	src1 := domain.HandleRef(strings.Repeat("1", 64))
	src2 := domain.HandleRef(strings.Repeat("2", 64))

	blob, err := w.Materialize(ctx, cfg, artifactfx.Payload("derived text"), domain.PutOptions{}, "")
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}

	opts := domain.ApplyPut(domain.WithParentRefs(src2, src1)) // order as given
	m, mb, err := w.AssembleManifest(cfg, artifactfx.Payload(""), opts, blob, nil, "")
	if err != nil {
		t.Fatalf("AssembleManifest: %v", err)
	}

	if got := m.HandleRefs; len(got) != 2 || got[0] != src2 || got[1] != src1 {
		t.Fatalf("handle_refs not carried in order: %v", got)
	}

	// The edges must round-trip through the encoded form: they live in the
	// manifest body, not in a side table.
	decoded, err := artifact.Decode(mb)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.HandleRefs) != 2 || decoded.HandleRefs[0] != src2 {
		t.Fatalf("handle_refs lost in encode/decode: %v", decoded.HandleRefs)
	}
}

// Without the option nothing appears: a plain Put stays edge-free, so existing
// callers and the index rows they produce are unaffected.
func TestAssembleManifest_NoParentRefsByDefault(t *testing.T) {
	w, cfg := harness(t)

	blob, err := w.Materialize(context.Background(), cfg, artifactfx.Payload("standalone"), domain.PutOptions{}, "")
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	m, _, err := w.AssembleManifest(cfg, artifactfx.Payload(""), domain.PutOptions{}, blob, nil, "")
	if err != nil {
		t.Fatalf("AssembleManifest: %v", err)
	}
	if len(m.HandleRefs) != 0 {
		t.Fatalf("edge-free Put produced handle_refs: %v", m.HandleRefs)
	}
}

// Declaring the same sources produces the same edges regardless of how the
// caller assembled the slice; the option copies, so later mutation of the
// caller's slice cannot reach into the resolved options.
func TestWithParentRefs_CopiesAndReplaces(t *testing.T) {
	src := domain.HandleRef(strings.Repeat("a", 64))
	mine := []domain.HandleRef{src}

	opts := domain.ApplyPut(domain.WithParentRefs(mine...))
	mine[0] = domain.HandleRef(strings.Repeat("b", 64))

	if opts.ParentRefs[0] != src {
		t.Fatalf("option did not copy the caller's slice: %v", opts.ParentRefs)
	}

	// Repeated application replaces rather than appends: the last declaration
	// of an artifact's sources is the whole truth about them.
	opts = domain.ApplyPut(
		domain.WithParentRefs(src),
		domain.WithParentRefs(domain.HandleRef(strings.Repeat("c", 64))),
	)
	if len(opts.ParentRefs) != 1 {
		t.Fatalf("repeated WithParentRefs appended: %v", opts.ParentRefs)
	}

	// An empty declaration clears: "no sources" is expressible.
	if opts = domain.ApplyPut(domain.WithParentRefs()); opts.ParentRefs != nil {
		t.Fatalf("empty WithParentRefs left refs: %v", opts.ParentRefs)
	}
}
