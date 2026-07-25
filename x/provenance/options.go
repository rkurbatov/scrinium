package provenance

import (
	"encoding/json"
	"errors"
	"fmt"

	"scrinium.dev/domain"
)

// ErrBadProduction — a production record is malformed and the artifact is not
// written: relation kinds that do not match the inputs, an empty kind, a
// duplicate input, an operation missing where there are inputs, params that
// are not JSON. It fires in the wrapper, before the core Put, so a bad record
// never reaches the manifest. Shape limits on the edges themselves (the count
// cap, empty or duplicate refs) stay with the core.
var ErrBadProduction = errors.New("provenance: bad production record")

// ErrHasDerivatives — a Delete was refused because the artifact still has
// derivatives (incoming consumption edges). Until the core accounts for
// artifact→artifact edges this guard is the only protection a source has, and
// it lives in the wrapper: a deployment without this extension does not have
// it.
var ErrHasDerivatives = errors.New("provenance: artifact has derivatives")

// ErrForked — a supersede chain branches: more than one artifact claims to
// replace the same one. Detecting the fork is mechanical; choosing the winner
// is a policy this extension does not have.
var ErrForked = errors.New("provenance: superseded by more than one artifact")

// Input is one edge of a production record: which artifact was consumed, and
// as what. Rel is an opaque kind — the host's vocabulary ("derived",
// "member", "supersedes") — which this package stores and indexes but does
// not interpret.
type Input struct {
	Ref domain.HandleRef
	Rel string
}

// Production is a full record of how an artifact came to be, as the host
// declares it at write time.
type Production struct {
	// Inputs are the source artifacts, in order. Order is significant and
	// preserved: for an assembly it is the order of the parts.
	Inputs []Input

	// Op names the operation. Required when there are inputs.
	Op string

	// Params are the operation's parameters, opaque JSON. They decide two
	// mechanical questions: whether this work was already done, and what is
	// now stale because the parameters moved on.
	Params json.RawMessage

	// Repro declares reproducibility: can this artifact be recomputed from
	// its inputs by the same tool. False for anything fetched from outside,
	// judged by a human, or produced by a non-deterministic model.
	Repro bool

	// Outcome is how the attempt ended; empty means OutcomeOK. A failed
	// attempt is written as an artifact too — that is what makes the failure
	// count derivable rather than stored.
	Outcome Outcome

	// Seq identifies this output when one input was split into many. Zero
	// means the output is not part of a split.
	Seq int
}

// WithProduction declares a Put's production record (ADR-112). It resolves
// into two halves of one manifest: the edge targets go to the core option
// (Manifest.HandleRefs, where reference accounting and GC see them), and the
// meaning — kinds, operation, parameters, reproducibility, outcome — travels
// on the generic per-call hint channel to this extension's wrapper, which
// validates it and stamps Ext.
//
// The split is the point: the core learns that this artifact holds those
// artifacts, and nothing more. What "ocr" means, what the parameters say,
// whether the result can be recomputed — none of that is core vocabulary, and
// none of it reaches the core.
//
// Errors surface at write time in the wrapper (ErrBadProduction), not here:
// an option cannot fail.
func WithProduction(p Production) domain.PutOption {
	refs := make([]domain.HandleRef, len(p.Inputs))
	for i, in := range p.Inputs {
		refs[i] = in.Ref
	}
	// The hint carries the meaning; encoding failure is deferred to the
	// wrapper, which reports it as a bad record rather than panicking here.
	hint, err := json.Marshal(wireProduction{
		Rel:     relsOf(p.Inputs),
		Op:      p.Op,
		Params:  p.Params,
		Repro:   p.Repro,
		Outcome: p.Outcome,
		Seq:     p.Seq,
	})
	if err != nil {
		hint = []byte(`{"broken":true}`)
	}
	return func(o *domain.PutOptions) {
		domain.WithParentRefs(refs...)(o)
		domain.WithExtHint(Key, string(hint))(o)
	}
}

// wireProduction is the on-the-hint form of a Production: the meaning without
// the refs, which travel as the core option. It is private on purpose — the
// hint channel is an implementation detail between the option and the wrapper
// of the same extension.
type wireProduction struct {
	Rel     []string        `json:"rel,omitempty"`
	Op      string          `json:"op,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Repro   bool            `json:"repro"`
	Outcome Outcome         `json:"outcome,omitempty"`
	Seq     int             `json:"seq,omitempty"`
}

func relsOf(in []Input) []string {
	if len(in) == 0 {
		return nil
	}
	rels := make([]string, len(in))
	for i, e := range in {
		rels[i] = e.Rel
	}
	return rels
}

// hintedProduction reads this extension's per-call hint out of a Put's
// resolved options and turns it into a Block. ok=false means the Put carries
// no production record and must pass through untouched.
func hintedProduction(opts []domain.PutOption) (Block, bool, error) {
	resolved := domain.ApplyPut(opts...)
	raw, ok := resolved.ExtHints[Key]
	if !ok || raw == "" {
		return Block{}, false, nil
	}
	var w wireProduction
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return Block{}, false, fmt.Errorf("%w: undecodable record: %v", ErrBadProduction, err)
	}
	b := Block{
		V:       SchemaVersion,
		Rel:     w.Rel,
		Op:      w.Op,
		Params:  w.Params,
		Repro:   w.Repro,
		Outcome: w.Outcome,
		Seq:     w.Seq,
	}
	return b.Normalized(), true, nil
}
