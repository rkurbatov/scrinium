package provenance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"scrinium.dev/domain/extpocket"
)

// Key is the Ext schema key this extension owns: Manifest.Ext["provenance"].
const Key = "provenance"

// SchemaVersion is the current version of the Block shape. It is written into
// every block and checked on read. A reader must accept every historical
// version it ever wrote (the projection rule for Ext schemas), so bumping it
// means teaching Decode the older shape, never dropping it.
const SchemaVersion = 1

// Outcome records how the production attempt ended. A failure is a record like
// any other — an artifact whose payload is the diagnostic (or empty) — so the
// count of consecutive failures for a piece of work is derivable by traversal
// instead of being kept as a counter somewhere mutable.
type Outcome string

const (
	// OutcomeOK — the operation produced the artifact it intended to.
	OutcomeOK Outcome = "ok"
	// OutcomeFailed — the operation ran and failed; this artifact records
	// the attempt, not a result.
	OutcomeFailed Outcome = "failed"
)

// DefaultSupersedeRel is the relation kind whose chains this extension
// resolves mechanically (the only kind it interprets at all). It is
// configurable per store: a host with its own word for replacement sets
// Config.SupersedeRel instead.
const DefaultSupersedeRel = "supersedes"

// Block is the provenance half of a production record — everything the core
// does not know. It is parallel to Manifest.HandleRefs: Rel[i] is the kind of
// edge HandleRefs[i], so the two arrays are always the same length.
type Block struct {
	// V is the schema version of this block.
	V int `json:"v"`

	// Rel is the relation kind per edge, positionally aligned with
	// HandleRefs. Kinds are opaque strings: this package does not define,
	// enumerate or interpret them, with the sole exception of the
	// configured supersede kind.
	Rel []string `json:"rel,omitempty"`

	// Op names the operation that produced the artifact ("ocr", "thumbnail",
	// "distill"). Required whenever there are inputs — an artifact derived
	// from something was derived BY something.
	Op string `json:"op,omitempty"`

	// Params are the operation's parameters, opaque JSON. They matter for two
	// mechanical questions and no others: has this exact work been done
	// before, and what was produced with parameters now superseded.
	Params json.RawMessage `json:"params,omitempty"`

	// PKey is the idempotency key: a hash over Op and the canonical form of
	// Params. Derived, not supplied — the wrapper computes it so two callers
	// spelling the same params differently still collide.
	PKey string `json:"pkey,omitempty"`

	// Repro declares whether the operation is reproducible: whether losing
	// this artifact means recomputing it from its inputs with the same tool,
	// or losing it for good. Extraction, reduction, reformatting, splitting
	// and assembly are reproducible; anything fetched from outside, judged by
	// a human, or answered by a non-deterministic model is not.
	Repro bool `json:"repro"`

	// Outcome is how the attempt ended. Empty decodes as OutcomeOK.
	Outcome Outcome `json:"outcome,omitempty"`

	// Seq orders the outputs when one input is split into many (pages,
	// chunks, frames): each output is its own record sharing the input, and
	// Seq says which one this is. Zero means "not part of a split".
	Seq int `json:"seq,omitempty"`
}

// Normalized returns the block with defaults resolved, so callers of Decode
// never have to special-case an omitted field.
func (b Block) Normalized() Block {
	if b.Outcome == "" {
		b.Outcome = OutcomeOK
	}
	return b
}

// Validate enforces the block's internal shape. It checks alignment with the
// edge count it will accompany — the invariant that makes Rel meaningful —
// and the fields that cannot be absent. It deliberately does not judge the
// VALUES of Rel, Op or Params: their vocabulary is the host's.
func (b Block) Validate(edges int) error {
	if b.V != SchemaVersion {
		return fmt.Errorf("%w: unsupported schema version %d", ErrBadProduction, b.V)
	}
	if len(b.Rel) != edges {
		return fmt.Errorf("%w: %d relation kinds for %d edges",
			ErrBadProduction, len(b.Rel), edges)
	}
	for i, rel := range b.Rel {
		if rel == "" {
			return fmt.Errorf("%w: empty relation kind at position %d", ErrBadProduction, i)
		}
	}
	if edges > 0 && b.Op == "" {
		return fmt.Errorf("%w: %d inputs but no operation named", ErrBadProduction, edges)
	}
	if len(b.Params) > 0 && !json.Valid(b.Params) {
		return fmt.Errorf("%w: params are not valid JSON", ErrBadProduction)
	}
	if b.Seq < 0 {
		return fmt.Errorf("%w: negative sequence number %d", ErrBadProduction, b.Seq)
	}
	return nil
}

// Decode extracts the provenance block from an artifact's Ext. ok=false means
// the manifest carries no provenance record — the ordinary case for artifacts
// written without this extension, and not an error. A block of an unknown
// future version also returns ok=false rather than a guess.
func Decode(ext json.RawMessage) (Block, bool, error) {
	raw, ok, err := extpocket.Get(ext, Key)
	if err != nil {
		return Block{}, false, fmt.Errorf("provenance: %w", err)
	}
	if !ok {
		return Block{}, false, nil
	}
	var b Block
	if err := json.Unmarshal(raw, &b); err != nil {
		return Block{}, false, fmt.Errorf("provenance: decode block: %w", err)
	}
	if b.V != SchemaVersion {
		return Block{}, false, nil
	}
	return b.Normalized(), true, nil
}

// stamp writes b into ext under Key, preserving every other key already there
// (a vfsmeta payload, an nsid stamp).
func stamp(ext json.RawMessage, b Block) (json.RawMessage, error) {
	blockJSON, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("provenance: encode block: %w", err)
	}
	out, err := extpocket.Put(ext, Key, blockJSON)
	if err != nil {
		return nil, fmt.Errorf("provenance: %w", err)
	}
	return out, nil
}

// ParamsKey is the idempotency key for a unit of work: a hash over the
// operation name and the canonical form of its parameters. Two callers that
// spell the same parameters differently — key order, whitespace — produce the
// same key, which is the whole point: "have we already done this" must not
// depend on how the request was written.
//
// The digest is extension-local, not a content address: it never appears in a
// manifest's identity and is not bound to the store's hash registry.
func ParamsKey(op string, params json.RawMessage) (string, error) {
	canon, err := canonJSON(params)
	if err != nil {
		return "", fmt.Errorf("provenance: canonicalize params: %w", err)
	}
	h := sha256.New()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write(canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// canonJSON rewrites v in a canonical form: object keys sorted, no insignificant
// whitespace, arrays left in order (their order is data). Empty input
// canonicalizes to "null" so an absent parameter set still hashes.
//
// The standard encoder does the work: it sorts map keys, and UseNumber keeps
// numeric literals verbatim, so 1 and 1.0 stay different parameters.
func canonJSON(v json.RawMessage) ([]byte, error) {
	if len(bytes.TrimSpace(v)) == 0 {
		return []byte("null"), nil
	}
	var any interface{}
	dec := json.NewDecoder(bytes.NewReader(v))
	dec.UseNumber()
	if err := dec.Decode(&any); err != nil {
		return nil, err
	}
	return json.Marshal(any)
}
