package provenance

import (
	"bytes"
	"context"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/store"
)

// Evictor removes an artifact whose derivatives are kept — the deliberate
// exception to the rule that a derivative pins its source.
//
// It is not another plane and not a second delete path. Ordinary deletion rules
// apply in full: logical delete, tombstone and its grace period, blob reference
// counting, GC, compaction, retention, the store's deletion policy. Eviction
// adds exactly two things — a precondition (the receipt) and the receipt
// artifact itself. That is why it needs no core change: everything below Delete
// sees nothing new.
//
// It takes the store it was given and does its work through the ordinary data
// plane, so both the extension's guard and any authorization gate wrapped
// around that store apply. Reaching for eviction is deliberate by construction:
// the caller has to build an Evictor and fill in a receipt, and a receipt
// without a stated reason and decider is refused.
type Evictor struct {
	store store.DataStore
	idx   *Index
	cfg   Config
}

// NewEvictor binds an evictor to a store and the extension's index. Pass the
// SAME store handle the application writes through — the wrapped one. Passing
// an unwrapped store would skip the guard and any rights gate, which is exactly
// the mistake this type exists to make hard.
func NewEvictor(s store.DataStore, idx *Index, cfg Config) (*Evictor, error) {
	if s == nil || idx == nil {
		return nil, fmt.Errorf("provenance: evictor needs a store and an index")
	}
	return &Evictor{store: s, idx: idx, cfg: cfg}, nil
}

// Evict writes the receipt and then deletes the artifact.
//
// The order is load-bearing and its failure direction is the safe one: a crash
// between the two leaves a receipt for an artifact that still exists, which is
// harmless and self-correcting on a retry. The reverse order would leave a
// dangling reference with no explanation — the state this mechanism exists to
// prevent.
//
// Idempotent: an artifact that already carries a receipt does not get a second
// one, so a retry after a failed delete (retention still active, deletion
// policy, a crash) re-attempts only the deletion. A receipt is a standing
// decision — when retention expires, the delete goes through.
//
// The receipt is written WITHOUT the caller's session on purpose: rolling back
// an eviction batch would otherwise delete the explanations of artifacts that
// are already gone.
func (e *Evictor) Evict(ctx context.Context, id domain.ArtifactID, spec ReceiptSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}

	// Already explained? Then only the deletion is outstanding.
	if _, has, err := e.idx.Receipt(ctx, id); err != nil {
		return err
	} else if has {
		return e.store.Delete(ctx, id)
	}

	rh, err := e.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("provenance: evict %s: %w", id, err)
	}
	m := rh.Manifest()
	_ = rh.Close()

	doc, err := receiptFor(spec, m).encode()
	if err != nil {
		return err
	}

	if _, err := e.store.Put(ctx, domain.Artifact{Payload: bytes.NewReader(doc)},
		WithProduction(Production{
			Inputs: []Input{{Ref: domain.HandleRef(id), Rel: e.cfg.evictRel()}},
			Op:     EvictOp,
			Repro:  false, // a judgement is not recomputable
		}),
	); err != nil {
		return fmt.Errorf("provenance: write receipt for %s: %w", id, err)
	}

	return e.store.Delete(ctx, id)
}

// ReadReceipt fetches and decodes the receipt explaining an artifact's
// disappearance. It is what a traversal that hit an unresolvable handle asks:
// the answer distinguishes a deliberate decision from data loss.
func (e *Evictor) ReadReceipt(ctx context.Context, evicted domain.ArtifactID) (Receipt, bool, error) {
	rid, has, err := e.idx.Receipt(ctx, evicted)
	if err != nil || !has {
		return Receipt{}, false, err
	}
	rh, err := e.store.Get(ctx, rid)
	if err != nil {
		return Receipt{}, false, fmt.Errorf("provenance: read receipt %s: %w", rid, err)
	}
	defer func() { _ = rh.Close() }()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(rh); err != nil {
		return Receipt{}, false, fmt.Errorf("provenance: read receipt %s: %w", rid, err)
	}
	r, err := DecodeReceipt(buf.Bytes())
	if err != nil {
		return Receipt{}, false, err
	}
	return r, true, nil
}
