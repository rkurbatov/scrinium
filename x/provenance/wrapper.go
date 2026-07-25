package provenance

import (
	"context"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
)

// extensionName is this extension's stable name: the wrapper descriptor, the
// per-call hint key and the Ext schema key are all this one word.
const extensionName = Key

// derivativeLookup is the narrow read this wrapper needs from the extension's
// own index to guard a Delete: does the artifact still have children. Declared
// as a port rather than a concrete index so the wrapper can be built and
// tested without one — and so a deployment that installs the wrapper without
// the index gets a wrapper that stamps but does not guard, explicitly rather
// than by accident.
type derivativeLookup interface {
	HasChildren(ctx context.Context, id domain.ArtifactID) (bool, error)
}

// Config tunes the extension. The zero value is valid.
type Config struct {
	// SupersedeRel is the one relation kind this extension interprets: the
	// chains it resolves to a head. Empty means DefaultSupersedeRel. A host
	// with its own word for replacement names it here; every other kind stays
	// opaque either way.
	SupersedeRel string

	// GuardDeletes turns on the interim protection of sources: a Delete of an
	// artifact that still has derivatives is refused with ErrHasDerivatives.
	// It requires the index (the lookup), and it exists because the core does
	// not yet account for artifact→artifact edges; when it does, the core's
	// accounting supersedes this guard and the flag goes away.
	GuardDeletes bool
}

func (c Config) supersedeRel() string {
	if c.SupersedeRel == "" {
		return DefaultSupersedeRel
	}
	return c.SupersedeRel
}

// factory builds the provenance data-plane wrapper. It is Behavioral and
// order-free (ADR-75/88): it changes no blob physics and no addressing — only
// what metadata a manifest carries, and whether a delete is allowed.
type factory struct {
	cfg    Config
	lookup derivativeLookup // nil = stamp only, no delete guard
}

// Wrap decorates inner so a Put carrying WithProduction is validated and
// stamped, and — when configured with the index — a Delete of an artifact with
// live derivatives is refused.
func (f factory) Wrap(inner store.DataStore, _ wrapper.Deps) (store.DataStore, error) {
	if f.cfg.GuardDeletes && f.lookup == nil {
		return nil, fmt.Errorf("provenance: GuardDeletes needs the extension's index")
	}
	return &provStore{DataStore: inner, cfg: f.cfg, lookup: f.lookup}, nil
}

// Descriptor self-describes the wrapper for the Rules Engine.
func (f factory) Descriptor() wrapper.Descriptor {
	return wrapper.Descriptor{Name: extensionName, Class: wrapper.Behavioral}
}

var _ wrapper.Factory = factory{}

// provStore is the provenance data plane. It overrides exactly two methods:
// Put, to validate and stamp a production record, and Delete, to hold the
// interim guard over sources. Everything else — reads, walks, blob and
// maintenance operations — falls through to the inner store: provenance
// confines nothing and hides nothing.
type provStore struct {
	store.DataStore
	cfg    Config
	lookup derivativeLookup
}

// Put validates the production record and stamps it into Ext. A Put with no
// record passes through untouched, which is the ordinary case for an origin:
// an artifact that came from outside the system has no production record, and
// that absence is meaningful rather than missing.
//
// Validation happens here, before the core Put, and rejects rather than
// repairs: an artifact is immutable once written, so a half-meant record would
// be permanent. The core still checks the edges themselves (count, empty,
// duplicates) — this checks the meaning attached to them.
func (s *provStore) Put(ctx context.Context, a domain.Artifact, opts ...domain.PutOption) (domain.ArtifactID, error) {
	block, ok, err := hintedProduction(opts)
	if err != nil {
		return "", err
	}
	if !ok {
		return s.DataStore.Put(ctx, a, opts...)
	}

	refs := domain.ApplyPut(opts...).ParentRefs
	if err := block.Validate(len(refs)); err != nil {
		return "", err
	}
	if err := checkDistinct(refs); err != nil {
		return "", err
	}

	// PKey is derived here, not taken from the caller: two callers spelling
	// the same parameters differently must collide, or "already done" would
	// depend on formatting.
	if block.Op != "" {
		pkey, err := ParamsKey(block.Op, block.Params)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrBadProduction, err)
		}
		block.PKey = pkey
	}

	ext, err := stamp(a.Ext, block)
	if err != nil {
		return "", err
	}
	a.Ext = ext
	return s.DataStore.Put(ctx, a, opts...)
}

// Delete refuses an artifact that still has derivatives (ErrHasDerivatives)
// when the guard is configured, and otherwise delegates. This is the interim
// stand-in for the core's reference accounting over artifact→artifact edges:
// once the core refuses such a delete itself, this override becomes redundant
// and is removed. Its weakness is deliberate and worth stating: it only
// protects callers who hold the wrapped store.
func (s *provStore) Delete(ctx context.Context, id domain.ArtifactID) error {
	if !s.cfg.GuardDeletes {
		return s.DataStore.Delete(ctx, id)
	}
	has, err := s.lookup.HasChildren(ctx, id)
	if err != nil {
		return fmt.Errorf("provenance: check derivatives of %s: %w", id, err)
	}
	if has {
		return fmt.Errorf("%s: %w", id, ErrHasDerivatives)
	}
	return s.DataStore.Delete(ctx, id)
}

var _ store.DataStore = (*provStore)(nil)

// checkDistinct rejects the same input listed twice in one record. The core
// rejects duplicate edges too; this check exists so the caller gets the
// provenance error (and the position) for a record-level mistake, rather than
// a manifest-level one from deeper down.
func checkDistinct(refs []domain.HandleRef) error {
	seen := make(map[domain.HandleRef]struct{}, len(refs))
	for i, r := range refs {
		if r == "" {
			return fmt.Errorf("%w: empty input at position %d", ErrBadProduction, i)
		}
		if _, dup := seen[r]; dup {
			return fmt.Errorf("%w: input %s repeated at position %d", ErrBadProduction, r, i)
		}
		seen[r] = struct{}{}
	}
	return nil
}
