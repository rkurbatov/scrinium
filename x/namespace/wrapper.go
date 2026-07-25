package namespace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/domain/extpocket"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/errs"
)

// ErrNamespaceConflict is returned when WithNamespace is passed to a Put
// that a scoped wrapper already pins to a namespace (wrapper B). A scoped
// handle's namespace is fixed; it cannot be overridden per call.
var ErrNamespaceConflict = errors.New("namespace: WithNamespace conflicts with the scoped namespace")

// WithNamespace targets a Put at the namespace named name (ADR-79/99). It
// is importable from this package — the only place that knows the word
// "namespace": it writes the name into the generic per-call ext-hint
// channel under this extension's key, and the open wrapper resolves it to
// an nsid and stamps Ext. The core sees only an opaque hint, never a
// namespace.
//
// Errors surface at write time, not here: an unknown name fails in the
// open wrapper (Managed policy, ADR-96 K2); passing it while a scoped
// wrapper pins the namespace is rejected as a conflict.
func WithNamespace(name string) domain.PutOption {
	return domain.WithExtHint(extensionName, name)
}

// hintedNamespace reads this extension's per-call hint (the WithNamespace
// name) from a Put's resolved options. "" means no namespace was named.
func hintedNamespace(opts []domain.PutOption) string {
	return domain.ApplyPut(opts...).ExtHints[extensionName]
}

// openFactory builds the OPEN namespace wrapper (wrapper A) — the
// mandatory data-plane wrapper an UNBOUND namespace extension occupies. It
// lets a Put target any namespace by name via WithNamespace, resolving the
// name to an nsid through the registry and stamping Ext; a Put with no
// WithNamespace hint is left unstamped (belongs to no namespace). It
// confines nothing — reads and listing see the whole store. The factory
// carries the registry (the resolver), bound by the time the first Put
// runs (ADR-101 §4 env delivery precedes any write).
type openFactory struct{ reg *Registry }

// Wrap decorates inner so a per-call WithNamespace name is resolved and
// stamped into Ext on Put.
func (f openFactory) Wrap(inner store.DataStore, _ wrapper.Deps) (store.DataStore, error) {
	if f.reg == nil {
		return nil, fmt.Errorf("namespace: open wrapper has no registry")
	}
	return &openStore{DataStore: inner, reg: f.reg}, nil
}

// Descriptor self-describes the open wrapper for the Rules Engine.
// Behavioral and order-free, same as the scoped wrapper.
func (f openFactory) Descriptor() wrapper.Descriptor {
	return wrapper.Descriptor{Name: extensionName, Class: wrapper.Behavioral}
}

var _ wrapper.Factory = openFactory{}

// openStore is the open-mode data plane: on Put it resolves a per-call
// WithNamespace name to an nsid (via the registry) and stamps it into Ext;
// with no hint it passes through unstamped. It overrides ONLY Put — read,
// walk, and delete are not confined (open mode is the whole store), so
// they fall through to the embedded inner store.
type openStore struct {
	store.DataStore
	reg *Registry
}

// Put stamps the resolved nsid for a WithNamespace name, or passes through
// unchanged when no namespace was named. An unknown name is an error
// (Managed policy, ADR-96 K2) — resolution is lazy here, against the
// registry the assembler bound at open.
func (s *openStore) Put(ctx context.Context, a domain.Artifact, opts ...domain.PutOption) (domain.ArtifactID, error) {
	name := hintedNamespace(opts)
	if name == "" {
		return s.DataStore.Put(ctx, a, opts...)
	}
	view, err := s.reg.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("namespace: resolve %q: %w", name, err)
	}
	id, ok := view.Resolve(name)
	if !ok {
		return "", fmt.Errorf("namespace: WithNamespace %q does not resolve to an existing namespace", name)
	}
	ext, err := stampNSID(a.Ext, id)
	if err != nil {
		return "", err
	}
	a.Ext = ext
	return s.DataStore.Put(ctx, a, opts...)
}

var _ store.DataStore = (*openStore)(nil)

// scopedFactory builds the SCOPED namespace data-plane wrapper (wrapper B)
// bound to one NamespaceID. It is a Behavioral wrapper (ADR-75/88): it
// changes no blob physics, only where writes land and what Walk lists. The
// factory is constructed by a bound Extension (NewScoped), not blank-import
// registered, because it carries the resolved nsid as state.
type scopedFactory struct{ nsid NamespaceID }

// Wrap decorates inner so every write is stamped with the bound nsid and
// listing is confined to it.
func (f scopedFactory) Wrap(inner store.DataStore, _ wrapper.Deps) (store.DataStore, error) {
	if f.nsid == "" {
		return nil, fmt.Errorf("namespace: scoped wrapper has no bound nsid")
	}
	return &scopedStore{DataStore: inner, nsid: f.nsid}, nil
}

// Descriptor self-describes the wrapper for the Rules Engine. namespace is
// Behavioral and order-free (the open behavioral set, ADR-75).
func (f scopedFactory) Descriptor() wrapper.Descriptor {
	return wrapper.Descriptor{Name: extensionName, Class: wrapper.Behavioral}
}

var _ wrapper.Factory = scopedFactory{}

// scopedStore is a one-namespace VIEW of the data plane. It embeds the
// inner DataStore so maintenance/structural methods (Verify*, PutBlob,
// RollbackSession, Capacity) pass through unchanged, and overrides the
// client read/walk/write methods to confine them to the bound namespace:
//
//   - Put    stamps the bound nsid into Ext;
//   - Walk   yields only artifacts of the bound namespace;
//   - Get    returns ErrArtifactNotFound for an artifact outside it;
//   - Delete refuses (ErrArtifactNotFound) an artifact outside it.
//
// This is VIEW confinement, not access control: a holder of the unscoped
// store still sees everything, and RBAC stays a separate concern (M7,
// AuthGate). Within this handle the bound namespace is the whole world.
type scopedStore struct {
	store.DataStore
	nsid NamespaceID
}

// member reports whether a manifest belongs to the bound namespace. An
// artifact with no nsid stamp belongs to no scoped namespace.
func (s *scopedStore) member(m domain.Manifest) bool {
	id, ok, err := nsidOf(m.Ext)
	return err == nil && ok && id == s.nsid
}

// Put stamps the bound nsid into the artifact's Ext (merging with any
// keys other extensions placed there) and forwards to the inner store. A
// scoped store PINS its namespace: passing WithNamespace through it is a
// conflict and is rejected (ErrNamespaceConflict), rather than silently
// overridden — the bound namespace is the whole world of this handle.
func (s *scopedStore) Put(ctx context.Context, a domain.Artifact, opts ...domain.PutOption) (domain.ArtifactID, error) {
	if hintedNamespace(opts) != "" {
		return "", fmt.Errorf("namespace %q: %w", s.nsid, ErrNamespaceConflict)
	}
	ext, err := stampNSID(a.Ext, s.nsid)
	if err != nil {
		return "", err
	}
	a.Ext = ext
	return s.DataStore.Put(ctx, a, opts...)
}

// Walk yields only artifacts of the bound namespace, ignoring the argument:
// a scoped store is the one-namespace view. It delegates to the core's
// extension-agnostic WalkByExt over the nsid projection (proj_ext) — the
// filter runs in the index, no manifest-file I/O, and the "scrinium.namespace"/"nsid"
// coordinates live here in the extension, not in the core.
func (s *scopedStore) Walk(ctx context.Context, cb func(domain.Manifest) error) error {
	return s.DataStore.WalkByExt(ctx, indexName, nsidField, string(s.nsid), cb)
}

// Get returns the artifact only if it belongs to the bound namespace;
// otherwise it is invisible from this view (ErrArtifactNotFound). The
// manifest is available without I/O, so the membership check is cheap.
func (s *scopedStore) Get(ctx context.Context, id domain.ArtifactID, opts ...domain.GetOption) (domain.ReadHandle, error) {
	h, err := s.DataStore.Get(ctx, id, opts...)
	if err != nil {
		return nil, err
	}
	if !s.member(h.Manifest()) {
		_ = h.Close()
		return nil, errs.ErrArtifactNotFound
	}
	return h, nil
}

// Delete refuses an artifact outside the bound namespace (ErrArtifactNotFound):
// a scoped handle cannot delete across namespaces. Membership is read from
// the manifest (no blob I/O) before the inner Delete.
func (s *scopedStore) Delete(ctx context.Context, id domain.ArtifactID) error {
	h, err := s.DataStore.Get(ctx, id)
	if err != nil {
		return err
	}
	member := s.member(h.Manifest())
	_ = h.Close()
	if !member {
		return errs.ErrArtifactNotFound
	}
	return s.DataStore.Delete(ctx, id)
}

var _ store.DataStore = (*scopedStore)(nil)

// stampNSID sets the "nsid" key in an artifact's Ext to id, preserving any
// other keys already present (e.g. a vfsmeta payload). An empty Ext
// becomes a fresh object carrying just the stamp. It errors if Ext is
// present but is not a JSON object.
func stampNSID(ext json.RawMessage, id NamespaceID) (json.RawMessage, error) {
	idJSON, err := json.Marshal(string(id))
	if err != nil {
		return nil, err
	}
	out, err := extpocket.Put(ext, nsidField, idJSON)
	if err != nil {
		return nil, fmt.Errorf("namespace: %w", err)
	}
	return out, nil
}
