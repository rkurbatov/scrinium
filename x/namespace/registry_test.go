package namespace

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/systemstore"
	"scrinium.dev/errs"
	"scrinium.dev/extension"
)

// memSysStore is an in-memory SystemStore keyed by (scoped) name, storing
// the latest payload per name. Enough to exercise the registry's
// load/persist round-trips through a real ScopedSystemStore.
type memSysStore struct{ items map[string][]byte }

func newMemSysStore() *memSysStore { return &memSysStore{items: map[string][]byte{}} }

func (m *memSysStore) Put(_ context.Context, a systemstore.NamedArtifact) error {
	b, err := io.ReadAll(a.Payload)
	if err != nil {
		return err
	}
	m.items[a.Name] = b
	return nil
}

func (m *memSysStore) Get(_ context.Context, name string) (domain.ReadHandle, error) {
	b, ok := m.items[name]
	if !ok {
		return nil, errs.ErrArtifactNotFound
	}
	return bytesHandle{bytes.NewReader(b)}, nil
}

func (m *memSysStore) Delete(_ context.Context, name string) error {
	delete(m.items, name)
	return nil
}

func (m *memSysStore) Walk(_ context.Context, prefix string, cb func(string, domain.Manifest) error) error {
	for n := range m.items {
		if strings.HasPrefix(n, prefix) {
			if err := cb(n, domain.Manifest{}); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ systemstore.Store = (*memSysStore)(nil)

// bytesHandle adapts a *bytes.Reader to domain.ReadHandle for tests.
type bytesHandle struct{ *bytes.Reader }

func (bytesHandle) Close() error               { return nil }
func (bytesHandle) SupportsRandomAccess() bool { return true }
func (h bytesHandle) ReadAtCtx(_ context.Context, p []byte, off int64) (int, error) {
	return h.ReadAt(p, off)
}
func (bytesHandle) Manifest() domain.Manifest { return domain.Manifest{} }

func newRegistry(t *testing.T) (*Registry, *memSysStore) {
	t.Helper()
	mem := newMemSysStore()
	sys, err := extension.NewScopedSystemStore("namespace", mem)
	if err != nil {
		t.Fatalf("NewScopedSystemStore: %v", err)
	}
	return NewRegistry(sys), mem
}

func TestRegistry_CreateResolveRoundTrip(t *testing.T) {
	r, mem := newRegistry(t)
	ctx := context.Background()

	id, err := r.Create(ctx, "docs")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("Create returned empty id")
	}

	v, err := r.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, ok := v.Resolve("docs")
	if !ok || got != id {
		t.Errorf("Resolve(docs) = %q,%v, want %q,true", got, ok, id)
	}
	name, ok := v.Name(id)
	if !ok || name != "docs" {
		t.Errorf("Name(%q) = %q,%v, want docs,true", id, name, ok)
	}

	// The registry must land under the extension's scoped name.
	if _, ok := mem.items["extension.namespace.registry"]; !ok {
		t.Errorf("registry not stored at scoped name; keys = %v", keys(mem))
	}
}

func TestRegistry_DuplicateNameRejected(t *testing.T) {
	r, _ := newRegistry(t)
	ctx := context.Background()
	if _, err := r.Create(ctx, "docs"); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := r.Create(ctx, "docs")
	if !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("duplicate Create err = %v, want ErrAlreadyExists", err)
	}
}

func TestRegistry_LoadEmpty(t *testing.T) {
	r, _ := newRegistry(t)
	v, err := r.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := v.Resolve("nope"); ok {
		t.Error("empty registry resolved a name")
	}
	if n := len(v.List()); n != 0 {
		t.Errorf("empty registry List = %d, want 0", n)
	}
}

func TestRegistry_Delete(t *testing.T) {
	r, _ := newRegistry(t)
	ctx := context.Background()
	if _, err := r.Create(ctx, "docs"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := r.Delete(ctx, "docs"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	v, _ := r.Load(ctx)
	if _, ok := v.Resolve("docs"); ok {
		t.Error("deleted namespace still resolves")
	}
	if err := r.Delete(ctx, "missing"); !errors.Is(err, errs.ErrArtifactNotFound) {
		t.Errorf("Delete(missing) err = %v, want ErrArtifactNotFound", err)
	}
}

func TestRegistry_MultipleNamespaces(t *testing.T) {
	r, _ := newRegistry(t)
	ctx := context.Background()
	ids := map[string]NamespaceID{}
	for _, name := range []string{"docs", "media", "scratch"} {
		id, err := r.Create(ctx, name)
		if err != nil {
			t.Fatalf("Create(%s): %v", name, err)
		}
		ids[name] = id
	}
	v, _ := r.Load(ctx)
	if len(v.List()) != 3 {
		t.Fatalf("List = %d, want 3", len(v.List()))
	}
	for name, id := range ids {
		if got, ok := v.Resolve(name); !ok || got != id {
			t.Errorf("Resolve(%s) = %q,%v, want %q", name, got, ok, id)
		}
	}
}

func TestRegistry_RejectsBadName(t *testing.T) {
	r, _ := newRegistry(t)
	ctx := context.Background()
	if _, err := r.Create(ctx, ""); err == nil {
		t.Error("Create(empty): want error")
	}
	if _, err := r.Create(ctx, "*"); err == nil {
		t.Error("Create(wildcard): want error")
	}
}

func keys(m *memSysStore) []string {
	out := make([]string, 0, len(m.items))
	for k := range m.items {
		out = append(out, k)
	}
	return out
}

// PointerRef: these fakes never hold pointer artifacts.
func (m *memSysStore) PointerRef(context.Context, string) (domain.ManifestDigest, bool, error) {
	return "", false, nil
}
