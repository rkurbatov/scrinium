package extension

import (
	"context"
	"io"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/systemstore"
)

// recordingSysStore is a minimal SystemStore that records the fully
// scoped names it is asked for and serves Walk from a preloaded set.
type recordingSysStore struct {
	put, get, del []string
	walkNames     []string // scoped names available to Walk
}

func (r *recordingSysStore) Put(_ context.Context, a systemstore.NamedArtifact) error {
	if a.Payload != nil {
		_, _ = io.Copy(io.Discard, a.Payload)
	}
	r.put = append(r.put, a.Name)
	return nil
}

func (r *recordingSysStore) Get(_ context.Context, name string) (domain.ReadHandle, error) {
	r.get = append(r.get, name)
	return nil, nil
}

func (r *recordingSysStore) Delete(_ context.Context, name string) error {
	r.del = append(r.del, name)
	return nil
}

func (r *recordingSysStore) Walk(_ context.Context, prefix string, cb func(string, domain.Manifest) error) error {
	for _, n := range r.walkNames {
		if strings.HasPrefix(n, prefix) {
			if err := cb(n, domain.Manifest{}); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ systemstore.Store = (*recordingSysStore)(nil)

func TestScopedSystemStore_PrefixesNames(t *testing.T) {
	rec := &recordingSysStore{}
	s, err := NewScopedSystemStore("namespace", rec)
	if err != nil {
		t.Fatalf("NewScopedSystemStore: %v", err)
	}
	ctx := context.Background()

	if err := s.Put(ctx, systemstore.NamedArtifact{Name: "registry", Keep: systemstore.KeepVersions(3)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Get(ctx, "registry"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := s.Delete(ctx, "registry"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	const want = "extension.namespace.registry"
	if len(rec.put) != 1 || rec.put[0] != want {
		t.Errorf("Put name = %v, want [%q]", rec.put, want)
	}
	if len(rec.get) != 1 || rec.get[0] != want {
		t.Errorf("Get name = %v, want [%q]", rec.get, want)
	}
	if len(rec.del) != 1 || rec.del[0] != want {
		t.Errorf("Delete name = %v, want [%q]", rec.del, want)
	}
}

func TestScopedSystemStore_WalkStripsPrefixAndIsolates(t *testing.T) {
	rec := &recordingSysStore{walkNames: []string{
		"extension.namespace.registry",
		"extension.namespace.queue.0000000001",
		"extension.other.secret",        // another extension's scope
		"config.StoreConfig.0000000001", // the engine's own artifact
	}}
	s, err := NewScopedSystemStore("namespace", rec)
	if err != nil {
		t.Fatalf("NewScopedSystemStore: %v", err)
	}

	var got []string
	if err := s.Walk(context.Background(), "", func(name string, _ domain.Manifest) error {
		got = append(got, name)
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	want := []string{"registry", "queue.0000000001"}
	if len(got) != len(want) {
		t.Fatalf("Walk names = %v, want %v (other scopes and engine artifacts must be invisible)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Walk name[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestScopedSystemStore_WalkSubPrefix(t *testing.T) {
	rec := &recordingSysStore{walkNames: []string{
		"extension.namespace.registry",
		"extension.namespace.queue.0000000001",
		"extension.namespace.queue.0000000002",
	}}
	s, _ := NewScopedSystemStore("namespace", rec)

	var got []string
	_ = s.Walk(context.Background(), "queue.", func(name string, _ domain.Manifest) error {
		got = append(got, name)
		return nil
	})
	if len(got) != 2 {
		t.Fatalf("Walk(\"queue.\") = %v, want 2 entries", got)
	}
}

func TestScopedSystemStore_RejectsBadScopeName(t *testing.T) {
	rec := &recordingSysStore{}
	for _, name := range []string{"", "a.b", "a/b", "a b"} {
		if _, err := NewScopedSystemStore(name, rec); err == nil {
			t.Errorf("NewScopedSystemStore(%q): want error, got nil", name)
		}
	}
}

func TestScopedSystemStore_RejectsEmptyArtifactName(t *testing.T) {
	rec := &recordingSysStore{}
	s, _ := NewScopedSystemStore("namespace", rec)
	ctx := context.Background()
	if err := s.Put(ctx, systemstore.NamedArtifact{Name: ""}); err == nil {
		t.Error("Put with empty name: want error, got nil")
	}
	if _, err := s.Get(ctx, ""); err == nil {
		t.Error("Get with empty name: want error, got nil")
	}
	if err := s.Delete(ctx, ""); err == nil {
		t.Error("Delete with empty name: want error, got nil")
	}
}

func TestScopedSystemStore_NilBacking(t *testing.T) {
	if _, err := NewScopedSystemStore("namespace", nil); err == nil {
		t.Error("NewScopedSystemStore(nil backing): want error, got nil")
	}
}

// PointerRef: these fakes never hold pointer artifacts.
func (r *recordingSysStore) PointerRef(context.Context, string) (domain.ManifestDigest, bool, error) {
	return "", false, nil
}
