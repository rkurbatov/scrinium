package extension

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/systemstore"
)

// fakeValidator records what the veto saw and returns a configurable verdict.
type fakeValidator struct {
	called   bool
	gotName  string
	gotBody  []byte
	gotStore systemstore.Store
	reject   error
}

func (f *fakeValidator) ValidateSystemWrite(_ context.Context, name string, proposed []byte, current systemstore.Store) error {
	f.called = true
	f.gotName = name
	f.gotBody = proposed
	f.gotStore = current
	return f.reject
}

// capturingSysStore records the payload bytes of the last Put, to prove the
// reader handed to the backing store survived the validator's buffering.
type capturingSysStore struct {
	recordingSysStore
	lastPayload []byte
}

func (c *capturingSysStore) Put(ctx context.Context, a systemstore.NamedArtifact) error {
	if a.Payload != nil {
		b, _ := io.ReadAll(a.Payload)
		c.lastPayload = b
		a.Payload = nil
	}
	return c.recordingSysStore.Put(ctx, a)
}

func TestScopedVeto_RejectBlocksWrite(t *testing.T) {
	rec := &recordingSysStore{}
	deny := errors.New("nope")
	val := &fakeValidator{reject: deny}
	s, err := NewScopedSystemStore("namespace", rec, WithValidator(val))
	if err != nil {
		t.Fatalf("NewScopedSystemStore: %v", err)
	}

	err = s.Put(context.Background(), systemstore.NamedArtifact{Name: "registry"})
	if !errors.Is(err, deny) {
		t.Errorf("Put error = %v, want the veto error", err)
	}
	if !val.called {
		t.Error("validator was not consulted")
	}
	if len(rec.put) != 0 {
		t.Errorf("backing store saw a write despite veto: %v", rec.put)
	}
}

func TestScopedVeto_AcceptWritesAndSeesLocalName(t *testing.T) {
	rec := &recordingSysStore{}
	val := &fakeValidator{}
	s, _ := NewScopedSystemStore("namespace", rec, WithValidator(val))

	if err := s.Put(context.Background(), systemstore.NamedArtifact{Name: "registry"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if val.gotName != "registry" {
		t.Errorf("validator name = %q, want local %q", val.gotName, "registry")
	}
	if val.gotStore == nil {
		t.Error("validator current store is nil")
	}
	const want = "extension.namespace.registry"
	if len(rec.put) != 1 || rec.put[0] != want {
		t.Errorf("backing Put = %v, want [%q]", rec.put, want)
	}
}

func TestScopedVeto_BuffersPayloadAndReplenishes(t *testing.T) {
	cs := &capturingSysStore{}
	val := &fakeValidator{}
	s, _ := NewScopedSystemStore("namespace", cs, WithValidator(val))

	body := []byte("hello-registry")
	if err := s.Put(context.Background(), systemstore.NamedArtifact{
		Name:    "registry",
		Payload: bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !bytes.Equal(val.gotBody, body) {
		t.Errorf("validator proposed = %q, want %q", val.gotBody, body)
	}
	if !bytes.Equal(cs.lastPayload, body) {
		t.Errorf("backing store payload = %q, want %q (reader not replenished)", cs.lastPayload, body)
	}
}

func TestScopedVeto_ExternalRefProposedNil(t *testing.T) {
	rec := &recordingSysStore{}
	val := &fakeValidator{}
	s, _ := NewScopedSystemStore("namespace", rec, WithValidator(val))

	if err := s.Put(context.Background(), systemstore.NamedArtifact{
		Name:        "ref",
		ExternalRef: domain.ManifestDigest("abc00000"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !val.called {
		t.Fatal("validator not consulted for external-ref write")
	}
	if val.gotBody != nil {
		t.Errorf("validator proposed = %q, want nil for external-ref artifact", val.gotBody)
	}
}

func TestScopedVeto_NoValidatorDelegates(t *testing.T) {
	rec := &recordingSysStore{}
	s, _ := NewScopedSystemStore("namespace", rec) // no WithValidator

	if err := s.Put(context.Background(), systemstore.NamedArtifact{Name: "registry"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(rec.put) != 1 {
		t.Errorf("backing Put = %v, want exactly one write", rec.put)
	}
}
