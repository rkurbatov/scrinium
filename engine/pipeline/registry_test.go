package pipeline

import (
	"errors"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/errs"
)

type stubFactory struct{ id string }

func (f *stubFactory) NewEncoder(ctx EncodeContext) Encoder      { return nil }
func (f *stubFactory) NewDecoder(_ domain.PipelineStage) Decoder { return nil }

func TestTransformerRegistry_RegisterAndGet(t *testing.T) {
	r := NewTransformerRegistry()
	r.Register("zstd", &stubFactory{id: "zstd"})

	f, err := r.Get("zstd")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if f.(*stubFactory).id != "zstd" {
		t.Errorf("wrong factory returned")
	}
}

func TestTransformerRegistry_UnsupportedAlgorithm(t *testing.T) {
	r := NewTransformerRegistry()
	_, err := r.Get("0e0e0e0e")
	if !errors.Is(err, errs.ErrUnsupportedAlgorithm) {
		t.Fatalf("expected errs.ErrUnsupportedAlgorithm, got %v", err)
	}
}

func TestTransformerRegistry_ChainedRegistration(t *testing.T) {
	r := NewTransformerRegistry().
		Register("a", &stubFactory{id: "a"}).
		Register("b", &stubFactory{id: "b"}).
		Register("c", &stubFactory{id: "c"})

	for _, id := range []string{"a", "b", "c"} {
		f, err := r.Get(id)
		if err != nil {
			t.Errorf("Get(%q): %v", id, err)
			continue
		}
		if f.(*stubFactory).id != id {
			t.Errorf("Get(%q) returned wrong factory", id)
		}
	}
}
