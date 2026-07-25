package artifact

import (
	"errors"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/errs"
)

// checkHandleRefs is the shape gate on artifact→artifact edges at the encode
// boundary (ADR-112): every entry names a target, no target repeats. Existence
// is deliberately NOT checked — an edge is declarative, and resolving it here
// would put an index read on the write path and break restore.
func TestCheckHandleRefs(t *testing.T) {
	var (
		a = domain.HandleRef(strings.Repeat("a", 64))
		b = domain.HandleRef(strings.Repeat("b", 64))
		c = domain.HandleRef(strings.Repeat("c", 64))
	)

	cases := []struct {
		name    string
		refs    []domain.HandleRef
		wantErr bool
	}{
		{"no edges", nil, false},
		{"empty slice", []domain.HandleRef{}, false},
		{"single source", []domain.HandleRef{a}, false},
		{"several distinct sources, order preserved", []domain.HandleRef{c, a, b}, false},
		{"nonexistent target is accepted (declarative edge)", []domain.HandleRef{domain.HandleRef(strings.Repeat("f", 64))}, false},

		{"empty ref", []domain.HandleRef{a, "", b}, true},
		{"duplicate target", []domain.HandleRef{a, b, a}, true},
		{"duplicate adjacent", []domain.HandleRef{b, b}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkHandleRefs(domain.Manifest{HandleRefs: tc.refs})
			switch {
			case tc.wantErr && !errors.Is(err, errs.ErrInvalidHandleRef):
				t.Fatalf("want ErrInvalidHandleRef, got %v", err)
			case !tc.wantErr && err != nil:
				t.Fatalf("valid edge array rejected: %v", err)
			}
		})
	}
}

// The count cap and the shape check share one gate, so a manifest that
// violates the cap still reports the cap error (the more specific limit).
func TestCheckRefLimits_CapBeforeShape(t *testing.T) {
	refs := make([]domain.HandleRef, domain.MaxHandleRefs+1)
	for i := range refs {
		refs[i] = "" // also violates shape; the cap must win
	}
	if err := checkRefLimits(domain.Manifest{HandleRefs: refs}); !errors.Is(err, errs.ErrTooManyRefs) {
		t.Fatalf("want ErrTooManyRefs, got %v", err)
	}
}

// A valid edge array must pass the combined gate, so a legitimate production
// record is never blocked at encode time.
func TestCheckRefLimits_ValidEdgesPass(t *testing.T) {
	m := domain.Manifest{
		HandleRefs: []domain.HandleRef{
			domain.HandleRef(strings.Repeat("1", 64)),
			domain.HandleRef(strings.Repeat("2", 64)),
		},
	}
	if err := checkRefLimits(m); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}
