package extension

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/engine/systemstore"
)

// scopeRoot is the reserved first token of every scoped system-artifact
// name. An extension's private space is "extension.<name>."; the engine's
// own artifacts (config, checkpoints, leases) never carry it, so the two
// never collide.
const scopeRoot = "extension."

// ScopedSystemStore is a SystemStore handle confined to one extension's
// private name space. Every name an extension passes is transparently
// prefixed with "extension.<name>." before it reaches the backing
// SystemStore, and that prefix is stripped from the names Walk reports.
// An extension therefore addresses its own artifacts by short, local
// names and cannot read, write, or enumerate another extension's (or the
// engine's) artifacts: the prefix is prepended here, never supplied by
// the caller, so there is no way to escape the scope.
//
// It satisfies systemstore.Store, so an extension can pass it anywhere a
// SystemStore is expected — e.g. as the backing store of a versioned
// registry built on the named Keep contract (ADR-85/101).
type ScopedSystemStore struct {
	name   string
	prefix string
	sys    systemstore.Store
	veto   SystemArtifactValidator
}

var _ systemstore.Store = (*ScopedSystemStore)(nil)

// NewScopedSystemStore confines sys to the extension named name. name
// must be a single clean token — non-empty and free of '.', '/', and
// whitespace — so one extension's scope can neither overlap nor escape
// another's.
func NewScopedSystemStore(name string, sys systemstore.Store, opts ...ScopedOption) (*ScopedSystemStore, error) {
	if err := validateScopeName(name); err != nil {
		return nil, err
	}
	if sys == nil {
		return nil, fmt.Errorf("extension: scoped system store %q: nil backing store", name)
	}
	s := &ScopedSystemStore{name: name, prefix: scopeRoot + name + ".", sys: sys}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// ScopedOption configures a ScopedSystemStore at construction.
type ScopedOption func(*ScopedSystemStore)

// WithValidator installs a pre-write veto (ADR-108): before any Put delegates
// to the backing store, the validator inspects the proposed write and may
// reject it, in which case nothing is written. The assembler installs it only
// on the owning extension's scope, so another extension's store has no veto.
func WithValidator(v SystemArtifactValidator) ScopedOption {
	return func(s *ScopedSystemStore) { s.veto = v }
}

// Name reports the extension scope this handle is bound to.
func (s *ScopedSystemStore) Name() string { return s.name }

// Put writes a with its Name scoped to this extension. When a pre-write
// validator is installed (ADR-108), it runs first and may veto the write —
// a rejection aborts the Put atomically, before anything reaches the backing
// store.
func (s *ScopedSystemStore) Put(ctx context.Context, a systemstore.NamedArtifact) error {
	scoped, err := s.scopeName(a.Name)
	if err != nil {
		return err
	}

	if s.veto != nil {
		// Buffer the inline payload so the validator can inspect it, then hand
		// Put a fresh reader (ReadAll drained the original). External-ref
		// artifacts carry no inline body, so proposed stays nil. The validator
		// sees the LOCAL name and reads its own committed state through s.
		var proposed []byte
		if a.ExternalRef == "" && a.Payload != nil {
			body, rerr := io.ReadAll(a.Payload)
			if rerr != nil {
				return fmt.Errorf("extension %q: buffer system payload for validation: %w", s.name, rerr)
			}
			proposed = body
			a.Payload = bytes.NewReader(body)
		}
		if verr := s.veto.ValidateSystemWrite(ctx, a.Name, proposed, s); verr != nil {
			return verr
		}
	}

	a.Name = scoped
	return s.sys.Put(ctx, a)
}

// Get opens the named artifact within this extension's scope.
func (s *ScopedSystemStore) Get(ctx context.Context, name string) (domain.ReadHandle, error) {
	scoped, err := s.scopeName(name)
	if err != nil {
		return nil, err
	}
	return s.sys.Get(ctx, scoped)
}

// Delete removes the named artifact within this extension's scope.
func (s *ScopedSystemStore) Delete(ctx context.Context, name string) error {
	scoped, err := s.scopeName(name)
	if err != nil {
		return err
	}
	return s.sys.Delete(ctx, scoped)
}

// Walk enumerates the extension's artifacts whose local name starts with
// prefix (prefix "" walks the whole scope). Names handed to cb are local:
// the "extension.<name>." prefix is stripped, so the callback sees the
// same short names the extension wrote.
func (s *ScopedSystemStore) Walk(ctx context.Context, prefix string, cb func(name string, m domain.Manifest) error) error {
	return s.sys.Walk(ctx, s.prefix+prefix, func(name string, m domain.Manifest) error {
		return cb(strings.TrimPrefix(name, s.prefix), m)
	})
}

// scopeName prefixes a non-empty local name with the extension scope.
// PointerRef scopes the name and forwards. Present for interface parity: an
// extension's own pointer artifacts resolve the same way the engine's do.
func (s *ScopedSystemStore) PointerRef(ctx context.Context, name string) (domain.ManifestDigest, bool, error) {
	scoped, err := s.scopeName(name)
	if err != nil {
		return "", false, err
	}
	return s.sys.PointerRef(ctx, scoped)
}

func (s *ScopedSystemStore) scopeName(local string) (string, error) {
	if local == "" {
		return "", fmt.Errorf("extension %q: empty artifact name", s.name)
	}
	return s.prefix + local, nil
}

// validateScopeName enforces that a scope token is a single clean segment,
// so it cannot overlap or escape another extension's space.
func validateScopeName(name string) error {
	if name == "" {
		return fmt.Errorf("extension: empty scope name")
	}
	if strings.ContainsAny(name, "./ \t\n") {
		return fmt.Errorf("extension: scope name %q must be a single token (no '.', '/', or whitespace)", name)
	}
	return nil
}
