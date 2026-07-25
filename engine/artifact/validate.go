package artifact

// validate.go — manifest structural validation (slots and references limits).

import (
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/errs"
)

// checkRefLimits enforces the per-field reference caps (ADR-93): blob_refs
// and handle_refs each fit a 16-bit count, so the chunk/member list is
// bounded at 65535. The encode path has no overall byte cap — it is bounded
// field-by-field; reads are guarded by MaxManifestSize (32 MiB, checked in
// Decode/DecodeEncrypted).
func checkRefLimits(m domain.Manifest) error {
	if len(m.BlobRefs) > domain.MaxBlobRefs || len(m.HandleRefs) > domain.MaxHandleRefs {
		return errs.ErrTooManyRefs
	}
	return checkHandleRefs(m)
}

// checkHandleRefs enforces the SHAPE of the artifact→artifact edges (ADR-112):
// every entry names a target, and a manifest never lists the same target
// twice. It runs at the same encode boundary as checkRefLimits, so a
// structurally invalid edge array is never serialised — whether it came from
// a public Put (WithParentRefs) or from a structural wrapper (bundler members).
//
// What it deliberately does NOT check is existence: an edge is a declaration.
// Resolving targets here would make the write path depend on the index and
// would break restore, where a target's replica may arrive after its child.
// Dangling edges are a graph-integrity concern, detected by traversal, not a
// write-time error.
func checkHandleRefs(m domain.Manifest) error {
	if len(m.HandleRefs) == 0 {
		return nil
	}
	seen := make(map[domain.HandleRef]struct{}, len(m.HandleRefs))
	for i, ref := range m.HandleRefs {
		if ref == "" {
			return fmt.Errorf("%w: empty ref at position %d", errs.ErrInvalidHandleRef, i)
		}
		if _, dup := seen[ref]; dup {
			return fmt.Errorf("%w: duplicate ref %s at position %d",
				errs.ErrInvalidHandleRef, ref, i)
		}
		seen[ref] = struct{}{}
	}
	return nil
}

// validateSlot enforces the manifest slot invariant (ADR-92/104): a manifest
// is exactly one kind, decided by which identity slot is filled, and each kind
// carries the structure its kind requires (a system artifact, in particular,
// is always inline with no blob_ref). It runs at the encode boundary
// (Encode and encodeEncrypted), beside checkRefLimits, so a structurally
// invalid manifest is never serialised.
func validateSlot(m domain.Manifest) error {
	hasHandle := m.ArtifactID != ""
	hasName := m.Name != ""
	hasIdentityMeta := m.IdentityMetaHash != "" || len(m.IdentityNonce) > 0

	switch {
	case hasHandle && hasName:
		return fmt.Errorf("%w: both handle (%s) and name (%q) are set",
			errs.ErrInvalidManifestSlot, m.ArtifactID, m.Name)

	case hasHandle: // user
		if !hasIdentityMeta {
			return fmt.Errorf("%w: user artifact carries no identity-meta",
				errs.ErrInvalidManifestSlot)
		}

	case hasName: // system
		if len(m.InlineBlob) == 0 {
			return fmt.Errorf("%w: system artifact %q has no inline payload",
				errs.ErrInvalidManifestSlot, m.Name)
		}
		if m.LayoutHeader.BlobStorage != domain.LayoutInline {
			return fmt.Errorf("%w: system artifact %q is not inline (%q)",
				errs.ErrInvalidManifestSlot, m.Name, m.LayoutHeader.BlobStorage)
		}
		if hasIdentityMeta {
			return fmt.Errorf("%w: system artifact %q carries identity-meta",
				errs.ErrInvalidManifestSlot, m.Name)
		}

	default: // container — both slots empty
		if len(m.BlobRefs) == 0 {
			return fmt.Errorf("%w: container has no blob_refs",
				errs.ErrInvalidManifestSlot)
		}
		if len(m.InlineBlob) != 0 {
			return fmt.Errorf("%w: container carries an inline blob",
				errs.ErrInvalidManifestSlot)
		}
		if hasIdentityMeta {
			return fmt.Errorf("%w: container carries identity-meta",
				errs.ErrInvalidManifestSlot)
		}
	}

	// Layout coherence (ADR-66/92): inline content is embedded in the body,
	// not a physical blob, so an inline manifest carries no blob_ref
	if m.LayoutHeader.BlobStorage == domain.LayoutInline && len(m.BlobRefs) != 0 {
		return fmt.Errorf("%w: inline manifest carries blob_refs",
			errs.ErrInvalidManifestSlot)
	}

	return nil
}
