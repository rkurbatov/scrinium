package store

// Reading a manifest back into the index — the one action the open-time
// reconciliation performs on a file the index does not know (ADR-118).

import (
	"context"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/engine/layout"
)

// IngestManifest reads the manifest file named by digest, verifies it against
// its own name, and writes the row the index is missing.
//
// The verification is what makes deletion decidable elsewhere: a manifest
// file is named by the hash of its bytes, so a write interrupted mid-flight
// cannot pass, and a whole manifest cannot fail. The reconciliation deletes
// only on errs.ErrCorruptedManifest coming out of here, so this method must
// not dress any other failure in that error — a missing key or an I/O fault
// leaves the file alone.
//
// The physical address is derived from the manifest, not looked up: the index
// is precisely what we are repairing, so it cannot be the source. For an
// inline manifest there is no blob and the zero address is correct.
func (s *store) IngestManifest(ctx context.Context, digest domain.ManifestDigest) error {
	cfg := s.snapshotConfig()

	// loadManifestByDigest reads the file, checks bytes against the name
	// (ErrCorruptedManifest on mismatch) and decodes the body.
	m, err := s.contentIO().LoadByDigest(ctx, digest, s.crypto.KeyProvider(), string(cfg.ContentHasher))
	if err != nil {
		return err
	}

	addr := domain.PhysicalAddress{}
	if ref := m.PrimaryBlobRef(); ref != "" {
		path, perr := layout.BlobPath(cfg.PathTopology, domain.BlobTypeRegular, string(ref))
		if perr != nil {
			return fmt.Errorf("store.IngestManifest: blob path for %q: %w", ref, perr)
		}
		addr.Path = path
	}

	if err := s.index.IndexManifest(ctx, m, addr); err != nil {
		return fmt.Errorf("store.IngestManifest: index %q: %w", digest, err)
	}
	return nil
}
