package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	store2 "scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/layout"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// Delete logically removes an artifact from the Store. It does not free
// physical bytes — that is the GC Agent's job.
//
// Currently supported:
//   - BlobManifest only (TOC needs the chunker decorator to read chunk
//     refs from the TOC blob).
//   - Inline blobs are removed by deleting the manifest row alone —
//     there is no blobs row to decrement.
//   - Target blobs decrement the single ref_count.
//   - Pack manifests are invisible to clients and surface as
//     errs.ErrArtifactNotFound; GC touches them, not client Delete.
//
// Order of operations:
//  1. Load manifest.
//  2. Retention check — defends the artifact regardless of Store policy.
//  3. DeletionPolicy check — Store-level toggle.
//  4. Driver.Remove(manifestPath) — the truth on disk stops saying "exists".
//  5. StoreIndex.DeleteManifest — one transaction: decrement blob
//     ref_counts and remove the manifest row.
//  6. EventArtifactDeleted — only after everything succeeded.
//
// A crash between (4) and (5) leaves an index row without its manifest
// file: a stale cache entry, dropped by the open-time reconciliation
// (ADR-118). The reverse order would leave the file without a row, which
// that same reconciliation would read back in — resurrecting a deleted
// artifact. Truth first, cache second.
func (s *store) Delete(ctx context.Context, id domain.ArtifactID) error {
	if err := s.enterWrite(ctx); err != nil {
		return err
	}

	manifest, err := s.loadManifest(ctx, id)
	if err != nil {
		return err // errs.ErrArtifactNotFound or errs.ErrCorruptedManifest
	}

	// Handleless manifests are not user-visible (ADR-83).
	if err := guardHandleless(manifest); err != nil {
		return err
	}

	// Retention precedes policy: retention is artifact-level protection,
	// stronger than the Store-level policy. A retention-protected
	// artifact is refused before the policy is even consulted.
	if !manifest.RetentionUntil.IsZero() && manifest.RetentionUntil.After(time.Now()) {
		return errs.ErrRetentionNotExpired
	}

	cfg := s.snapshotConfig()
	if cfg.DeletionPolicy == store2.DeletionPolicyNoDelete {
		return errs.ErrDeletionForbidden
	}

	// File first, index second (ADR-30 as revised by ADR-118). The manifest
	// on disk is the truth and the index is a cache of it, so the crash
	// window must never leave truth saying "present" while the cache says
	// "deleted": the open-time reconciliation reads an unknown manifest back
	// INTO the index, which would resurrect a deleted artifact. Removing the
	// file first inverts the leftover — a row without a file, i.e. a stale
	// cache entry, which the same reconciliation drops. The cost of a crash
	// in this window is inflated ref_counts (blobs reclaimed later, by the
	// media reconciliation) — a leak instead of a resurrection.
	manifestPath, err := layout.ManifestPath(manifest.Digest)
	if err != nil {
		return fmt.Errorf("store.Delete: manifest path: %w", err)
	}
	if err := s.drv.Remove(ctx, manifestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Nothing has changed yet: the row still stands and the artifact is
		// still readable. Surfacing the error leaves a consistent store.
		return s.traceErr(ctx, "Delete", fmt.Errorf("store.Delete: remove manifest file: %w", err), artifactIDAttr(id), slog.String("stage", "remove"))
	}

	// Deletion is keyed by digest; the index derives the blobs to decrement
	// from manifest_blobs (Inline has no edges; Target has its one blob).
	// A failure here leaves a row pointing at a file that is gone — a stale
	// cache entry, dropped by the reconciliation that follows an open.
	if err := s.index.DeleteManifest(ctx, manifest.Digest); err != nil {
		return s.traceErr(ctx, "Delete", fmt.Errorf("store.Delete: index: %w", err), artifactIDAttr(id), slog.String("stage", "index"))
	}

	s.publish(event.EventArtifactDeleted, event.ArtifactDeletedPayload{ArtifactID: id})
	s.componentLogger("store").LogAttrs(ctx, slog.LevelDebug, "artifact deleted",
		storeIDAttr(s), artifactIDAttr(id),
		slog.String("blob_storage", manifest.LayoutHeader.BlobStorage))
	return nil
}
