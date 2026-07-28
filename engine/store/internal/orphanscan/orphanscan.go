package orphanscan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/layout"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// OrphanReport is the result of a Reconcile pass. Errors collects non-fatal
// per-file failures that neither stop the pass nor block the Store from
// opening.
type OrphanReport struct {
	StagingRemoved int
	// BlobsRemoved stays at zero here: the open-time pass never deletes a
	// blob (ADR-118). Reclaiming blobs belongs to the GC agent and to the
	// media reconciliation, both two-phase and both outside open. The field
	// remains so those passes can report through the same shape.
	BlobsRemoved int
	// ManifestsRemoved counts fragments only — files whose bytes do not hash
	// to their own name, i.e. writes interrupted mid-flight.
	ManifestsRemoved int
	// ManifestsIndexed counts whole manifests the index did not know and the
	// pass read back into it.
	ManifestsIndexed int
	Errors           []error
	Duration         time.Duration
}

// recoverIndex is the slice of index.StoreIndex the pass depends on: one
// question, "does the index know this manifest". Declaring the narrow port
// here keeps reconciliation decoupled from index methods it never calls. Any
// full index.StoreIndex value satisfies it structurally.
type recoverIndex interface {
	ManifestExistsByDigest(ctx context.Context, digest domain.ManifestDigest) (bool, error)
}

// ManifestIngester reads a manifest file the index does not know and writes
// it into the index. Supplied by the Store because reading involves the
// crypto material and the content plane, neither of which belongs here.
//
// The contract that matters: a file whose bytes do not hash to its own name
// must come back as errs.ErrCorruptedManifest, and nothing else may. That is
// the only signal on which the pass deletes.
type ManifestIngester interface {
	IngestManifest(ctx context.Context, digest domain.ManifestDigest) error
}

// Reconcile is the open-time pass over the Location. It is a reconciliation,
// not a cleanup (ADR-118): its job is to make the index a complete picture of
// the manifests on disk, and to sweep the debris of writes that never
// finished.
//
// Three actions, and the difference between them is the point:
//
//  1. staging/* — removed unconditionally. Staging is per-Put and
//     per-process, so anything that survived a restart is garbage from an
//     interrupted write.
//  2. manifests/<x>/<y>/<digest> — reconciled. A file the index already
//     knows is left alone. A file it does not know is READ: a manifest names
//     itself by the hash of its own bytes, so a fragment of an interrupted
//     write fails that check and is removed, while a whole manifest is truth
//     the index has forgotten and is read back into it. This covers a crash
//     between writing the manifest and writing the row, a hand-merged tree,
//     a restore from a copy — all one action.
//  3. blobs — untouched. The index not resolving a ref means the index does
//     not know, not that the bytes are garbage; a blob is only reclaimable
//     when no manifest references it, which is the GC agent's criterion,
//     applied two-phase and with a delay, after completeness is proven.
//
// The pass never deletes a file because the index lacks a row for it. The
// index is derived; the manifest is truth. Deleting truth on a cache's word
// is how an empty index takes the corpus with it.
func Reconcile(ctx context.Context, drv driver.Driver, idx recoverIndex, ing ManifestIngester) (OrphanReport, error) {
	start := time.Now()
	report := OrphanReport{}

	// 1. Sweep .staging/. Unconditional removal: any file here is from a
	// crashed prior process. Per-file Remove errors do not stop the sweep.
	if err := drv.ListObjectsWithModTime(ctx, domain.StagingPrefix, time.Time{},
		func(om driver.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if rmErr := drv.Remove(ctx, om.Path); rmErr != nil {
				report.Errors = append(report.Errors,
					fmt.Errorf("reconcile: staging remove %q: %w", om.Path, rmErr))
				return nil
			}
			report.StagingRemoved++
			return nil
		}); err != nil {
		report.Duration = time.Since(start)
		return report, fmt.Errorf("reconcile: staging sweep: %w", err)
	}

	// 2. Reconcile manifests/. Names are enough for the common case: the
	// file is read only when the index does not know it.
	if err := drv.ListObjectsWithModTime(ctx, "manifests", time.Time{},
		func(om driver.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			digest, err := layout.DigestFromManifestPath(om.Path)
			if err != nil {
				// A name we cannot parse is a file of unknown nature. Not
				// ours to judge, so not ours to delete.
				report.Errors = append(report.Errors,
					fmt.Errorf("reconcile: manifests parse: %w", err))
				return nil
			}
			known, err := idx.ManifestExistsByDigest(ctx, digest)
			if err != nil {
				// Index trouble is not evidence about the file. Leave it be.
				report.Errors = append(report.Errors,
					fmt.Errorf("reconcile: manifests exists %q: %w", digest, err))
				return nil
			}
			if known {
				return nil
			}
			if ing == nil {
				report.Errors = append(report.Errors,
					fmt.Errorf("reconcile: manifest %q unknown to the index and no ingester supplied", digest))
				return nil
			}
			switch err := ing.IngestManifest(ctx, digest); {
			case err == nil:
				report.ManifestsIndexed++
			case errors.Is(err, errs.ErrCorruptedManifest):
				// The bytes do not hash to the name: an interrupted write,
				// never a whole artifact. This is the one deletion the pass
				// performs, and it rests on the file's own arithmetic rather
				// than on anyone's opinion.
				if rmErr := drv.Remove(ctx, om.Path); rmErr != nil {
					report.Errors = append(report.Errors,
						fmt.Errorf("reconcile: fragment remove %q: %w", om.Path, rmErr))
					return nil
				}
				report.ManifestsRemoved++
			default:
				// Unreadable for any other reason — missing key, I/O, a
				// decoder that does not know this schema. Report and keep.
				report.Errors = append(report.Errors,
					fmt.Errorf("reconcile: ingest %q: %w", digest, err))
			}
			return nil
		}); err != nil {
		report.Duration = time.Since(start)
		return report, fmt.Errorf("reconcile: manifests pass: %w", err)
	}

	report.Duration = time.Since(start)
	return report, nil
}

// PublishOrphanReport emits EventOrphanScanCompleted when a Publisher
// is wired. The payload carries counts, not the error values
// themselves. No-op on a nil Publisher.
func PublishOrphanReport(pub event.Publisher, r OrphanReport) {
	if pub == nil {
		return
	}
	pub.Publish(event.Event{
		Type: event.EventOrphanScanCompleted,
		Payload: event.OrphanScanCompletedPayload{
			StagingRemoved:   r.StagingRemoved,
			BlobsRemoved:     r.BlobsRemoved,
			ManifestsRemoved: r.ManifestsRemoved,
			ManifestsIndexed: r.ManifestsIndexed,
			NonFatalErrors:   len(r.Errors),
			Duration:         r.Duration,
		},
	})
}
