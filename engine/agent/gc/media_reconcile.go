package gc

// Media reconciliation: the one class of unreferenced bytes the ordinary
// Mark+Sweep cycle cannot see (ADR-118).
//
// The cycle works from the index outwards — a blob whose ref_count fell to
// zero. That covers everything that was ever indexed. But a write that
// crashed between renaming the blob into place and writing the manifest
// leaves a file that was NEVER indexed: no row, so nothing to decrement, so
// nothing for the cycle to notice. The open-time pass does not touch it
// either — it refuses to judge blobs by the index's word (ADR-118) — so
// without this phase the file leaks forever.
//
// Three conditions make the judgement safe, and all three are required:
//
//  1. The index has proven completeness in this session. Without it,
//     "the index does not know this ref" means "the index does not know",
//     which is not evidence about the bytes.
//  2. Deletion stays two-phase. The file is tombstoned and removed a grace
//     period later, so a writer that is mid-Put right now — its blob on
//     disk, its manifest not yet written — is not overtaken.
//  3. It runs as maintenance, never at open. Opening a store must not
//     turn into a walk over every blob.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/layout"
	"scrinium.dev/errs"
)

// indexCompletenessReporter is the seam through which the agent learns
// whether the index may be treated as the whole truth about this Location.
// The Store establishes it per session by reconciling the manifests
// (ADR-118); it is deliberately not part of the Store facade — only the
// passes that delete by absence have any use for it.
type indexCompletenessReporter interface {
	IndexComplete() bool
}

// reconcileMedia walks the blob files and tombstones those the index has
// never heard of. Returns the number of files newly marked.
//
// A file whose name does not parse is left alone: its nature is unknown, and
// unknown is not garbage.
func (a *gcAgent) reconcileMedia(ctx context.Context, stats *GCStats) error {
	reporter, ok := a.store.(indexCompletenessReporter)
	if !ok || !reporter.IndexComplete() {
		// Not a failure: the phase simply has no right to run yet. The next
		// cycle after a completed reconciliation will do the work.
		return nil
	}

	var unknown []string
	listErr := a.drv.ListObjectsWithModTime(ctx, "blobs", time.Time{},
		func(om driver.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			ref, err := layout.RefFromBlobPath(om.Path)
			if err != nil {
				return nil // unparseable name: not ours to judge
			}
			_, resolveErr := a.idx.Resolve(ctx, ref)
			switch {
			case errors.Is(resolveErr, errs.ErrArtifactNotFound):
				unknown = append(unknown, om.Path)
			case resolveErr != nil:
				// Index trouble says nothing about the file. Skip it; the
				// next cycle asks again.
				return nil
			}
			return nil
		})
	if listErr != nil && !errors.Is(listErr, fs.SkipAll) {
		return fmt.Errorf("reconcile media: list blobs: %w", listErr)
	}

	// Collect-then-act: MarkTombstone must not run under an open list
	// cursor, the same discipline mark/sweep follow.
	for _, path := range unknown {
		if err := ctx.Err(); err != nil {
			return err
		}
		marked, err := a.drv.IsTombstone(ctx, path)
		if err == nil && marked {
			continue // already waiting out its grace period
		}
		if err := a.drv.MarkTombstone(ctx, path); err != nil {
			if agent.IsCtxErr(err) {
				return err
			}
			continue // transient driver error: next cycle retries
		}
		stats.MarkedBlobs++
	}
	return nil
}
