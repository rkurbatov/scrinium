package scrub

// The census: counting manifest files against manifest rows (ADR-118).
//
// An index that lost part of its rows cannot be caught by asking it
// anything — it answers, it holds rows, it is not behind on recent writes.
// The only cheap signal is a disagreement in totals: how many manifest files
// the Location holds versus how many rows the index has. Names are enough
// for that, so nothing is read and nothing is decrypted.
//
// The census never deletes and never repairs. A disagreement means the index
// is not the whole truth, which is a reason to reconcile — and a reason for
// the passes that delete by absence to stand down until someone does.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
)

// census is the outcome of one count.
type census struct {
	Files int64
	Rows  int64
}

// Agrees reports whether the index accounts for every manifest on the
// Location. Files may legitimately exceed rows for a moment — a manifest
// written but not yet indexed — but a persistent gap is the signature of a
// lossy index.
func (c census) Agrees() bool { return c.Files == c.Rows }

// takeCensus counts both sides. It walks names only: no manifest is read,
// so the cost is a directory listing rather than a pass over the corpus, and
// no key is required even under an encrypting ManifestCrypto.
func (a *scrubAgent) takeCensus(ctx context.Context) (census, error) {
	var c census

	listErr := a.drv.ListObjectsWithModTime(ctx, "manifests", time.Time{},
		func(driver.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			c.Files++
			return nil
		})
	if listErr != nil && !errors.Is(listErr, fs.SkipAll) {
		return c, fmt.Errorf("census: list manifests: %w", listErr)
	}

	counter, ok := a.idx.(manifestCounter)
	if !ok {
		// A backend that cannot enumerate its manifests cannot be censused;
		// that is a gap in the backend, not a finding about the store.
		return c, errNoCensus
	}
	if err := counter.IterateManifests(ctx, func(domain.Manifest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.Rows++
		return nil
	}); err != nil {
		return c, fmt.Errorf("census: count rows: %w", err)
	}
	return c, nil
}

// manifestCounter is the index surface the census needs. Declared here so
// the scrub agent's own port stays about verification.
type manifestCounter interface {
	IterateManifests(ctx context.Context, cb func(domain.Manifest) error) error
}

// errNoCensus marks "this backend cannot be counted" — reported, never
// treated as a discrepancy.
var errNoCensus = errors.New("scrub: index cannot enumerate manifests")
