package store

// Index readiness: deciding, at open, whether the index may be trusted as a
// complete picture of the manifests on the Location — and therefore whether
// the orphan sweep is allowed to run (ADR-118).
//
// The sweep removes every manifest file absent from the index and every blob
// whose ref the index cannot resolve. That is correct for leftovers of a
// crashed write and catastrophic for an index that simply has not been
// populated yet: an empty index would take the whole corpus with it, and the
// checkpoint blob that restores the index along with it. So completeness is
// never assumed — it is established here, per session, and the sweep is gated
// on it.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/errs"
)

// indexPosture is what an index looks like at open, before anything has been
// done about it.
type indexPosture uint8

const (
	// posturePopulated — the index already holds manifests: it survived a
	// previous session and is trusted as complete for this one.
	posturePopulated indexPosture = iota
	// postureEmpty — the index holds nothing. Whether that is normal or a
	// fault depends on the Location and on the crypto mode.
	postureEmpty
	// postureDamaged — the index could not answer at all.
	postureDamaged
)

// indexPlan is the decision taken at open.
type indexPlan struct {
	// recover asks for the recovery procedure (ADR-118: checkpoint + tail).
	// It is an OPTIMISATION, not a precondition: the reconciliation pass that
	// runs at every open reads every unknown manifest into the index by
	// itself, so an index without a supplied procedure still ends up
	// complete — it just pays a full sweep instead of a checkpoint restore.
	recover bool
	// complete marks the index trustworthy for this session as it stands,
	// with no work to do.
	complete bool
	// err, when non-nil, refuses the open outright.
	err error
}

// manifestProbe is the slice of index.StoreIndex planIndexOpen needs: one
// cheap question, "is there anything at all". Declared narrowly so the
// decision can be unit-tested without a full index.
type manifestProbe interface {
	IterateManifests(ctx context.Context, cb func(domain.Manifest) error) error
}

// errStopProbe unwinds IterateManifests after the first row: presence is all
// the probe needs, and a full walk of a large index would make every open
// proportional to the corpus.
var errStopProbe = errors.New("scrinium: probe stop")

// probeIndex classifies the index without walking it: it asks for one row and
// stops. An error from the walk is damage — the index could not answer a
// question it must always be able to answer.
func probeIndex(ctx context.Context, idx manifestProbe) indexPosture {
	found := false
	err := idx.IterateManifests(ctx, func(domain.Manifest) error {
		found = true
		return errStopProbe
	})
	switch {
	case found:
		return posturePopulated
	case err == nil, errors.Is(err, errStopProbe):
		return postureEmpty
	default:
		return postureDamaged
	}
}

// locationHasManifests reports whether the Location holds at least one
// manifest file. Like the index probe it stops at the first hit: the question
// is existence, not count.
func locationHasManifests(ctx context.Context, drv driver.Driver) (bool, error) {
	found := false
	err := drv.ListObjectsWithModTime(ctx, "manifests", time.Time{},
		func(driver.ObjectMeta) error {
			found = true
			return fs.SkipAll
		})
	if err != nil && !errors.Is(err, fs.SkipAll) {
		return false, err
	}
	return found, nil
}

// indexIsEphemeral reports whether the configuration itself says the index
// does not survive a close. Paranoid forbids an index readable on disk
// (ADR-56), so the only admissible index there is an ephemeral one — and a
// fresh, empty index at open is then the expected state, not a fault.
func indexIsEphemeral(crypto config.ManifestCrypto, idx index.StoreIndex) bool {
	if crypto != config.ManifestCryptoParanoid {
		return false
	}
	r, ok := idx.(index.AtRestReporter)
	return ok && r.AtRest() == index.AtRestEphemeral
}

// planIndexOpen decides what open should do about the index (ADR-118). The
// split that matters is not "empty versus damaged" but *expected versus
// anomalous*:
//
//   - the configuration says the index is ephemeral → recovery is part of
//     opening: run it, silently, every time;
//   - the index was supposed to survive and did not → refuse, and say how to
//     rebuild. Rebuilding silently here would hide the real fault: the wrong
//     path, someone else's store, a disk that lost its file.
//
// allowRecovery is the caller's explicit "yes, rebuild" — the option that
// turns the refusal into the same procedure the ephemeral case runs.
func planIndexOpen(
	ctx context.Context,
	crypto config.ManifestCrypto,
	idx index.StoreIndex,
	drv driver.Driver,
	allowRecovery bool,
) indexPlan {
	posture := probeIndex(ctx, idx)

	if posture == postureDamaged {
		if allowRecovery {
			// The damaged index is never repaired in place and never
			// deleted here: recovery populates a fresh one, and the
			// damaged file stays for post-mortem.
			return indexPlan{recover: true}
		}
		return indexPlan{err: fmt.Errorf(
			"%w: the index could not be read; it is left untouched for inspection — "+
				"rebuild into a fresh index to continue", errs.ErrIndexDamaged)}
	}

	if posture == posturePopulated {
		return indexPlan{complete: true}
	}

	// Empty from here on.
	if indexIsEphemeral(crypto, idx) {
		// Expected: the index never survives a close in this mode.
		return indexPlan{recover: true}
	}

	hasManifests, err := locationHasManifests(ctx, drv)
	if err != nil {
		return indexPlan{err: fmt.Errorf("probe location for manifests: %w", err)}
	}
	if !hasManifests {
		// Nothing to recover and nothing to sweep: a fresh Location is
		// complete by vacuous truth.
		return indexPlan{complete: true}
	}
	if allowRecovery {
		return indexPlan{recover: true}
	}
	return indexPlan{err: fmt.Errorf(
		"%w: the index was expected to survive the last close but is empty, while the "+
			"Location holds manifests; check that this is the intended store and index, "+
			"then allow the rebuild explicitly (store.WithAllowIndexRecovery)",
		errs.ErrIndexIncomplete)}
}
