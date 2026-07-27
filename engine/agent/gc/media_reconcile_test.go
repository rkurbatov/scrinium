package gc

// Media reconciliation is a pass that deletes on the index's word, so every
// case here is about the conditions that make that word trustworthy — and
// about staying two-phase, which is what protects a writer that is mid-Put.

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
)

// --- fakes ---------------------------------------------------------------

type fakeMediaDriver struct {
	driver.Driver
	blobs      []string
	marked     map[string]bool
	tombstoned map[string]bool
}

func newFakeMediaDriver(paths ...string) *fakeMediaDriver {
	return &fakeMediaDriver{
		blobs:      paths,
		marked:     map[string]bool{},
		tombstoned: map[string]bool{},
	}
}

func (d *fakeMediaDriver) ListObjectsWithModTime(_ context.Context, prefix string, _ time.Time, cb func(driver.ObjectMeta) error) error {
	if prefix != "blobs" {
		return nil
	}
	for _, p := range d.blobs {
		if err := cb(driver.ObjectMeta{Path: p}); err != nil {
			return err
		}
	}
	return nil
}

func (d *fakeMediaDriver) MarkTombstone(_ context.Context, path string) error {
	d.marked[path] = true
	return nil
}

func (d *fakeMediaDriver) IsTombstone(_ context.Context, path string) (bool, error) {
	return d.tombstoned[path], nil
}

// fakeMediaIndex answers Resolve only for the refs it was given.
type fakeMediaIndex struct {
	known map[string]bool
}

func (i fakeMediaIndex) ListOrphanBlobs(context.Context, func(string) error) error { return nil }

func (i fakeMediaIndex) Resolve(_ context.Context, ref string) (domain.PhysicalAddress, error) {
	if i.known[ref] {
		return domain.PhysicalAddress{Path: "blobs/xx/yy/" + ref}, nil
	}
	return domain.PhysicalAddress{}, errs.ErrArtifactNotFound
}

func (i fakeMediaIndex) DeleteOrphanBlob(context.Context, string) (bool, error) { return false, nil }

// completenessStore reports whether the index may be trusted as whole.
type completenessStore struct {
	store.Store
	complete bool
}

func (s completenessStore) IndexComplete() bool { return s.complete }

// silentStore does not implement the reporter at all — the shape of a store
// that has said nothing about its index.
type silentStore struct{ store.Store }

func newAgentFor(st store.Store, drv driver.Driver, idx gcIndex) *gcAgent {
	return &gcAgent{store: st, drv: drv, idx: idx}
}

// --- cases ---------------------------------------------------------------

const (
	knownBlob   = "blobs/ab/cd/abcdef01"
	unknownBlob = "blobs/de/ad/deadbeef"
	junkFile    = "blobs/aa/bb/not-a-ref"
)

// The core case: a blob nobody references and the index never knew — the
// leftover of a crash between the blob's rename and its manifest.
func TestReconcileMedia_MarksUnknownBlob(t *testing.T) {
	drv := newFakeMediaDriver(knownBlob, unknownBlob)
	idx := fakeMediaIndex{known: map[string]bool{"abcdef01": true}}
	a := newAgentFor(completenessStore{complete: true}, drv, idx)

	var stats GCStats
	if err := a.reconcileMedia(context.Background(), &stats); err != nil {
		t.Fatalf("reconcileMedia: %v", err)
	}
	if !drv.marked[unknownBlob] {
		t.Error("a blob the index never knew must be tombstoned")
	}
	if drv.marked[knownBlob] {
		t.Error("a blob the index knows must not be touched")
	}
	if stats.MarkedBlobs != 1 {
		t.Errorf("MarkedBlobs: got %d, want 1", stats.MarkedBlobs)
	}
}

// Without proven completeness the same file is not evidence of anything:
// "the index does not know it" may simply mean the index knows nothing yet.
func TestReconcileMedia_RefusesWithoutProvenIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   store.Store
	}{
		{"index not proven complete", completenessStore{complete: false}},
		{"store says nothing about its index", silentStore{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drv := newFakeMediaDriver(unknownBlob)
			a := newAgentFor(tc.st, drv, fakeMediaIndex{known: map[string]bool{}})

			var stats GCStats
			if err := a.reconcileMedia(context.Background(), &stats); err != nil {
				t.Fatalf("reconcileMedia: %v", err)
			}
			if len(drv.marked) != 0 {
				t.Fatalf("nothing may be marked: %v", drv.marked)
			}
			if stats.MarkedBlobs != 0 {
				t.Errorf("MarkedBlobs: got %d, want 0", stats.MarkedBlobs)
			}
		})
	}
}

// Marking is the first of two phases: the file is not removed here, and a
// file already waiting out its grace period is not marked twice.
func TestReconcileMedia_StaysTwoPhase(t *testing.T) {
	drv := newFakeMediaDriver(unknownBlob)
	drv.tombstoned[unknownBlob] = true
	a := newAgentFor(completenessStore{complete: true}, drv, fakeMediaIndex{known: map[string]bool{}})

	var stats GCStats
	if err := a.reconcileMedia(context.Background(), &stats); err != nil {
		t.Fatalf("reconcileMedia: %v", err)
	}
	if len(drv.marked) != 0 {
		t.Errorf("an already-tombstoned file must not be marked again: %v", drv.marked)
	}
	if stats.MarkedBlobs != 0 {
		t.Errorf("MarkedBlobs: got %d, want 0", stats.MarkedBlobs)
	}
}

// A name the layout cannot parse belongs to something we did not write.
func TestReconcileMedia_LeavesUnparseableName(t *testing.T) {
	drv := newFakeMediaDriver(junkFile)
	a := newAgentFor(completenessStore{complete: true}, drv, fakeMediaIndex{known: map[string]bool{}})

	var stats GCStats
	if err := a.reconcileMedia(context.Background(), &stats); err != nil {
		t.Fatalf("reconcileMedia: %v", err)
	}
	if len(drv.marked) != 0 {
		t.Fatalf("a file with an unparseable name must be left alone: %v", drv.marked)
	}
}
