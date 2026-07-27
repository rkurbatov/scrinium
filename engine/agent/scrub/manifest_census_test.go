package scrub

// The census reports; it never acts. These cases pin both halves of that:
// the count is right, and a disagreement changes nothing on disk.

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
)

type censusDriver struct {
	driver.Driver
	manifests []string
	removed   []string
}

func (d *censusDriver) ListObjectsWithModTime(_ context.Context, prefix string, _ time.Time, cb func(driver.ObjectMeta) error) error {
	if prefix != "manifests" {
		return nil
	}
	for _, p := range d.manifests {
		if err := cb(driver.ObjectMeta{Path: p}); err != nil {
			return err
		}
	}
	return nil
}

func (d *censusDriver) Remove(_ context.Context, path string) error {
	d.removed = append(d.removed, path)
	return nil
}

// countingIndex holds a number of rows and can refuse to enumerate at all,
// which is how a backend without the capability behaves.
type countingIndex struct {
	scrubIndex
	rows      int
	cannotRun bool
}

func (i countingIndex) IterateManifests(_ context.Context, cb func(domain.Manifest) error) error {
	if i.cannotRun {
		return errors.New("backend cannot enumerate")
	}
	for n := 0; n < i.rows; n++ {
		if err := cb(domain.Manifest{}); err != nil {
			return err
		}
	}
	return nil
}

// bareIndex does not implement enumeration at all.
type bareIndex struct{ scrubIndex }

func newCensusAgent(drv driver.Driver, idx scrubIndex) *scrubAgent {
	return &scrubAgent{drv: drv, idx: idx}
}

func TestCensus_AgreesWhenIndexHoldsEveryManifest(t *testing.T) {
	drv := &censusDriver{manifests: []string{"manifests/a/b/1", "manifests/c/d/2"}}
	a := newCensusAgent(drv, countingIndex{rows: 2})

	c, err := a.takeCensus(context.Background())
	if err != nil {
		t.Fatalf("takeCensus: %v", err)
	}
	if c.Files != 2 || c.Rows != 2 {
		t.Fatalf("census: got %+v, want 2/2", c)
	}
	if !c.Agrees() {
		t.Error("equal counts must agree")
	}
}

// The case the census exists for: an index that answers, holds rows, and is
// still missing some. Nothing about it is repaired here — and nothing is
// deleted, which is the important half.
func TestCensus_DisagreesOnALossyIndex(t *testing.T) {
	drv := &censusDriver{manifests: []string{"manifests/a/b/1", "manifests/c/d/2", "manifests/e/f/3"}}
	a := newCensusAgent(drv, countingIndex{rows: 1})

	c, err := a.takeCensus(context.Background())
	if err != nil {
		t.Fatalf("takeCensus: %v", err)
	}
	if c.Agrees() {
		t.Fatalf("3 files against 1 row must disagree: %+v", c)
	}
	if len(drv.removed) != 0 {
		t.Fatalf("the census must not remove anything: %v", drv.removed)
	}
}

func TestCensus_BackendWithoutEnumeration(t *testing.T) {
	drv := &censusDriver{manifests: []string{"manifests/a/b/1"}}
	a := newCensusAgent(drv, bareIndex{})

	_, err := a.takeCensus(context.Background())
	if !errors.Is(err, errNoCensus) {
		t.Fatalf("got %v, want errNoCensus", err)
	}
}

func TestCensus_EnumerationFailureIsNotADiscrepancy(t *testing.T) {
	drv := &censusDriver{manifests: []string{"manifests/a/b/1"}}
	a := newCensusAgent(drv, countingIndex{cannotRun: true})

	_, err := a.takeCensus(context.Background())
	if err == nil {
		t.Fatal("an enumeration failure must surface as an error")
	}
	if errors.Is(err, errNoCensus) {
		t.Fatal("a failing enumeration is not the same as a backend that has none")
	}
}
