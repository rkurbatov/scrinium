package drivertest

import (
	"io/fs"
	"testing"
	"time"

	"scrinium.dev/engine/driver"
)

func runListObjects(t *testing.T, f Factory) {
	// With a zero `since`, every object is yielded; iteration is
	// recursive across the prefix subtree.
	t.Run("ZeroSinceYieldsAll", func(t *testing.T) {
		d := f.New(t)
		putBlob(t, d, "a", "x")
		putBlob(t, d, "sub/b", "y")
		putBlob(t, d, "sub/deep/c", "z")

		var seen int
		err := d.ListObjectsWithModTime(t.Context(), "", time.Time{}, func(driver.ObjectMeta) error {
			seen++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if seen != 3 {
			t.Fatalf("got %d objects, want 3", seen)
		}
	})

	// A `since` in the future excludes everything written now.
	t.Run("FutureSinceYieldsNone", func(t *testing.T) {
		d := f.New(t)
		putBlob(t, d, "a", "x")
		putBlob(t, d, "b", "y")

		future := time.Now().Add(time.Hour)
		err := d.ListObjectsWithModTime(t.Context(), "", future, func(m driver.ObjectMeta) error {
			t.Errorf("unexpected object %q with a future cutoff", m.Path)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	// Returning fs.SkipAll stops iteration without surfacing an error.
	t.Run("StopWalk", func(t *testing.T) {
		d := f.New(t)
		for i := 0; i < 5; i++ {
			putBlob(t, d, string(rune('a'+i)), "x")
		}
		var seen int
		err := d.ListObjectsWithModTime(t.Context(), "", time.Time{}, func(driver.ObjectMeta) error {
			seen++
			if seen == 2 {
				return fs.SkipAll
			}
			return nil
		})
		if err != nil {
			t.Fatalf("fs.SkipAll should be swallowed, got %v", err)
		}
		if seen != 2 {
			t.Fatalf("expected to stop at 2, saw %d", seen)
		}
	})

	// A missing prefix is an empty walk, not an error.
	t.Run("MissingPrefixIsEmpty", func(t *testing.T) {
		d := f.New(t)
		err := d.ListObjectsWithModTime(t.Context(), "0e0e0e0e", time.Time{}, func(m driver.ObjectMeta) error {
			t.Errorf("callback should not be invoked, got %v", m)
			return nil
		})
		if err != nil {
			t.Fatalf("missing prefix should be empty walk, got %v", err)
		}
	})
}
