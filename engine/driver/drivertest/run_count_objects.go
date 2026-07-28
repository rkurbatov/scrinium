package drivertest

import "testing"

func runCountObjects(t *testing.T, f Factory) {
	// CountObjects counts regular objects recursively under the prefix.
	t.Run("CountsRecursively", func(t *testing.T) {
		d := f.New(t)
		for _, p := range []string{"a", "sub/b", "sub/c", "sub/deep/d"} {
			putBlob(t, d, p, "x")
		}
		n, err := d.CountObjects(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			t.Fatalf("count: got %d, want 4", n)
		}
	})

	// A missing prefix counts as zero, not an error.
	t.Run("MissingPrefixIsZero", func(t *testing.T) {
		d := f.New(t)
		n, err := d.CountObjects(t.Context(), "0e0e0e0e")
		if err != nil {
			t.Fatalf("missing prefix should not error, got %v", err)
		}
		if n != 0 {
			t.Fatalf("count: got %d, want 0", n)
		}
	})
}
