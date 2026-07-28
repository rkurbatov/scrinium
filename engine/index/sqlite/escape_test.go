package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"scrinium.dev/domain"
)

// TestEscapeSQLString covers the single-quote doubling that makes a value
// safe to splice into the VACUUM INTO / ATTACH string literal — those take
// a literal, not a bind parameter, so the escaping is the only defence. The
// o'brien case is the one an ordinary filesystem path can hit.
func TestEscapeSQLString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no_quote", "plain.db", "plain.db"},
		{"single_quote", "o'brien.db", "o''brien.db"},
		{"leading_quote", "'x", "''x"},
		{"trailing_quote", "x'", "x''"},
		{"adjacent_quotes", "a''b", "a''''b"},
		{"only_quote", "'", "''"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := escapeSQLString(c.in); got != c.want {
				t.Errorf("escapeSQLString(%q): got %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestWriteCheckpoint_PathWithSingleQuote is the end-to-end guard: a dest
// path containing a single quote must round-trip through VACUUM INTO
// without a SQL syntax error, proving WriteCheckpoint escapes the literal
// rather than concatenating it raw. Mirrors TestWriteCheckpoint_CreatesCheckpoint
// with a quoted destination.
func TestWriteCheckpoint_PathWithSingleQuote(t *testing.T) {
	idx, _ := newDiskIndex(t)
	insertBlob(t, idx, "b10b0001", strings.Repeat("a", 64), 1024,
		domain.PhysicalAddress{Path: "p"}, 1)
	insertManifest(t, idx, domain.Manifest{
		ArtifactID: "art-1",
		BlobRefs:   []domain.BlobRef{"b10b0001"}, CreatedAt: time.Now(),
	})

	dest := filepath.Join(t.TempDir(), "o'brien.db")
	if err := idx.WriteCheckpoint(context.Background(), dest); err != nil {
		t.Fatalf("WriteCheckpoint to quoted path: %v", err)
	}

	// The checkpoint is a self-contained, openable database with the data —
	// confirms the quoted path was escaped, not mangled.
	snap, err := NewStore(context.Background(), dest)
	if err != nil {
		t.Fatalf("NewStore quoted checkpoint: %v", err)
	}
	if got := countRows(t, snap, "manifests"); got != 1 {
		t.Errorf("checkpoint manifests: got %d, want 1", got)
	}
}
