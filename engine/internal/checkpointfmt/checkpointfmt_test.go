package checkpointfmt_test

import (
	"context"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/internal/checkpointfmt"
)

func TestName_ParseID_RoundTrip(t *testing.T) {
	want := time.Date(2026, 6, 6, 17, 4, 5, 123456789, time.UTC)
	name := checkpointfmt.Name(want)
	if name != checkpointfmt.Prefix+"20260606T170405.123456789Z" {
		t.Fatalf("Name = %q", name)
	}
	got, err := checkpointfmt.ParseID(name)
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("round-trip: got %v, want %v", got, want)
	}
}

func TestID_NormalizesToUTC(t *testing.T) {
	zone := time.FixedZone("X", 5*3600)
	local := time.Date(2026, 6, 6, 22, 4, 5, 0, zone) // 17:04:05Z
	if got := checkpointfmt.ID(local); got != "20260606T170405.000000000Z" {
		t.Errorf("ID = %q, want UTC-normalized", got)
	}
}

func TestParseID_RejectsForeign(t *testing.T) {
	if _, err := checkpointfmt.ParseID("other/2026"); err == nil {
		t.Error("expected error for name without prefix")
	}
	if _, err := checkpointfmt.ParseID(checkpointfmt.Prefix + "not-a-time"); err == nil {
		t.Error("expected error for unparseable timestamp")
	}
}

// fakeWalker serves a fixed set of names under any prefix.
type fakeWalker struct {
	names []string
	err   error
}

func (f fakeWalker) Walk(_ context.Context, _ string, cb func(string, domain.Manifest) error) error {
	if f.err != nil {
		return f.err
	}
	for _, n := range f.names {
		if err := cb(n, domain.Manifest{}); err != nil {
			return err
		}
	}
	return nil
}

func TestLatest_PicksNewest_SkipsForeign(t *testing.T) {
	w := fakeWalker{names: []string{
		checkpointfmt.Prefix + "20260101T000000.000000000Z",
		checkpointfmt.Prefix + "20260606T170405.000000000Z", // newest
		checkpointfmt.Prefix + "20260303T120000.000000000Z",
		checkpointfmt.Prefix + "garbage", // skipped
	}}
	name, at, ok, err := checkpointfmt.Latest(context.Background(), w)
	if err != nil || !ok {
		t.Fatalf("Latest: ok=%v err=%v", ok, err)
	}
	if name != checkpointfmt.Prefix+"20260606T170405.000000000Z" {
		t.Errorf("latest name = %q", name)
	}
	if at.Year() != 2026 || at.Month() != 6 || at.Day() != 6 {
		t.Errorf("latest createdAt = %v", at)
	}
}

func TestLatest_EmptyIsNotAnError(t *testing.T) {
	_, _, ok, err := checkpointfmt.Latest(context.Background(), fakeWalker{})
	if err != nil {
		t.Fatalf("Latest empty: unexpected err %v", err)
	}
	if ok {
		t.Error("ok should be false when no checkpoints exist")
	}
}
