package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/errs"
)

// probeStub answers the one question planIndexOpen asks of an index.
type probeStub struct {
	index.StoreIndex
	rows    int
	failErr error
	profile index.AtRest
}

func (p probeStub) IterateManifests(_ context.Context, cb func(domain.Manifest) error) error {
	if p.failErr != nil {
		return p.failErr
	}
	for i := 0; i < p.rows; i++ {
		if err := cb(domain.Manifest{}); err != nil {
			return err
		}
	}
	return nil
}

func (p probeStub) AtRest() index.AtRest { return p.profile }

// ephemeralIndex reports itself ephemeral; a bare probeStub reports nothing,
// which is how an unknown backend behaves.
func ephemeralIndex(rows int) index.StoreIndex {
	return probeStub{rows: rows, profile: index.AtRestEphemeral}
}

func TestProbeIndex(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("backend gone")

	if got := probeIndex(ctx, probeStub{rows: 3}); got != posturePopulated {
		t.Errorf("populated index: got %v", got)
	}
	if got := probeIndex(ctx, probeStub{rows: 0}); got != postureEmpty {
		t.Errorf("empty index: got %v", got)
	}
	if got := probeIndex(ctx, probeStub{failErr: boom}); got != postureDamaged {
		t.Errorf("failing index: got %v", got)
	}
}

func TestProbeIndexStopsAtFirstRow(t *testing.T) {
	// Presence, not count: a million-row index must not cost a million
	// callbacks on every open.
	seen := 0
	idx := probeStub{rows: 1000}
	_ = probeIndex(context.Background(), stopCounter{probeStub: idx, seen: &seen})
	if seen != 1 {
		t.Fatalf("callback ran %d times, want 1", seen)
	}
}

type stopCounter struct {
	probeStub
	seen *int
}

func (s stopCounter) IterateManifests(_ context.Context, cb func(domain.Manifest) error) error {
	for i := 0; i < s.rows; i++ {
		*s.seen++
		if err := cb(domain.Manifest{}); err != nil {
			return err
		}
	}
	return nil
}

func TestIndexIsEphemeral(t *testing.T) {
	cases := []struct {
		name   string
		crypto config.ManifestCrypto
		idx    index.StoreIndex
		want   bool
	}{
		{"paranoid + ephemeral", config.ManifestCryptoParanoid, ephemeralIndex(0), true},
		{"paranoid + plaintext index", config.ManifestCryptoParanoid, probeStub{profile: index.AtRestPlaintext}, false},
		{"paranoid + index that reports nothing", config.ManifestCryptoParanoid, silentIndex{}, false},
		{"plain + ephemeral", config.ManifestCryptoPlain, ephemeralIndex(0), false},
		{"sealed + ephemeral", config.ManifestCryptoSealed, ephemeralIndex(0), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := indexIsEphemeral(tc.crypto, tc.idx); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSettleIndexWithoutProcedureDefersToThePass(t *testing.T) {
	// No procedure supplied is not an error: the reconciliation that runs at
	// every open reads unknown manifests back in by itself. What must NOT
	// happen is the index being called complete before that work is done.
	s := &store{}
	if err := settleIndex(context.Background(), s, indexPlan{recover: true}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if s.indexComplete {
		t.Fatal("completeness must be established by the pass, not claimed in advance")
	}
}

func TestSettleIndexRunsSuppliedProcedureAsFastPath(t *testing.T) {
	ran := false
	s := &store{recoverIndex: recovererFunc(func(context.Context) error {
		ran = true
		return nil
	})}
	if err := settleIndex(context.Background(), s, indexPlan{recover: true}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if !ran {
		t.Error("recovery procedure was not invoked")
	}
	if !s.indexComplete {
		t.Error("index not marked complete after recovery")
	}
}

func TestPlanIndexOpenRefusesEmptyPersistentIndex(t *testing.T) {
	// The anomalous case: the index was expected to survive and did not,
	// while the Location holds manifests. Refuse and name the way out.
	plan := planIndexOpen(context.Background(), config.ManifestCryptoPlain,
		probeStub{profile: index.AtRestPlaintext}, manifestsPresentDriver{}, false)
	if !errors.Is(plan.err, errs.ErrIndexIncomplete) {
		t.Fatalf("got %v, want ErrIndexIncomplete", plan.err)
	}
	if plan.recover || plan.complete {
		t.Fatalf("a refusal must not also plan work: %+v", plan)
	}
}

func TestPlanIndexOpenAllowsRebuildWhenPermitted(t *testing.T) {
	plan := planIndexOpen(context.Background(), config.ManifestCryptoPlain,
		probeStub{profile: index.AtRestPlaintext}, manifestsPresentDriver{}, true)
	if plan.err != nil {
		t.Fatalf("explicit permission should not refuse: %v", plan.err)
	}
	if !plan.recover {
		t.Fatal("explicit permission should plan recovery")
	}
}

func TestSettleIndexPropagatesRefusal(t *testing.T) {
	s := &store{}
	want := errors.New("refused")
	if err := settleIndex(context.Background(), s, indexPlan{err: want}); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if s.indexComplete {
		t.Fatal("index marked complete after a refusal")
	}
}

func TestSettleIndexMarksCompleteWithoutRecovery(t *testing.T) {
	s := &store{}
	if err := settleIndex(context.Background(), s, indexPlan{complete: true}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if !s.indexComplete {
		t.Error("populated index not marked complete")
	}
}

// silentIndex implements no reporting capability at all — the shape of an
// unknown backend, which must never be taken for ephemeral.
type silentIndex struct{ index.StoreIndex }

func (silentIndex) IterateManifests(context.Context, func(domain.Manifest) error) error { return nil }

// manifestsPresentDriver reports one manifest on the Location and nothing
// else; the pass only ever asks "is there at least one".
type manifestsPresentDriver struct{ driver.Driver }

func (manifestsPresentDriver) ListObjectsWithModTime(_ context.Context, prefix string, _ time.Time, cb func(driver.ObjectMeta) error) error {
	if prefix != "manifests" {
		return nil
	}
	return cb(driver.ObjectMeta{Path: "manifests/ab/cd/abcdef01"})
}

type recovererFunc func(context.Context) error

func (f recovererFunc) RecoverIndex(ctx context.Context) error { return f(ctx) }
