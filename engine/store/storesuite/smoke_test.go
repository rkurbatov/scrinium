package storesuite

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/store"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

// TestSmoke_MillionSmallFiles is the M1 exit-criterion smoke:
// writing one million small files on a local SSD must run stably
// with O(1) memory (milestones, M1 exit clause).
//
// We exercise Put -> Walk-count -> Get-roundtrip on N unique
// payloads and assert two things:
//
//  1. Every artifact survives a round-trip: Walk sees N rows
//     and a sample subset reads back byte-identical content.
//
//  2. Heap usage stays bounded. Per Principle 7, "O(1) memory
//     under streaming" — Go-heap delta after a forced GC should
//     be a small constant, not proportional to N. The ceiling
//     below is conservative; if it ever fires, something started
//     accumulating per-artifact state in memory.
//
// This test must always run without -race: the path is single-
// threaded and the race detector adds ~10x overhead for nothing.
// The dedicated `make smoke` target enforces that; do not move
// this test into the generic test runner.
//
// Gated by testing.Short(). Override N via SCRINIUM_SMOKE_N for
// quicker triage runs.
func TestSmoke_MillionSmallFiles(t *testing.T) {
	if os.Getenv("SCRINIUM_SMOKE") != "1" {
		t.Skip("smoke: gated by SCRINIUM_SMOKE=1; run via `make smoke`")
	}

	const (
		// Default of 200k is enough to prove O(1) memory and
		// catch any per-artifact accumulation. The literal spec
		// target is 1M; that takes 12-15 minutes on Mac APFS and
		// is run explicitly via `make smoke N=1000000` before a
		// release, not on every smoke run.
		defaultN = 100_000
		// Each artifact: 32 bytes of unique content. Small enough
		// to keep on-disk total in the low-GB range, large enough
		// to exercise hashing + dedup paths.
		payloadSize = 32
		// Heap delta ceiling. With a disk-backed sqlite index
		// (modernc page cache ~8 MiB) and streaming Put, actual
		// delta on 1M is in the tens of MiB. 128 MiB gives healthy
		// headroom while still firing loudly on accidental O(N)
		// accumulation.
		heapDeltaCeiling = 128 << 20 // 128 MiB
		// Progress reports — emitted via emit() straight to stderr,
		// NOT via t.Logf. The testing package buffers per-test logs
		// until the test ends; gotestsum's "testname" format flushes
		// them in one batch at exit, which is useless for a multi-
		// minute run. Direct stderr writes show up live.
		reportEvery = 10_000
	)

	n := defaultN
	if v, ok := os.LookupEnv("SCRINIUM_SMOKE_N"); ok && v != "" {
		var parsed int
		if _, err := fmt.Sscanf(v, "%d", &parsed); err == nil && parsed > 0 {
			n = parsed
		}
	}
	emit("config: N=%d, payload=%dB, heap-ceiling=%s",
		n, payloadSize, humanize_Bytes(heapDeltaCeiling))

	s, _ := storefx.InitDisk(t)
	ctx := context.Background()

	// --- Baseline heap, before the loop ---
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	emit("baseline HeapAlloc: %s", humanize_Bytes(int64(baseline.HeapAlloc)))

	// --- Put loop ---
	ids := make([]domain.ArtifactID, 0, 3) // first, mid, last for sample Get
	emit("Put: starting %d artifacts", n)
	startPut := time.Now()
	for i := 0; i < n; i++ {
		p := makePayload(i, payloadSize)
		id, err := s.Put(ctx,
			domain.Artifact{Payload: bytes.NewReader(p)})
		if err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
		if i == 0 || i == n/2 || i == n-1 {
			ids = append(ids, id)
		}
		if (i+1)%reportEvery == 0 {
			reportProgress("Put", i+1, n, startPut)
		}
	}
	putElapsed := time.Since(startPut)
	emit("Put: done in %v (avg %.1f Put/s, %.1f us/Put)",
		putElapsed,
		float64(n)/putElapsed.Seconds(),
		float64(putElapsed.Microseconds())/float64(n))

	// --- Heap snapshot after the loop ---
	runtime.GC()
	var afterPut runtime.MemStats
	runtime.ReadMemStats(&afterPut)
	delta := int64(afterPut.HeapAlloc) - int64(baseline.HeapAlloc)
	emit("HeapAlloc after Put: %s (delta from baseline: %s)",
		humanize_Bytes(int64(afterPut.HeapAlloc)), humanize_Bytes(delta))
	if delta > heapDeltaCeiling {
		t.Errorf("heap delta %s exceeds ceiling %s — likely O(N) accumulation",
			humanize_Bytes(delta), humanize_Bytes(int64(heapDeltaCeiling)))
	}

	// --- Walk: every artifact visible through the index ---
	emit("Walk: counting manifests in 'smoke' namespace")
	startWalk := time.Now()
	var seen int
	if err := s.Walk(ctx, func(_ domain.Manifest) error {
		seen++
		if seen%reportEvery == 0 {
			reportProgress("Walk", seen, n, startWalk)
		}
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	emit("Walk: %d manifests in %v (avg %.1f Walk/s)",
		seen, time.Since(startWalk),
		float64(seen)/time.Since(startWalk).Seconds())
	if seen != n {
		t.Errorf("Walk count: got %d, want %d", seen, n)
	}

	// --- Sample Get: round-trip the saved ids ---
	emit("Get: round-trip %d sampled artifacts", len(ids))
	for k, id := range ids {
		var sampleIdx int
		switch k {
		case 0:
			sampleIdx = 0
		case 1:
			sampleIdx = n / 2
		case 2:
			sampleIdx = n - 1
		}
		want := makePayload(sampleIdx, payloadSize)
		rh, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get sample #%d (id=%q): %v", sampleIdx, id, err)
		}
		got := readAllAndClose(t, rh)
		if !bytes.Equal(got, want) {
			t.Errorf("sample #%d: payload mismatch (got %d bytes, want %d)",
				sampleIdx, len(got), len(want))
		}
	}
	emit("Get: all %d samples matched", len(ids))

	emit("smoke OK: %d artifacts, total wall time %v", n, time.Since(startPut))
}

// emit writes one line straight to stderr, bypassing the testing
// package's per-test buffer. Using t.Logf in a long-running test
// means all output appears in one batch at the end — useless for
// live progress.
func emit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "  "+format+"\n", args...)
}

// reportProgress emits one progress line. Same rationale as emit:
// direct stderr writes for live updates.
func reportProgress(op string, done, total int, start time.Time) {
	elapsed := time.Since(start)
	pct := float64(done) / float64(total) * 100
	rate := float64(done) / elapsed.Seconds()
	var eta time.Duration
	if rate > 0 {
		eta = time.Duration(float64(total-done)/rate) * time.Second
	}
	fmt.Fprintf(os.Stderr, "  %s: %d/%d (%.1f%%, %.0f/s, elapsed %v, ETA %v)\n",
		op, done, total, pct, rate,
		elapsed.Truncate(time.Second), eta.Truncate(time.Second))
}

// makePayload returns deterministic payloadSize-byte content: an
// 8-byte little-endian index followed by a recognisable filler.
// Each i yields a unique payload, so dedup never fires and the
// loop measures the worst case for index growth.
func makePayload(i int, size int) []byte {
	if size < 8 {
		panic("payload size must be >= 8")
	}
	p := make([]byte, size)
	binary.LittleEndian.PutUint64(p[:8], uint64(i))
	for j := 8; j < size; j++ {
		p[j] = byte(0xA0 | (j & 0x0F))
	}
	return p
}

func readAllAndClose(t *testing.T, rh domain.ReadHandle) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rh); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if err := rh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// newEncryptedDiskStore creates a disk-backed Store with encrypted
// manifests and a passphrase-protected DEK. Returns the open Store
// (already AutoUnlocked, ready for Put/Get) and its root path.
//
// The smoke variant uses Paranoid; pass Sealed to exercise
// the partial-encryption path. Both modes need WithPassphrase +
// WithAutoUnlock so the smoke loop never has to prompt.
func newEncryptedDiskStore(t *testing.T, crypto config.ManifestCrypto) (store.Store, string) {
	t.Helper()
	drv := driverfx.LocalFS(t)
	root := drv.Root()
	// Paranoid forbids an index that persists readable on disk (ADR-56), so
	// the only admissible one is ephemeral. That is not a smoke-test
	// concession: it is the single combination the mode actually runs in,
	// which makes this the right thing to be measuring (ADR-119).
	idx := indexfx.Memory(t)

	// storefx.InitOn does not expose a "with passphrase + crypto"
	// variant, and we don't need it elsewhere — wire InitStore
	// and OpenStore directly so the smoke factory stays in this
	// file.
	cfg := config.StoreConfig{ManifestCrypto: crypto}
	provider := func(_ context.Context, _ domain.PassphraseHint) ([]byte, error) {
		return []byte("smoke-pw"), nil
	}
	if _, _, err := store.InitStore(context.Background(), drv,
		store.WithConfig(cfg),
		store.WithPassphrase(provider),
		store.WithStoreIndex(idx),
		store.WithHashRegistry(storefx.Hashes()),
	); err != nil {
		t.Fatalf("InitStore: %v", err)
	}
	s, err := store.OpenStore(context.Background(), drv,
		store.WithConfig(cfg),
		store.WithPassphrase(provider),
		store.WithAutoUnlock(),
		store.WithStoreIndex(idx),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return s, root
}

// TestSmoke_EncryptedRoundTrip is the M2.3 exit-criterion smoke:
// encrypted manifests must round-trip stably with bounded memory.
// Smaller N than the Plain smoke (10k vs 100k) — encrypted Put
// adds AES-GCM-on-manifest overhead, which is microseconds per
// artifact but cumulatively meaningful at 1M scale; 10k is
// enough to demonstrate stability and catch per-artifact key-
// material accumulation.
//
// The index here is in-memory, which is what Paranoid requires: an index
// readable on disk would hold in the clear the very fields the mode
// encrypts (ADR-56/119). The memory cost of that choice is the mode's own
// (≈1.7 KB per artifact), so this smoke also stands as its measurement.
//
// Uses Paranoid mode rather than Sealed. Paranoid is the
// more demanding of the two (entire body encrypted, IV-driven
// non-determinism on Put), so passing here implies Sealed
// also works. A separate Sealed-specific smoke would only
// be warranted if a future divergence in the two paths emerges.
//
// Gated by SCRINIUM_SMOKE_ENCRYPTED=1 to keep smoke runs of the
// Plain path independent. The dedicated `make smoke-encrypted`
// target enforces this gate.
func TestSmoke_EncryptedRoundTrip(t *testing.T) {
	if os.Getenv("SCRINIUM_SMOKE_ENCRYPTED") != "1" {
		t.Skip("encrypted smoke: gated by SCRINIUM_SMOKE_ENCRYPTED=1; run via `make smoke-encrypted`")
	}

	const (
		defaultN         = 10_000
		payloadSize      = 32
		heapDeltaCeiling = 128 << 20
		reportEvery      = 1_000
	)

	n := defaultN
	if v, ok := os.LookupEnv("SCRINIUM_SMOKE_N"); ok && v != "" {
		var parsed int
		if _, err := fmt.Sscanf(v, "%d", &parsed); err == nil && parsed > 0 {
			n = parsed
		}
	}
	emit("config: N=%d, crypto=Paranoid, payload=%dB, heap-ceiling=%s",
		n, payloadSize, humanize_Bytes(heapDeltaCeiling))

	// Disk-backed Store, encrypted with Paranoid. Same factory
	// as the Plain smoke would have used, with the additional
	// crypto knobs.
	s, _ := newEncryptedDiskStore(t, config.ManifestCryptoParanoid)
	ctx := context.Background()

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	emit("baseline HeapAlloc: %s", humanize_Bytes(int64(baseline.HeapAlloc)))

	// --- Put loop ---
	//
	// Track three sampled positions (first, mid, last) by saving
	// (idx, id) pairs rather than just ids. The idx lets us
	// regenerate the deterministic payload via makePayload(idx, size)
	// at Get time and byte-compare; this catches a class of bugs
	// (decryption silently producing zero-length or junk output)
	// that a length-only check would miss.
	type sample struct {
		idx int
		id  domain.ArtifactID
	}
	samples := make([]sample, 0, 3)

	emit("Put: starting %d artifacts", n)
	startPut := time.Now()
	for i := 0; i < n; i++ {
		p := makePayload(i, payloadSize)
		id, err := s.Put(ctx,
			domain.Artifact{Payload: bytes.NewReader(p)})
		if err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
		if i == 0 || i == n/2 || i == n-1 {
			samples = append(samples, sample{idx: i, id: id})
		}
		if (i+1)%reportEvery == 0 {
			reportProgress("Put", i+1, n, startPut)
		}
	}
	emit("Put: done in %s", time.Since(startPut).Round(time.Millisecond))

	// --- Walk count ---
	var walkCount int64
	if err := s.Walk(ctx, func(domain.Manifest) error {
		walkCount++
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if walkCount != int64(n) {
		t.Errorf("Walk count: got %d, want %d", walkCount, n)
	}
	emit("Walk: %d rows confirmed", walkCount)

	// --- Sample Get round-trip with byte verification ---
	//
	// makePayload(idx, size) is deterministic: calling it with the
	// same idx at Get time reproduces the exact bytes that were
	// originally Put. Compare full payloads — not just lengths —
	// so a decrypt that silently produces zero-filled output (or
	// truncates, or shifts bytes) is caught here rather than
	// surfacing far downstream.
	for _, sm := range samples {
		rh, err := s.Get(ctx, sm.id)
		if err != nil {
			t.Fatalf("Get sample idx=%d (id=%q): %v", sm.idx, sm.id, err)
		}
		got, err := io.ReadAll(rh)
		_ = rh.Close()
		if err != nil {
			t.Fatalf("ReadAll sample idx=%d: %v", sm.idx, err)
		}
		want := makePayload(sm.idx, payloadSize)
		if !bytes.Equal(got, want) {
			t.Errorf("sample idx=%d: payload mismatch (got %d bytes, want %d)",
				sm.idx, len(got), len(want))
		}
	}
	emit("Get: all %d samples matched (bytes verified)", len(samples))

	// --- Heap ceiling ---
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := int64(after.HeapAlloc) - int64(baseline.HeapAlloc)
	emit("HeapAlloc delta: %s", humanize_Bytes(delta))
	if delta > heapDeltaCeiling {
		t.Errorf("HeapAlloc delta %s exceeds ceiling %s",
			humanize_Bytes(delta), humanize_Bytes(heapDeltaCeiling))
	}
}

func humanize_Bytes(n int64) string {
	if n < 0 {
		n = -n
	}
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
		TiB = 1024 * GiB
	)
	switch {
	case n >= TiB:
		return fmt.Sprintf("%.1f TiB", float64(n)/float64(TiB))
	case n >= GiB:
		return fmt.Sprintf("%.1f GiB", float64(n)/float64(GiB))
	case n >= MiB:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(MiB))
	case n >= KiB:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(KiB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
