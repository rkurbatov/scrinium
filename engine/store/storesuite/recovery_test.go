// Recovery: what the bootstrap orphan scan does with files left by a prior
// (crashed) process, the orphanscan transient-error contracts, and recovery
// of a lost descriptor from its Recovery Kit. Three entry points share this
// file because they are one concern (durability/recovery): Store bootstrap
// (InitStore/OpenStore run the scan and publish a report), the orphanscan
// package directly (its index-error branches), and RestoreDescriptorFromRecoveryKit.

package storesuite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/driver/faulty"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/named"
	"scrinium.dev/engine/layout"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/engine/store/internal/orphanscan"
	"scrinium.dev/errs"
	"scrinium.dev/event"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/eventfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
	"scrinium.dev/testutil/storekit"
)

// --- shared test rig ----------------------------------------------

// recoveryFixture exposes the Driver, Index and event Recorder separately so
// individual tests can stage on-disk preconditions BEFORE calling InitStore
// or OpenStore — the whole point is exercising what the bootstrap scan does
// with files placed by a prior (crashed) process. Events are captured with
// the shared eventfx.Recorder.
type recoveryFixture struct {
	drv driver.Driver
	idx index.StoreIndex
	rec *eventfx.Recorder
	// mustSurvive are paths the open-time pass is forbidden to delete:
	// files unknown to the index but not provably garbage (ADR-118).
	mustSurvive []string
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	return &recoveryFixture{
		drv: driverfx.LocalFS(t),
		idx: indexfx.Memory(t),
		rec: eventfx.New(),
	}
}

// initStore runs store.InitStore against the fixture. EventOrphanScanCompleted
// has been recorded by the time this returns.
func (f *recoveryFixture) initStore(t *testing.T) store.Store {
	t.Helper()
	s, _, err := store.InitStore(context.Background(), f.drv,
		store.WithStoreIndex(f.idx),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPublisher(f.rec),
	)
	if err != nil {
		t.Fatalf("InitStore: %v", err)
	}
	return s
}

// openStore runs store.OpenStore on the same Driver+Index — for
// "crash-then-reopen" scenarios.
func (f *recoveryFixture) openStore(t *testing.T) store.Store {
	t.Helper()
	s, err := store.OpenStore(context.Background(), f.drv,
		store.WithStoreIndex(f.idx),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPublisher(f.rec),
	)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return s
}

// stageFile plants a file with the given content at a root-relative path
// through the Driver — a synthetic orphan blob / manifest / staging file.
func (f *recoveryFixture) stageFile(t *testing.T, path, content string) {
	t.Helper()
	if err := f.drv.Put(context.Background(), path, strings.NewReader(content)); err != nil {
		t.Fatalf("Driver.Put(%q): %v", path, err)
	}
}

// fileExists probes whether the Driver still sees a path.
func (f *recoveryFixture) fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := f.drv.Stat(context.Background(), path)
	return err == nil
}

// lastReport returns the most recent EventOrphanScanCompleted payload.
func lastReport(t *testing.T, rec *eventfx.Recorder) event.OrphanScanCompletedPayload {
	t.Helper()
	evs := rec.ByType(event.EventOrphanScanCompleted)
	if len(evs) == 0 {
		t.Fatalf("EventOrphanScanCompleted: no events recorded")
	}
	p, ok := evs[len(evs)-1].Payload.(event.OrphanScanCompletedPayload)
	if !ok {
		t.Fatalf("EventOrphanScanCompleted: payload is %T, want OrphanScanCompletedPayload", evs[len(evs)-1].Payload)
	}
	return p
}

func reportCount(rec *eventfx.Recorder) int {
	return rec.Count(event.EventOrphanScanCompleted)
}

// manifestPathForID is the on-disk manifest path for a digest (manifest files
// are named by their digest).
func manifestPathForID(t *testing.T, digest domain.ManifestDigest) string {
	t.Helper()
	p, err := layout.ManifestPath(digest)
	if err != nil {
		t.Fatalf("artifact.ManifestPath(%q): %v", digest, err)
	}
	return p
}

// fakeRef returns a blob-ref-shaped string with a recognisable hex tail. The
// suffix makes each ref distinct; total length is comfortably above the
// 4-char shard minimum.
func fakeRef(suffix byte) string {
	return strings.Repeat("ab", 16) + fmt.Sprintf("%02x", suffix)
}

// --- bootstrap orphan sweep --------------------------------------

// TestRecovery_SweepsOrphansAtInit: InitStore runs the orphan scan, removes
// every recognisable orphan (blob / manifest / staging) and publishes exactly
// one EventOrphanScanCompleted whose counters match what was swept. A fresh
// store removes nothing; the all-three case also carries a positive Duration.
func TestRecovery_SweepsOrphansAtInit(t *testing.T) {
	type want struct{ blobs, manifests, staging int }
	cases := []struct {
		name          string
		stage         func(t *testing.T, f *recoveryFixture) []string // staged paths that must be gone after
		want          want
		checkDuration bool
	}{
		{"fresh store, nothing staged", func(t *testing.T, f *recoveryFixture) []string { return nil }, want{0, 0, 0}, false},
		// A blob the index does not know is NOT touched at open (ADR-118):
		// "the index has no row" means the index does not know, not that the
		// bytes are garbage. Reclaiming blobs is the GC agent's business.
		{"unknown blob survives", func(t *testing.T, f *recoveryFixture) []string {
			p := storekit.BlobPathForRef(t, fakeRef('a'))
			f.stageFile(t, p, "orphan blob content")
			f.mustSurvive = append(f.mustSurvive, p)
			return nil
		}, want{0, 0, 0}, false},
		// A manifest file whose bytes do not hash to its own name is a
		// fragment of an interrupted write — the one thing the pass deletes.
		{"fragment manifest", func(t *testing.T, f *recoveryFixture) []string {
			p := manifestPathForID(t, domain.ManifestDigest(fakeRef('m')))
			f.stageFile(t, p, "{}")
			return []string{p}
		}, want{0, 1, 0}, false},
		{"staging leftover", func(t *testing.T, f *recoveryFixture) []string {
			p := ".staging/leftover-deadbeef"
			f.stageFile(t, p, "stale staging from a crashed prior write")
			return []string{p}
		}, want{0, 0, 1}, false},
		{"one of each", func(t *testing.T, f *recoveryFixture) []string {
			b := storekit.BlobPathForRef(t, fakeRef('1'))
			f.stageFile(t, b, "x")
			f.mustSurvive = append(f.mustSurvive, b)
			m := manifestPathForID(t, domain.ManifestDigest(fakeRef('2')))
			f.stageFile(t, m, "{}")
			st := ".staging/leftover-3"
			f.stageFile(t, st, "x")
			return []string{m, st}
		}, want{0, 1, 1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveryFixture(t)
			staged := tc.stage(t, f)

			_ = f.initStore(t)

			if got := reportCount(f.rec); got != 1 {
				t.Fatalf("EventOrphanScanCompleted: got %d events, want 1", got)
			}
			r := lastReport(t, f.rec)
			if r.BlobsRemoved != tc.want.blobs || r.ManifestsRemoved != tc.want.manifests || r.StagingRemoved != tc.want.staging {
				t.Errorf("removed counts: got {b:%d m:%d s:%d}, want {b:%d m:%d s:%d}; report=%+v",
					r.BlobsRemoved, r.ManifestsRemoved, r.StagingRemoved,
					tc.want.blobs, tc.want.manifests, tc.want.staging, r)
			}
			if r.NonFatalErrors != 0 {
				t.Errorf("NonFatalErrors: got %d, want 0", r.NonFatalErrors)
			}
			for _, p := range staged {
				if f.fileExists(t, p) {
					t.Errorf("%q must be removed by the open-time pass", p)
				}
			}
			for _, p := range f.mustSurvive {
				if !f.fileExists(t, p) {
					t.Errorf("%q must survive: the pass never deletes on the index's word alone", p)
				}
			}
			if tc.checkDuration && r.Duration <= 0 {
				t.Errorf("payload.Duration: got %v, want > 0", r.Duration)
			}
		})
	}
}

// TestRecovery_LeavesUnparseableName: a file under manifests/ whose name is
// not "<algo>-<hex>" is of unknown nature — not ours to judge, so not ours to
// delete. It stays and a non-fatal error is recorded. (A junk file under
// blobs/ is not even looked at: the pass does not walk blobs.)
func TestRecovery_LeavesUnparseableName(t *testing.T) {
	f := newRecoveryFixture(t)

	junkManifest := "manifests/aa/bb/not-a-digest-just-some-text"
	f.stageFile(t, junkManifest, "mystery file")
	junkBlob := "blobs/aa/bb/not-a-blob-ref-just-some-text"
	f.stageFile(t, junkBlob, "another mystery")

	_ = f.initStore(t)

	if !f.fileExists(t, junkManifest) {
		t.Errorf("unparseable name %q must be left alone", junkManifest)
	}
	if !f.fileExists(t, junkBlob) {
		t.Errorf("file under blobs/ %q must be left alone — the pass does not walk blobs", junkBlob)
	}
	r := lastReport(t, f.rec)
	if r.ManifestsRemoved != 0 || r.BlobsRemoved != 0 {
		t.Errorf("removed counts: got %+v, want zeroes", r)
	}
	if r.NonFatalErrors == 0 {
		t.Errorf("NonFatalErrors: got 0, want >=1 (the unparseable name was expected to be reported)")
	}
}

// TestRecovery_DoesNotTouchLiveArtifact: a real index-backed artifact survives
// a subsequent recovery pass — blob and manifest are left in place, Walk still
// lists it, Get still reads it.
func TestRecovery_DoesNotTouchLiveArtifact(t *testing.T) {
	f := newRecoveryFixture(t)
	s := f.initStore(t)

	id, err := s.Put(context.Background(),
		domain.Artifact{Payload: bytes.NewReader([]byte("real payload"))})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Observe only the second pass.
	f.rec.Clear()
	s2 := f.openStore(t)

	r := lastReport(t, f.rec)
	if r.BlobsRemoved != 0 || r.ManifestsRemoved != 0 || r.StagingRemoved != 0 {
		t.Errorf("live artifact run: removed counts must be 0, got %+v", r)
	}

	var seen []domain.ArtifactID
	if err := s2.Walk(context.Background(), func(m domain.Manifest) error {
		seen = append(seen, m.ArtifactID)
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(seen) != 1 || seen[0] != id {
		t.Errorf("Walk after recovery: got %v, want [%v]", seen, id)
	}

	rh, err := s2.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	defer rh.Close()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "real payload" {
		t.Errorf("payload mismatch: got %q, want %q", got, "real payload")
	}
}

// TestRecovery_OpenStore_HandlesDebrisInjectedAfterInit: debris planted
// between sessions is dealt with on the next OpenStore, each kind on its own
// terms (ADR-118) — the fragment goes, the blob stays, the live artifact is
// untouched.
func TestRecovery_OpenStore_HandlesDebrisInjectedAfterInit(t *testing.T) {
	f := newRecoveryFixture(t)
	s := f.initStore(t)

	liveID, err := s.Put(context.Background(),
		domain.Artifact{Payload: bytes.NewReader([]byte("survivor"))})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	liveDigest := storekit.MustDigest(t, s, liveID)

	orphanBlob := storekit.BlobPathForRef(t, fakeRef('z'))
	f.stageFile(t, orphanBlob, "abandoned blob")
	orphanManifest := manifestPathForID(t, domain.ManifestDigest(fakeRef('y')))
	f.stageFile(t, orphanManifest, "{}")

	f.rec.Clear()
	s2 := f.openStore(t)

	if !f.fileExists(t, orphanBlob) {
		t.Errorf("blob %q must survive: an index without a row for it has said nothing about the bytes", orphanBlob)
	}
	if f.fileExists(t, orphanManifest) {
		t.Errorf("fragment %q must be removed: its bytes do not hash to its name", orphanManifest)
	}
	livePath := manifestPathForID(t, liveDigest)
	if !f.fileExists(t, livePath) {
		t.Errorf("live manifest %q must NOT be removed", livePath)
	}

	r := lastReport(t, f.rec)
	if r.BlobsRemoved != 0 {
		t.Errorf("BlobsRemoved: got %d, want 0 — the open-time pass never deletes blobs", r.BlobsRemoved)
	}
	if r.ManifestsRemoved != 1 {
		t.Errorf("ManifestsRemoved: got %d, want 1", r.ManifestsRemoved)
	}

	rh, err := s2.Get(context.Background(), liveID)
	if err != nil {
		t.Fatalf("Get(live): %v", err)
	}
	got, _ := io.ReadAll(rh)
	rh.Close()
	if string(got) != "survivor" {
		t.Errorf("live payload mismatch: got %q, want %q", got, "survivor")
	}
}

// TestRecovery_NoPublisher_NoPanic: with no publisher wired, the pass still
// runs — it just stays silent. Guards against a nil-publisher
// dereference in the report path.
func TestRecovery_NoPublisher_NoPanic(t *testing.T) {
	d := driverfx.LocalFS(t)

	fragment := manifestPathForID(t, domain.ManifestDigest(fakeRef('q')))
	if err := d.Put(context.Background(), fragment, strings.NewReader("half a manifest")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, err := store.InitStore(context.Background(), d,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if err != nil {
		t.Fatalf("InitStore (no publisher): %v", err)
	}

	if _, err := d.Stat(context.Background(), fragment); err == nil {
		t.Errorf("fragment %q must be removed even without a publisher", fragment)
	}
}

// --- orphanscan package: transient index-error contracts ---------

// faultyIndex wraps a real StoreIndex and injects errors into the two methods
// recoverOrphans consults: Resolve (per-blob) and ManifestExistsByDigest
// (per-manifest). All other calls pass through. Used to exercise the
// "transient index error" branch: an index-infrastructure failure during a
// sweep must NOT remove the orphan (better to leave a possibly-orphan file
// than to mistake healthy data for orphan because of a transient SQLite
// hiccup). It wraps the index interface, so it has no driverfx equivalent.
type faultyIndex struct {
	index.StoreIndex
	resolveErr        error // if non-nil, Resolve returns this
	manifestExistsErr error // if non-nil, ManifestExistsByDigest returns this
}

func (f *faultyIndex) Resolve(ctx context.Context, ref string) (domain.PhysicalAddress, error) {
	if f.resolveErr != nil {
		return domain.PhysicalAddress{}, f.resolveErr
	}
	return f.StoreIndex.Resolve(ctx, ref)
}

func (f *faultyIndex) ManifestExistsByDigest(ctx context.Context, digest domain.ManifestDigest) (bool, error) {
	if f.manifestExistsErr != nil {
		return false, f.manifestExistsErr
	}
	return f.StoreIndex.ManifestExistsByDigest(ctx, digest)
}

// The pass in isolation. Its whole contract is what it REFUSES to do, so the
// cases below are mostly about files that must still be there afterwards.

// stubIngester stands in for the Store's manifest ingestion. Whatever it
// returns decides the pass's reaction to a file the index does not know.
type stubIngester struct {
	err   error
	calls int
}

func (s *stubIngester) IngestManifest(context.Context, domain.ManifestDigest) error {
	s.calls++
	return s.err
}

// TestReconcile_UnknownBlobSurvives: the index not resolving a blob ref means
// the index does not know it — never that the bytes are garbage. Blobs are
// reclaimed by GC, on the "no manifest references it" criterion, two-phase
// and with a delay. The open-time pass must not touch them.
func TestReconcile_UnknownBlobSurvives(t *testing.T) {
	f := newRecoveryFixture(t)
	blob := storekit.BlobPathForRef(t, fakeRef('a'))
	f.stageFile(t, blob, "bytes nobody has claimed yet")

	report, err := orphanscan.Reconcile(context.Background(), f.drv, f.idx, &stubIngester{})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.BlobsRemoved != 0 {
		t.Errorf("BlobsRemoved: got %d, want 0 — the pass never deletes blobs", report.BlobsRemoved)
	}
	if !f.fileExists(t, blob) {
		t.Fatal("blob deleted at open; reclaiming blobs belongs to GC")
	}
}

// TestReconcile_WholeManifestIsIngested: a manifest the index does not know
// is truth the index has forgotten — a crash before the row was written, a
// hand-merged tree, a restore. The pass reads it back in and deletes nothing.
func TestReconcile_WholeManifestIsIngested(t *testing.T) {
	f := newRecoveryFixture(t)
	path := manifestPathForID(t, domain.ManifestDigest(fakeRef('m')))
	f.stageFile(t, path, "whole manifest bytes")

	ing := &stubIngester{} // ingestion succeeds
	report, err := orphanscan.Reconcile(context.Background(), f.drv, f.idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ing.calls != 1 {
		t.Errorf("IngestManifest calls: got %d, want 1", ing.calls)
	}
	if report.ManifestsIndexed != 1 {
		t.Errorf("ManifestsIndexed: got %d, want 1", report.ManifestsIndexed)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if !f.fileExists(t, path) {
		t.Fatal("a whole manifest must never be deleted by the pass")
	}
}

// TestReconcile_FragmentIsRemoved: bytes that do not hash to their own name
// cannot be a whole manifest — only a write interrupted mid-flight. This is
// the single deletion the pass performs, and it rests on the file's own
// arithmetic rather than on the index's word.
func TestReconcile_FragmentIsRemoved(t *testing.T) {
	f := newRecoveryFixture(t)
	path := manifestPathForID(t, domain.ManifestDigest(fakeRef('f')))
	f.stageFile(t, path, "half a manifest")

	ing := &stubIngester{err: errs.ErrCorruptedManifest}
	report, err := orphanscan.Reconcile(context.Background(), f.drv, f.idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 1 {
		t.Errorf("ManifestsRemoved: got %d, want 1", report.ManifestsRemoved)
	}
	if f.fileExists(t, path) {
		t.Fatal("a fragment must be removed")
	}
}

// TestReconcile_UnreadableManifestSurvives: any failure other than a hash
// mismatch — a missing key, an I/O fault, an unknown schema — says nothing
// about whether the file is garbage. Report it and keep it.
func TestReconcile_UnreadableManifestSurvives(t *testing.T) {
	f := newRecoveryFixture(t)
	path := manifestPathForID(t, domain.ManifestDigest(fakeRef('u')))
	f.stageFile(t, path, "encrypted with a key we do not have")

	ing := &stubIngester{err: errors.New("no key for this manifest")}
	report, err := orphanscan.Reconcile(context.Background(), f.drv, f.idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if len(report.Errors) != 1 {
		t.Errorf("non-fatal errors: got %d, want 1", len(report.Errors))
	}
	if !f.fileExists(t, path) {
		t.Fatal("an unreadable manifest must survive: unreadable is not garbage")
	}
}

// TestReconcile_TransientIndexError_Preserves: if the index cannot answer
// "do you know this manifest", that is trouble with the index, not evidence
// about the file. Nothing is removed and nothing is ingested.
func TestReconcile_TransientIndexError_Preserves(t *testing.T) {
	f := newRecoveryFixture(t)
	path := manifestPathForID(t, domain.ManifestDigest(fakeRef('t')))
	f.stageFile(t, path, "{}")

	idx := &faultyIndex{StoreIndex: f.idx, manifestExistsErr: errors.New("sqlite hiccup")}
	ing := &stubIngester{}
	report, err := orphanscan.Reconcile(context.Background(), f.drv, idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ing.calls != 0 {
		t.Errorf("IngestManifest called %d times despite an index error", ing.calls)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if !f.fileExists(t, path) {
		t.Fatal("file removed on an index error")
	}
}

// TestReconcile_RemoveFails_FragmentStays: a fragment whose deletion fails
// stays on disk, the error is recorded, and the pass continues.
func TestReconcile_RemoveFails_FragmentStays(t *testing.T) {
	f := newRecoveryFixture(t)
	path := manifestPathForID(t, domain.ManifestDigest(fakeRef('r')))
	f.stageFile(t, path, "half a manifest")

	drv := driverfx.Faulty(t, f.drv, faulty.WithFailureRate(faulty.MethodRemove, 1.0))
	ing := &stubIngester{err: errs.ErrCorruptedManifest}
	report, err := orphanscan.Reconcile(context.Background(), drv, f.idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if len(report.Errors) == 0 {
		t.Error("a failed Remove must be recorded")
	}
	if !f.fileExists(t, path) {
		t.Fatal("the file should still be there after a failed Remove")
	}
}

func TestRestoreDescriptorFromRecoveryKit_RoundTrip(t *testing.T) {
	ctx := context.Background()
	drv := driverfx.LocalFS(t)

	_, kit, err := store.InitStore(ctx, drv,
		store.WithHashRegistry(storefx.Hashes()),
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithPassphrase(storefx.StaticPP("pw")),
	)
	if err != nil {
		t.Fatalf("InitStore (encrypted): %v", err)
	}
	if len(kit) == 0 {
		t.Fatal("InitStore returned an empty recovery kit for an encrypted store")
	}

	orig, err := descriptor.Read(ctx, drv, storefx.Hashes())
	if err != nil {
		t.Fatalf("read original descriptor: %v", err)
	}

	// Simulate catastrophic descriptor loss: remove both replicas.
	if err := descriptor.RemoveBoth(ctx, drv); err != nil {
		t.Fatalf("remove descriptor replicas: %v", err)
	}
	if _, err := descriptor.Read(ctx, drv, storefx.Hashes()); err == nil {
		t.Fatal("descriptor still readable after removing both replicas")
	}

	info, err := store.RestoreDescriptorFromRecoveryKit(ctx, drv, kit)
	if err != nil {
		t.Fatalf("RestoreDescriptorFromRecoveryKit: %v", err)
	}
	if !info.DescriptorWritten {
		t.Error("DescriptorWritten = false, want true")
	}
	if info.StoreID != orig.StoreID {
		t.Errorf("info.StoreID = %q, want %q", info.StoreID, orig.StoreID)
	}

	restored, err := descriptor.Read(ctx, drv, storefx.Hashes())
	if err != nil {
		t.Fatalf("read restored descriptor (L0): %v", err)
	}
	if restored.StoreID != orig.StoreID {
		t.Errorf("StoreID = %q, want %q", restored.StoreID, orig.StoreID)
	}
	if !restored.DEKEncrypted {
		t.Error("restored descriptor not marked DEKEncrypted")
	}
	if !bytes.Equal(restored.DEK, orig.DEK) {
		t.Error("restored wrapped DEK differs from the original")
	}
	if restored.KDFParams == nil || orig.KDFParams == nil {
		t.Fatal("KDFParams missing on original or restored descriptor")
	}
	if restored.KDFParams.Algorithm != orig.KDFParams.Algorithm {
		t.Errorf("KDF algorithm = %q, want %q", restored.KDFParams.Algorithm, orig.KDFParams.Algorithm)
	}
	if !bytes.Equal(restored.KDFParams.Salt, orig.KDFParams.Salt) {
		t.Error("restored KDF salt differs from the original")
	}
	if restored.KDFParams.Time != orig.KDFParams.Time ||
		restored.KDFParams.Memory != orig.KDFParams.Memory ||
		restored.KDFParams.Threads != orig.KDFParams.Threads {
		t.Error("restored KDF cost parameters differ from the original")
	}

	if _, err := named.LoadCell(ctx, drv, storefx.Hashes(), descriptor.BackupName); err != nil {
		t.Fatalf("L1 shadow descriptor not restored: %v", err)
	}
}

// TestRestoreDescriptorFromRecoveryKit_Rejects: corrupted kit bytes yield the
// corrupted-kit sentinel; a nil driver is a rejected programming error.
func TestRestoreDescriptorFromRecoveryKit_Rejects(t *testing.T) {
	cases := []struct {
		name    string
		drv     driver.Driver
		kit     []byte
		wantErr error // nil = any non-nil error is acceptable
	}{
		{"corrupted kit bytes", driverfx.LocalFS(t), []byte("not a recovery kit"), errs.ErrRecoveryKitCorrupted},
		{"nil driver", nil, []byte("x"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.RestoreDescriptorFromRecoveryKit(context.Background(), tc.drv, tc.kit)
			if tc.wantErr == nil {
				if err == nil {
					t.Fatal("got nil error, want non-nil")
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
