package orphanscan

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// White-box tests for the open-time reconciliation (ADR-118). The pass is
// defined as much by what it REFUSES to delete as by what it removes, so
// most cases below assert survival: a blob the index cannot resolve, a
// manifest the index has forgotten, a file that could not be read for any
// reason other than failing its own hash.
//
// The collaborators are hand-built fakes rather than a localfs driver and a
// real index: every branch turns on what the index and the ingester ANSWER,
// and a fake answers on demand without standing up a database.

// --- fixtures ------------------------------------------------------------

// fakeDriver serves a seeded set of object paths per prefix and records
// every Remove. The embedded driver.Driver is left nil: RecoverOrphans
// touches only ListObjectsWithModTime and Remove, so any other call would
// panic loudly rather than pass silently on a stub.
type fakeDriver struct {
	driver.Driver
	objects   map[string][]string // prefix -> object paths under it
	listErr   map[string]error    // prefix -> error returned by List
	removeErr map[string]error    // path   -> error returned by Remove
	removed   []string
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{
		objects:   map[string][]string{},
		listErr:   map[string]error{},
		removeErr: map[string]error{},
	}
}

func (d *fakeDriver) ListObjectsWithModTime(ctx context.Context, prefix string, _ time.Time, cb func(driver.ObjectMeta) error) error {
	if err := d.listErr[prefix]; err != nil {
		return err
	}
	for _, p := range d.objects[prefix] {
		if err := cb(driver.ObjectMeta{Path: p}); err != nil {
			return err
		}
	}
	return nil
}

func (d *fakeDriver) Remove(_ context.Context, path string) error {
	if err := d.removeErr[path]; err != nil {
		return err
	}
	d.removed = append(d.removed, path)
	return nil
}

func (d *fakeDriver) wasRemoved(path string) bool {
	for _, r := range d.removed {
		if r == path {
			return true
		}
	}
	return false
}

// fakeIndex answers the single question the pass asks: does the index know
// this manifest. The embedded interface is nil for the same loud-failure
// reason as fakeDriver.
type fakeIndex struct {
	index.StoreIndex
	manifestExists func(digest domain.ManifestDigest) (bool, error)
}

func (i fakeIndex) ManifestExistsByDigest(_ context.Context, digest domain.ManifestDigest) (bool, error) {
	return i.manifestExists(digest)
}

func manifestPresent(domain.ManifestDigest) (bool, error) { return true, nil }
func manifestMissing(domain.ManifestDigest) (bool, error) { return false, nil }
func manifestBoom(domain.ManifestDigest) (bool, error)    { return false, errors.New("index unavailable") }

// fakeIngester decides the fate of a manifest the index does not know: nil
// means the file was whole and went into the index, ErrCorruptedManifest
// means the bytes did not hash to the name, anything else means unreadable.
type fakeIngester struct {
	err  error
	seen []domain.ManifestDigest
}

func (f *fakeIngester) IngestManifest(_ context.Context, digest domain.ManifestDigest) error {
	f.seen = append(f.seen, digest)
	return f.err
}

func ingestWhole() *fakeIngester    { return &fakeIngester{} }
func ingestFragment() *fakeIngester { return &fakeIngester{err: errs.ErrCorruptedManifest} }
func ingestUnreadable() *fakeIngester {
	return &fakeIngester{err: errors.New("no key for this manifest")}
}

// Valid sharded paths. A ref/digest is the last path segment and must be
// ≥4 lowercase-hex chars (artifact.validateRefShape).
const (
	stagingFile = domain.StagingPrefix + "/tmp-write-1234"
	blobOrphan  = "blobs/de/ad/deadbeef"
	blobKnown   = "blobs/ab/cd/abcdef01"
	maniOrphan  = "manifests/de/ad/deadbeef"
	maniKnown   = "manifests/ab/cd/abcdef01"
	maniBadPath = "manifests/ab/cd/bad"
)

// --- staging sweep -------------------------------------------------------

func TestReconcile_StagingAlwaysRemoved(t *testing.T) {
	drv := newFakeDriver()
	drv.objects[domain.StagingPrefix] = []string{stagingFile, domain.StagingPrefix + "/tmp-write-5678"}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.StagingRemoved != 2 {
		t.Errorf("StagingRemoved: got %d, want 2", report.StagingRemoved)
	}
	if !drv.wasRemoved(stagingFile) {
		t.Error("staging file should be removed unconditionally")
	}
	if len(report.Errors) != 0 {
		t.Errorf("Errors: got %v, want none", report.Errors)
	}
}

func TestReconcile_StagingRemoveError_CollectedAndContinues(t *testing.T) {
	first := domain.StagingPrefix + "/a"
	second := domain.StagingPrefix + "/b"
	drv := newFakeDriver()
	drv.objects[domain.StagingPrefix] = []string{first, second}
	drv.removeErr[first] = errors.New("disk gone")
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.StagingRemoved != 1 {
		t.Errorf("StagingRemoved: got %d, want 1", report.StagingRemoved)
	}
	if len(report.Errors) != 1 {
		t.Fatalf("Errors: got %d, want 1", len(report.Errors))
	}
	if !drv.wasRemoved(second) {
		t.Error("second staging file should still be removed after the first errored")
	}
}

// --- blobs are not the pass's business -----------------------------------

// The index failing to resolve a ref says the index does not know it, never
// that the bytes are garbage. Reclaiming blobs is the GC agent's job, on the
// "no manifest references it" criterion, two-phase and delayed.
func TestReconcile_BlobsAreNeverTouched(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["blobs"] = []string{blobOrphan, blobKnown}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.BlobsRemoved != 0 {
		t.Errorf("BlobsRemoved: got %d, want 0", report.BlobsRemoved)
	}
	if drv.wasRemoved(blobOrphan) || drv.wasRemoved(blobKnown) {
		t.Fatalf("blobs removed at open: %v", drv.removed)
	}
}

// --- manifests: reconciliation -------------------------------------------

func TestReconcile_KnownManifestUntouched(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniKnown}
	idx := fakeIndex{manifestExists: manifestPresent}
	ing := ingestWhole()

	report, err := Reconcile(context.Background(), drv, idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(ing.seen) != 0 {
		t.Errorf("a manifest the index knows must not be read: %v", ing.seen)
	}
	if report.ManifestsIndexed != 0 || report.ManifestsRemoved != 0 {
		t.Errorf("counts: %+v", report)
	}
	if len(drv.removed) != 0 {
		t.Errorf("nothing should be removed: %v", drv.removed)
	}
}

// A whole manifest the index has forgotten is truth: it goes INTO the index.
// This is the crash-before-the-row case, and also a hand-merged tree.
func TestReconcile_UnknownWholeManifestIsIngested(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniOrphan}
	idx := fakeIndex{manifestExists: manifestMissing}
	ing := ingestWhole()

	report, err := Reconcile(context.Background(), drv, idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(ing.seen) != 1 {
		t.Fatalf("ingester calls: got %d, want 1", len(ing.seen))
	}
	if report.ManifestsIndexed != 1 {
		t.Errorf("ManifestsIndexed: got %d, want 1", report.ManifestsIndexed)
	}
	if drv.wasRemoved(maniOrphan) {
		t.Fatal("a whole manifest must never be removed")
	}
}

// Bytes that do not hash to their own name cannot be a whole manifest — only
// a write cut short. This is the single deletion the pass performs.
func TestReconcile_FragmentIsRemoved(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniOrphan}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestFragment())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 1 {
		t.Errorf("ManifestsRemoved: got %d, want 1", report.ManifestsRemoved)
	}
	if !drv.wasRemoved(maniOrphan) {
		t.Error("fragment should be removed")
	}
}

// Unreadable is not garbage: a missing key, an I/O fault or an unknown
// schema say nothing about whether the file is whole.
func TestReconcile_UnreadableManifestSurvives(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniOrphan}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestUnreadable())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if len(report.Errors) != 1 {
		t.Errorf("Errors: got %d, want 1", len(report.Errors))
	}
	if drv.wasRemoved(maniOrphan) {
		t.Fatal("an unreadable manifest must survive")
	}
}

// An index that cannot answer is index trouble, not evidence about the file.
func TestReconcile_ManifestExistsError_LeavesOnDisk(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniOrphan}
	idx := fakeIndex{manifestExists: manifestBoom}
	ing := ingestWhole()

	report, err := Reconcile(context.Background(), drv, idx, ing)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(ing.seen) != 0 {
		t.Errorf("nothing should be read when the index errors: %v", ing.seen)
	}
	if len(report.Errors) != 1 {
		t.Errorf("Errors: got %d, want 1", len(report.Errors))
	}
	if drv.wasRemoved(maniOrphan) {
		t.Fatal("file removed on an index error")
	}
}

// A name we cannot parse is a file of unknown nature: reported, not judged.
func TestReconcile_UnparseableManifestPath_SkippedWithError(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniBadPath}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.Errors) != 1 {
		t.Errorf("Errors: got %d, want 1", len(report.Errors))
	}
	if drv.wasRemoved(maniBadPath) {
		t.Fatal("a file with an unparseable name must not be removed")
	}
}

// A fragment whose Remove fails stays put; the pass records and continues.
func TestReconcile_FragmentRemoveFails_StaysAndIsReported(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniOrphan}
	drv.removeErr[maniOrphan] = errors.New("permission denied")
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestFragment())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsRemoved != 0 {
		t.Errorf("ManifestsRemoved: got %d, want 0", report.ManifestsRemoved)
	}
	if len(report.Errors) != 1 {
		t.Errorf("Errors: got %d, want 1", len(report.Errors))
	}
}

// An empty index is no longer a hazard: it means everything is unknown, and
// unknown-but-whole manifests are read in rather than deleted.
func TestReconcile_EmptyIndex_IngestsInsteadOfDeleting(t *testing.T) {
	drv := newFakeDriver()
	drv.objects["manifests"] = []string{maniKnown, maniOrphan}
	drv.objects["blobs"] = []string{blobKnown, blobOrphan}
	idx := fakeIndex{manifestExists: manifestMissing}

	report, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ManifestsIndexed != 2 {
		t.Errorf("ManifestsIndexed: got %d, want 2", report.ManifestsIndexed)
	}
	if len(drv.removed) != 0 {
		t.Fatalf("an empty index must delete nothing, removed: %v", drv.removed)
	}
}

// --- aborts --------------------------------------------------------------

func TestReconcile_ListError_AbortsWithError(t *testing.T) {
	boom := errors.New("list failed")
	drv := newFakeDriver()
	drv.listErr["manifests"] = boom
	drv.objects["manifests"] = []string{maniOrphan} // never reached
	idx := fakeIndex{manifestExists: manifestMissing}

	_, err := Reconcile(context.Background(), drv, idx, ingestWhole())
	if !errors.Is(err, boom) {
		t.Fatalf("expected the list error to propagate, got %v", err)
	}
	if drv.wasRemoved(maniOrphan) {
		t.Error("nothing should be removed once a List aborts the pass")
	}
}

func TestReconcile_ContextCancelled_Aborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drv := newFakeDriver()
	drv.objects[domain.StagingPrefix] = []string{stagingFile}
	idx := fakeIndex{manifestExists: manifestMissing}

	_, err := Reconcile(ctx, drv, idx, ingestWhole())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if drv.wasRemoved(stagingFile) {
		t.Error("a cancelled context must stop the pass before any Remove")
	}
}

// --- report publishing ---------------------------------------------------

// capturePublisher records published events for assertion.
type capturePublisher struct{ events []event.Event }

func (p *capturePublisher) Publish(e event.Event) { p.events = append(p.events, e) }

func TestPublishOrphanReport_NilPublisher_NoOp(t *testing.T) {
	// Must not panic on a nil Publisher (the minimal-stack default).
	PublishOrphanReport(nil, OrphanReport{StagingRemoved: 1})
}

func TestPublishOrphanReport_EmitsCounts(t *testing.T) {
	pub := &capturePublisher{}
	report := OrphanReport{
		StagingRemoved:   1,
		BlobsRemoved:     2,
		ManifestsRemoved: 3,
		ManifestsIndexed: 4,
		Errors:           []error{errors.New("x"), errors.New("y")},
		Duration:         5 * time.Millisecond,
	}

	PublishOrphanReport(pub, report)

	if len(pub.events) != 1 {
		t.Fatalf("events: got %d, want 1", len(pub.events))
	}
	ev := pub.events[0]
	if ev.Type != event.EventOrphanScanCompleted {
		t.Errorf("event type: got %q, want %q", ev.Type, event.EventOrphanScanCompleted)
	}
	payload, ok := ev.Payload.(event.OrphanScanCompletedPayload)
	if !ok {
		t.Fatalf("payload type: got %T, want OrphanScanCompletedPayload", ev.Payload)
	}
	if payload.BlobsRemoved != 2 || payload.ManifestsRemoved != 3 ||
		payload.StagingRemoved != 1 || payload.ManifestsIndexed != 4 {
		t.Errorf("payload counts: got %+v", payload)
	}
	// The payload carries the error COUNT, not the error values.
	if payload.NonFatalErrors != 2 {
		t.Errorf("NonFatalErrors: got %d, want 2", payload.NonFatalErrors)
	}
}
