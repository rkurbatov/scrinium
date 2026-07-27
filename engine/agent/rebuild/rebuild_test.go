package rebuild_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/agent/rebuild"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/internal/checkpointfmt"
	"scrinium.dev/engine/internal/named"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/systemstore"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/artifactfx"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/eventfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/leasefx"
	"scrinium.dev/testutil/storefx"
)

const rebuildHostID = "rebuild-host-0001"

// rebuildFixture writes artifacts through a store (so manifest files
// land on the Location and the original index is populated), then
// exposes a SECOND, empty index on the same driver. That empty index is
// the rebuild target: it stands in for a lost index the agent must
// reconstruct from the on-disk manifests.
type rebuildFixture struct {
	store   store.Store
	drv     driver.Driver
	origIdx index.StoreIndex
	rec     *eventfx.Recorder
	rebuilt index.StoreIndex
}

func newRebuildFixture(t *testing.T, opts ...store.StoreOption) rebuildFixture {
	t.Helper()
	rec := eventfx.New()
	all := append([]store.StoreOption{store.WithPublisher(rec)}, opts...)
	st, drv, idx := storefx.InitShared(t, all...)
	return rebuildFixture{
		store:   st,
		drv:     drv,
		origIdx: idx,
		rec:     rec,
		rebuilt: indexfx.Memory(t),
	}
}

func (f rebuildFixture) put(t *testing.T, ns, data string) domain.ArtifactID {
	t.Helper()
	id, err := f.store.Put(context.Background(), artifactfx.Payload(data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return id
}

// publishCheckpoint vacuums the populated original index into a temp file and
// publishes it under the checkpoint prefix in the Store's System() namespace,
// exactly as the checkpoint agent would. Returns the checkpoint name.
func (f rebuildFixture) publishCheckpoint(t *testing.T) string {
	t.Helper()
	cw, ok := f.origIdx.(index.CheckpointWriter)
	if !ok {
		t.Fatal("origIdx does not implement index.CheckpointWriter")
	}
	tmp := filepath.Join(t.TempDir(), "cp.db")
	if err := cw.WriteCheckpoint(context.Background(), tmp); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	rc, err := os.Open(tmp)
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer rc.Close()
	name := checkpointfmt.Name(time.Now())
	if err := f.store.System().Put(context.Background(),
		systemstore.NamedArtifact{Name: name, Payload: rc}); err != nil {
		t.Fatalf("publish checkpoint: %v", err)
	}
	return name
}

func newRebuild(t *testing.T, f rebuildFixture, cfg rebuild.RebuildConfig) rebuild.RebuildIndexAgent {
	t.Helper()
	a, err := rebuild.NewRebuildIndexAgent(f.store, f.drv, f.rebuilt, f.rec, rebuildHostID, "store-rebuild", cfg)
	if err != nil {
		t.Fatalf("NewRebuildIndexAgent: %v", err)
	}
	return a
}

func TestNewRebuild_RequiresDeps(t *testing.T) {
	f := newRebuildFixture(t)
	cases := map[string]func() (rebuild.RebuildIndexAgent, error){
		"nil store": func() (rebuild.RebuildIndexAgent, error) {
			return rebuild.NewRebuildIndexAgent(nil, f.drv, f.rebuilt, f.rec, rebuildHostID, "", rebuild.RebuildConfig{})
		},
		"nil driver": func() (rebuild.RebuildIndexAgent, error) {
			return rebuild.NewRebuildIndexAgent(f.store, nil, f.rebuilt, f.rec, rebuildHostID, "", rebuild.RebuildConfig{})
		},
		"nil index": func() (rebuild.RebuildIndexAgent, error) {
			return rebuild.NewRebuildIndexAgent(f.store, f.drv, nil, f.rec, rebuildHostID, "", rebuild.RebuildConfig{})
		},
		"nil bus": func() (rebuild.RebuildIndexAgent, error) {
			return rebuild.NewRebuildIndexAgent(f.store, f.drv, f.rebuilt, nil, rebuildHostID, "", rebuild.RebuildConfig{})
		},
		"empty host": func() (rebuild.RebuildIndexAgent, error) {
			return rebuild.NewRebuildIndexAgent(f.store, f.drv, f.rebuilt, f.rec, "", "", rebuild.RebuildConfig{})
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := mk(); err == nil {
				t.Fatal("expected constructor error, got nil")
			}
		})
	}
}

func TestRebuild_FullScan_RecoversTargetManifests(t *testing.T) {
	f := newRebuildFixture(t)
	id1 := f.put(t, "r", "first artifact payload that overflows inline")
	id2 := f.put(t, "r", "second artifact payload also target sized")

	for _, id := range []domain.ArtifactID{id1, id2} {
		if _, ok, _ := f.rebuilt.ResolveManifestDigest(context.Background(), id); ok {
			t.Fatalf("rebuild target unexpectedly already has %s", id)
		}
	}

	a := newRebuild(t, f, rebuild.RebuildConfig{Source: rebuild.RebuildSourceFullScan})
	res, err := a.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res == nil {
		t.Fatal("nil AgentResult")
	}

	for _, id := range []domain.ArtifactID{id1, id2} {
		_, ok, err := f.rebuilt.ResolveManifestDigest(context.Background(), id)
		if err != nil {
			t.Fatalf("ResolveManifestDigest(%s): %v", id, err)
		}
		if !ok {
			t.Errorf("manifest %s not recovered into the rebuilt index", id)
		}
	}
	st := a.Stats()
	if st.ManifestsIndexed < 2 {
		t.Errorf("ManifestsIndexed = %d, want >= 2", st.ManifestsIndexed)
	}
	if st.Source != rebuild.RebuildSourceFullScan {
		t.Errorf("Source = %q, want FullScan", st.Source)
	}
}

func TestRebuild_FullScan_RecoversInlineManifests(t *testing.T) {
	f := newRebuildFixture(t, store.WithConfig(config.StoreConfig{
		BlobStorage:     config.BlobStorageInline,
		InlineBlobLimit: 1024,
	}))
	id := f.put(t, "r", "x")

	a := newRebuild(t, f, rebuild.RebuildConfig{})
	if _, err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, ok, err := f.rebuilt.ResolveManifestDigest(context.Background(), id)
	if err != nil {
		t.Fatalf("ResolveManifestDigest: %v", err)
	}
	if !ok {
		t.Errorf("inline manifest %s not recovered", id)
	}
}

func TestRebuild_Validate_CheckpointSourceUnavailable(t *testing.T) {
	f := newRebuildFixture(t)
	a := newRebuild(t, f, rebuild.RebuildConfig{Source: rebuild.RebuildSourceCheckpoint})
	if err := a.Validate(context.Background()); !errors.Is(err, errs.ErrNoCheckpoint) {
		t.Fatalf("Validate(Checkpoint) = %v, want ErrNoCheckpoint", err)
	}
}

func TestRebuild_Validate_CheckpointAvailable(t *testing.T) {
	f := newRebuildFixture(t)
	f.publishCheckpoint(t) // now a checkpoint exists under the prefix
	a := newRebuild(t, f, rebuild.RebuildConfig{Source: rebuild.RebuildSourceCheckpoint})
	if err := a.Validate(context.Background()); err != nil {
		t.Fatalf("Validate(Checkpoint) with a checkpoint present: %v", err)
	}
}

func TestRebuild_Checkpoint_FastPath(t *testing.T) {
	f := newRebuildFixture(t)
	f.put(t, "ns", "artifact written before the checkpoint")
	name := f.publishCheckpoint(t)

	a := newRebuild(t, f, rebuild.RebuildConfig{
		Source:   rebuild.RebuildSourceCheckpoint,
		LeaseTTL: time.Minute,
	})
	if _, err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run (checkpoint fast-path): %v", err)
	}
	st := a.Stats()
	if st.Source != rebuild.RebuildSourceCheckpoint {
		t.Errorf("Source = %q, want %q", st.Source, rebuild.RebuildSourceCheckpoint)
	}
	if st.CheckpointUsed != name {
		t.Errorf("CheckpointUsed = %q, want %q", st.CheckpointUsed, name)
	}
}

// publishCheckpointExternal mirrors publishCheckpoint but stores the .db as an
// external headless data artifact and publishes a pointer envelope under the
// checkpoint name — exactly as the checkpoint agent does post-ADR-105.
func (f rebuildFixture) publishCheckpointExternal(t *testing.T) string {
	t.Helper()
	cw, ok := f.origIdx.(index.CheckpointWriter)
	if !ok {
		t.Fatal("origIdx does not implement index.CheckpointWriter")
	}
	tmp := filepath.Join(t.TempDir(), "cp.db")
	if err := cw.WriteCheckpoint(context.Background(), tmp); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	rc, err := os.Open(tmp)
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer rc.Close()
	hw, ok := store.HeadlessOf(f.store)
	if !ok {
		t.Fatal("store has no headless data plane")
	}
	digest, err := hw.WriteHeadless(context.Background(), rc)
	if err != nil {
		t.Fatalf("WriteHeadless: %v", err)
	}
	name := checkpointfmt.Name(time.Now())
	if err := f.store.System().Put(context.Background(),
		systemstore.NamedArtifact{Name: name, ExternalRef: digest}); err != nil {
		t.Fatalf("publish external checkpoint: %v", err)
	}
	return name
}

// The rebuild fast-path restores from a checkpoint stored as an external
// headless artifact (ADR-105): System().Get resolves the pointer envelope back
// to the .db stream, which fetchCheckpoint writes to a temp file and replays.
// Identity holds by construction (same store); the systemstore enforces the
// checkpoint store_id unconditionally (ADR-104).
func TestRebuild_Checkpoint_FastPath_External(t *testing.T) {
	f := newRebuildFixture(t)
	f.put(t, "ns", "artifact written before the checkpoint")
	name := f.publishCheckpointExternal(t)

	a := newRebuild(t, f, rebuild.RebuildConfig{
		Source:   rebuild.RebuildSourceCheckpoint,
		LeaseTTL: time.Minute,
	})
	if _, err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run (external checkpoint fast-path): %v", err)
	}
	st := a.Stats()
	if st.Source != rebuild.RebuildSourceCheckpoint {
		t.Errorf("Source = %q, want %q", st.Source, rebuild.RebuildSourceCheckpoint)
	}
	if st.CheckpointUsed != name {
		t.Errorf("CheckpointUsed = %q, want %q", st.CheckpointUsed, name)
	}
}

func TestRebuild_Auto_FallsBackToFullScanWithoutCheckpoint(t *testing.T) {
	f := newRebuildFixture(t)
	f.put(t, "ns", "artifact on the location")
	a := newRebuild(t, f, rebuild.RebuildConfig{
		Source:   rebuild.RebuildSourceAuto,
		LeaseTTL: time.Minute,
	})
	if _, err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run (auto, no checkpoint): %v", err)
	}
	if st := a.Stats(); st.Source != rebuild.RebuildSourceFullScan {
		t.Errorf("Source = %q, want FullScan fallback", st.Source)
	}
}

func TestRebuild_Validate_AutoSourcePasses(t *testing.T) {
	f := newRebuildFixture(t)
	a := newRebuild(t, f, rebuild.RebuildConfig{Source: rebuild.RebuildSourceAuto})
	if err := a.Validate(context.Background()); err != nil {
		t.Fatalf("Validate(Auto) = %v, want nil", err)
	}
}

func TestRebuild_RecoveryKit_CorruptedFails(t *testing.T) {
	f := newRebuildFixture(t)
	a := newRebuild(t, f, rebuild.RebuildConfig{RecoveryKit: []byte("not a valid kit")})
	if _, err := a.Run(context.Background()); !errors.Is(err, errs.ErrRecoveryKitCorrupted) {
		t.Fatalf("Run(corrupted kit) = %v, want ErrRecoveryKitCorrupted", err)
	}
}

// TestRebuild_RecoveryKit_RestoresDescriptor drives the catastrophic
// recovery path: an encrypted Store's descriptor replicas are deleted,
// then the agent rewrites the descriptor replicas from the kit before scanning. The
// scan itself reindexes nothing here (a Sealed Store's manifests are not
// decodable without a KeyProvider on M3) — the assertion is on the
// descriptor restore.
func TestRebuild_RecoveryKit_RestoresDescriptor(t *testing.T) {
	ctx := context.Background()
	drv := driverfx.LocalFS(t)
	idx := indexfx.Memory(t)
	rec := eventfx.New()

	st, kit, err := store.InitStore(ctx, drv,
		store.WithHashRegistry(storefx.Hashes()),
		store.WithStoreIndex(idx),
		store.WithPassphrase(storefx.StaticPP("pw")),
		store.WithPublisher(rec),
	)
	if err != nil {
		t.Fatalf("InitStore (encrypted): %v", err)
	}
	if len(kit) == 0 {
		t.Fatal("empty recovery kit for an encrypted store")
	}
	if _, err := st.Put(ctx, artifactfx.Payload("payload that lands as a manifest file on the location")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Simulate catastrophic descriptor loss.
	root := drv.Root()
	for _, name := range []string{"store.descriptor", "store.descriptor.backup"} {
		cp, err := named.CellPath(name)
		if err != nil {
			t.Fatalf("CellPath %s: %v", name, err)
		}
		if err := os.Remove(filepath.Join(root, cp)); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}

	rebuilt := indexfx.Memory(t)
	a, err := rebuild.NewRebuildIndexAgent(st, drv, rebuilt, rec, rebuildHostID, "store-rebuild",
		rebuild.RebuildConfig{RecoveryKit: kit})
	if err != nil {
		t.Fatalf("NewRebuildIndexAgent: %v", err)
	}

	res, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run(valid kit) = %v, want nil", err)
	}
	if res == nil {
		t.Fatal("nil AgentResult")
	}
	if !a.Stats().DescriptorRewrote {
		t.Error("DescriptorRewrote = false, want true")
	}

	// The descriptor cell is back on the Location.
	descCellPath, err := named.CellPath("store.descriptor")
	if err != nil {
		t.Fatalf("CellPath: %v", err)
	}
	rc, err := drv.Get(ctx, descCellPath)
	if err != nil {
		t.Fatalf("descriptor not restored on disk: %v", err)
	}
	_ = rc.Close()
}

func TestRebuild_BlockedByForeignLease(t *testing.T) {
	f := newRebuildFixture(t)
	f.put(t, "r", "data large enough to be a target blob payload")
	leasefx.StageForeign(t, f.drv, "store.agent.maintenance.lease", "other-host", "RebuildIndex", time.Hour)
	a := newRebuild(t, f, rebuild.RebuildConfig{})
	if _, err := a.Run(context.Background()); err == nil {
		t.Fatal("Run with a live foreign maintenance lease = nil, want lease-held failure")
	}
}

func TestRebuild_CancelledContext(t *testing.T) {
	f := newRebuildFixture(t)
	f.put(t, "r", "data large enough to be a target blob payload")
	a := newRebuild(t, f, rebuild.RebuildConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Run(ctx); err == nil {
		t.Fatal("Run(cancelled) = nil, want error")
	}
}
