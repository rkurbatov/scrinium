// OpenStore: setup rejection, descriptor reconcile outcomes (heal /
// not-found / corrupt / split-brain), encrypted-open state, immutable
// config matching, and the full end-to-end disk round-trip. Reconcile
// outcomes and config matching are category-6 tables; the e2e is a
// category-5-style integration over real localfs + sqlite. InitStore
// itself lives in lifecycle_init_test.go.

package storesuite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/driver/localfs"
	sqliteindex "scrinium.dev/engine/index/sqlite"
	"scrinium.dev/engine/internal/named"
	"scrinium.dev/engine/store"
	"scrinium.dev/engine/store/internal/descriptor"
	"scrinium.dev/engine/store/internal/reconcile"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

// corruptDescriptorReplica writes a valid Plain manifest whose inline payload
// is not a descriptor: LoadCell succeeds (hash matches) but Unmarshal fails,
// a deterministic Corrupted classification for reconcile.
func corruptDescriptorReplica(t *testing.T, drv driver.Driver, r descriptor.Replica) {
	t.Helper()
	name, err := r.Name()
	if err != nil {
		t.Fatalf("replica name: %v", err)
	}
	body, _, err := named.BuildInlineManifest(name, []byte("{not json"), string(config.HashSHA256), storefx.Hashes(), config.ManifestCryptoPlain, nil, "")
	if err != nil {
		t.Fatalf("build corrupt manifest: %v", err)
	}
	if err := named.WriteCell(context.Background(), drv, name, body, false); err != nil {
		t.Fatalf("write corrupt cell: %v", err)
	}
}

// removeDescriptorReplica deletes one replica cell.
func removeDescriptorReplica(t *testing.T, drv driver.Driver, r descriptor.Replica) {
	t.Helper()
	name, err := r.Name()
	if err != nil {
		t.Fatalf("replica name: %v", err)
	}
	if err := named.RemoveCell(context.Background(), drv, name); err != nil {
		t.Fatalf("remove replica cell: %v", err)
	}
}

// TestOpenStore_SetupRejected: a nil driver fails; a missing StoreIndex
// fails with a message naming the option (same dependency inversion as
// InitStore).
func TestOpenStore_SetupRejected(t *testing.T) {
	cases := []struct {
		name       string
		run        func(t *testing.T) error
		wantSubstr string // "" = any error
	}{
		{"nil driver", func(t *testing.T) error {
			_, err := store.OpenStore(context.Background(), nil)
			return err
		}, ""},
		{"missing store index", func(t *testing.T) error {
			drv := driverfx.LocalFS(t)
			storefx.InitPlainOn(t, drv)
			_, err := store.OpenStore(context.Background(), drv)
			return err
		}, "WithStoreIndex"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t)
			if err == nil {
				t.Fatalf("%s: expected an error", tc.name)
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("%s: error %v should mention %q", tc.name, err, tc.wantSubstr)
			}
		})
	}
}

// TestOpenStore_ReconcileOutcomes: how the L0/L1 replica pair maps to an
// open result. Both absent (fresh or wiped) → ErrStoreNotFound; any
// surviving replica unparseable with no valid partner → ErrStoreCorrupted;
// two valid-but-divergent replicas at equal sequence → ErrDescriptorSplitBrain.
// (The healable single-absent cases are TestOpenStore_HealsAbsentReplica.)
func TestOpenStore_ReconcileOutcomes(t *testing.T) {
	freshLoc := func(t *testing.T) driver.Driver {
		return driverfx.LocalFS(t)
	}
	removeBoth := func(t *testing.T) driver.Driver {
		drv := driverfx.LocalFS(t)
		storefx.InitPlainOn(t, drv)
		_ = descriptor.RemoveBoth(context.Background(), drv)
		return drv
	}
	corruptL0Only := func(t *testing.T) driver.Driver {
		drv := driverfx.LocalFS(t)
		corruptDescriptorReplica(t, drv, descriptor.L0)
		return drv
	}
	corruptBoth := func(t *testing.T) driver.Driver {
		drv := driverfx.LocalFS(t)
		storefx.InitPlainOn(t, drv)
		corruptDescriptorReplica(t, drv, descriptor.L0)
		corruptDescriptorReplica(t, drv, descriptor.L1)
		return drv
	}
	splitBrain := func(t *testing.T) driver.Driver {
		drv := driverfx.LocalFS(t)
		storefx.InitPlainOn(t, drv)
		// Fabricate a divergent L1: same Sequence, different StoreID.
		d0, _, err := reconcile.ReadReplica(context.Background(), drv, storefx.Hashes(), descriptor.L0)
		if err != nil {
			t.Fatal(err)
		}
		imposter := *d0
		imposter.StoreID = "99999999-aaaa-bbbb-cccc-dddddddddddd"
		if err := descriptor.WriteReplica(context.Background(), drv, storefx.Hashes(), &imposter, descriptor.L1); err != nil {
			t.Fatal(err)
		}
		return drv
	}
	cases := []struct {
		name  string
		setup func(t *testing.T) driver.Driver
		want  error
	}{
		{"fresh location", freshLoc, errs.ErrStoreNotFound},
		{"both replicas removed", removeBoth, errs.ErrStoreNotFound},
		{"L0 corrupt, L1 absent", corruptL0Only, errs.ErrStoreCorrupted},
		{"both replicas corrupt", corruptBoth, errs.ErrStoreCorrupted},
		{"split brain (divergent L1)", splitBrain, errs.ErrDescriptorSplitBrain},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := tc.setup(t)
			if _, err := storefx.TryOpenOn(t, drv); !errors.Is(err, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

// TestOpenStore_HealsAbsentReplica: a write that completed one replica but
// not the other is repaired on open — OpenStore succeeds and the missing
// replica is rewritten from the survivor.
func TestOpenStore_HealsAbsentReplica(t *testing.T) {
	cases := []struct {
		name   string
		remove descriptor.Replica
		check  descriptor.Replica
	}{
		{"missing L0 healed from L1", descriptor.L0, descriptor.L0},
		{"missing L1 healed from L0", descriptor.L1, descriptor.L1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := driverfx.LocalFS(t)
			storefx.InitPlainOn(t, drv)
			removeDescriptorReplica(t, drv, tc.remove)
			if _, err := storefx.TryOpenOn(t, drv); err != nil {
				t.Fatalf("OpenStore: %v", err)
			}
			if _, status, err := reconcile.ReadReplica(context.Background(), drv, storefx.Hashes(), tc.check); err != nil || status != reconcile.Valid {
				t.Errorf("%s: replica should be healed: status=%v, err=%v", tc.name, status, err)
			}
		})
	}
}

// TestOpenStore_EncryptedState: the open state of a store given its crypto
// and the auto-unlock options. Plain and auto-unlocked-with-passphrase
// open Unlocked; an encrypted store without auto-unlock opens Locked;
// auto-unlock without a passphrase fails ErrPassphraseRequired and with a
// wrong one ErrDecryptionFailed.
func TestOpenStore_EncryptedState(t *testing.T) {
	const pw = "hunter2"
	initPlain := func(t *testing.T, drv driver.Driver) { storefx.InitPlainOn(t, drv) }
	initEnc := func(t *testing.T, drv driver.Driver) { storefx.InitEncryptedOn(t, drv, pw) }

	cases := []struct {
		name      string
		init      func(t *testing.T, drv driver.Driver)
		openOpts  []store.StoreOption
		wantState domain.StoreState // checked only when wantErr == nil
		wantErr   error
	}{
		{"plain round-trip", initPlain, nil, domain.StateUnlocked, nil},
		{"encrypted, no auto-unlock → locked", initEnc, nil, domain.StateLocked, nil},
		{"encrypted, auto-unlock → unlocked", initEnc,
			[]store.StoreOption{store.WithPassphrase(storefx.StaticPP(pw)), store.WithAutoUnlock()},
			domain.StateUnlocked, nil},
		{"auto-unlock without passphrase", initEnc,
			[]store.StoreOption{store.WithAutoUnlock()},
			"", errs.ErrPassphraseRequired},
		{"auto-unlock wrong passphrase", initEnc,
			[]store.StoreOption{store.WithPassphrase(storefx.StaticPP("wrong")), store.WithAutoUnlock()},
			"", errs.ErrDecryptionFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := driverfx.LocalFS(t)
			tc.init(t, drv)
			s, err := storefx.TryOpenOn(t, drv, tc.openOpts...)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("%s: got %v, want %v", tc.name, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: OpenStore: %v", tc.name, err)
			}
			if s.State() != tc.wantState {
				t.Errorf("%s: State got %v, want %v", tc.name, s.State(), tc.wantState)
			}
		})
	}
}

// TestOpenStore_NonPlainManifestCryptoOpens: OpenStore accepts Sealed and
// Paranoid configurations (with auto-unlock) and reaches Unlocked. The
// body-encryption path itself is covered by the Put/Get tests; this only
// pins that such configs are accepted at open.
func TestOpenStore_NonPlainManifestCryptoOpens(t *testing.T) {
	for _, crypto := range []config.ManifestCrypto{
		config.ManifestCryptoSealed,
		config.ManifestCryptoParanoid,
	} {
		t.Run(string(crypto), func(t *testing.T) {
			drv := driverfx.LocalFS(t)
			idx := indexfx.Memory(t)
			cfg := config.StoreConfig{ManifestCrypto: crypto}

			if _, _, err := store.InitStore(context.Background(), drv,
				store.WithConfig(cfg),
				store.WithPassphrase(storefx.StaticPP("pw")),
				store.WithStoreIndex(idx),
				store.WithHashRegistry(storefx.Hashes()),
			); err != nil {
				t.Fatalf("InitStore: %v", err)
			}

			s, err := store.OpenStore(context.Background(), drv,
				store.WithConfig(cfg),
				store.WithPassphrase(storefx.StaticPP("pw")),
				store.WithAutoUnlock(),
				store.WithStoreIndex(idx),
				store.WithHashRegistry(storefx.Hashes()),
			)
			if err != nil {
				t.Fatalf("OpenStore: %v", err)
			}
			if s.State() != domain.StateUnlocked {
				t.Errorf("State: got %v, want Unlocked", s.State())
			}
		})
	}
}

// TestOpenStore_ConfigMatch: an immutable parameter supplied on open must
// match what InitStore persisted. An unset (zero) field is "not asserted"
// rather than a request for the zero value — hence partial configs pass,
// and DeletionPolicyLock can only be asserted true (asking for the
// engaged lock), never relaxed to false. Successful opens stay Unlocked.
func TestOpenStore_ConfigMatch(t *testing.T) {
	flat := config.StoreConfig{PathTopology: config.PathTopologyFlat, ContentHasher: config.HashBLAKE3}
	cases := []struct {
		name    string
		initCfg config.StoreConfig
		openCfg *config.StoreConfig // nil = open without WithConfig
		want    error
	}{
		{"no config on open", flat, nil, nil},
		{"matching config", flat, &flat, nil},
		{"mismatch path topology",
			config.StoreConfig{PathTopology: config.PathTopologyFlat},
			&config.StoreConfig{PathTopology: config.PathTopologySharded}, errs.ErrConfigMismatch},
		{"mismatch content hasher",
			config.StoreConfig{ContentHasher: config.HashSHA256},
			&config.StoreConfig{ContentHasher: config.HashBLAKE3}, errs.ErrConfigMismatch},
		{"partial config, unset field ignored", flat,
			&config.StoreConfig{ContentHasher: config.HashBLAKE3}, nil},
		{"deletion-lock stricter request mismatches",
			config.StoreConfig{DeletionPolicyLock: false},
			&config.StoreConfig{DeletionPolicyLock: true}, errs.ErrConfigMismatch},
		{"deletion-lock relaxed request ok",
			config.StoreConfig{DeletionPolicyLock: false},
			&config.StoreConfig{DeletionPolicyLock: false}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := driverfx.LocalFS(t)
			if _, _, err := store.InitStore(context.Background(), drv,
				store.WithConfig(tc.initCfg),
				store.WithStoreIndex(indexfx.Memory(t)),
				store.WithHashRegistry(storefx.Hashes()),
			); err != nil {
				t.Fatalf("%s: InitStore: %v", tc.name, err)
			}
			opts := []store.StoreOption{
				store.WithStoreIndex(indexfx.Memory(t)),
				store.WithHashRegistry(storefx.Hashes()),
			}
			if tc.openCfg != nil {
				opts = append(opts, store.WithConfig(*tc.openCfg))
			}
			s, err := store.OpenStore(context.Background(), drv, opts...)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("%s: got %v, want success", tc.name, err)
				}
				if s.State() != domain.StateUnlocked {
					t.Errorf("%s: state got %v, want Unlocked", tc.name, s.State())
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

// TestOpenStore_RestoresImmutableConfig: the active config of an opened
// store reflects the values persisted in system.config at init, not
// whatever defaults the caller passes (here, nothing).
func TestOpenStore_RestoresImmutableConfig(t *testing.T) {
	drv := driverfx.LocalFS(t)
	custom := config.StoreConfig{
		PathTopology:  config.PathTopologyFlat,
		ContentHasher: config.HashBLAKE3,
	}
	if _, _, err := store.InitStore(context.Background(), drv,
		store.WithConfig(custom),
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
	); err != nil {
		t.Fatal(err)
	}

	s, err := storefx.TryOpenOn(t, drv)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	got := s.Config()
	if got.PathTopology != "Flat" || got.ContentHasher != "blake3" {
		t.Errorf("active config should preserve InitStore values: %+v", got)
	}
}

// TestOpenStore_FirstOpenOnFreshIndex: the first open on a fresh
// in-memory index must succeed without crashing — a smoke on the open
// path (descriptor reconcile + system config read).
func TestOpenStore_FirstOpenOnFreshIndex(t *testing.T) {
	drv := driverfx.LocalFS(t)
	storefx.InitPlainOn(t, drv)
	if _, err := storefx.TryOpenOn(t, drv); err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
}

// TestLifecycle_FullDiskRoundTrip is the end-to-end lifecycle over real
// on-disk artifacts (localfs + sqlite): init writes the descriptor, the first
// system/config version, and the index; the open store serves
// Capacity/Walk/SetMaintenanceMode; after closing the index a fresh
// session reopens the same Location with the same StoreID and active
// config and resolves seeded index content; a conflicting immutable on
// reopen is rejected with ErrConfigMismatch.
func TestLifecycle_FullDiskRoundTrip(t *testing.T) {
	ctx := t.Context()
	location := t.TempDir()
	indexPath := filepath.Join(t.TempDir(), "index.db")

	descCellPath, _ := named.CellPath(descriptor.Name)
	if _, err := os.Stat(filepath.Join(location, descCellPath)); !os.IsNotExist(err) {
		t.Fatalf("descriptor unexpectedly present before Init: %v", err)
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("index unexpectedly present before Init: %v", err)
	}

	// --- Phase 1: InitStore ---
	drv1, err := localfs.New(location, localfs.WithFsync(false))
	if err != nil {
		t.Fatalf("localfs.New (phase 1): %v", err)
	}
	idx1, err := sqliteindex.NewStore(context.Background(), indexPath)
	if err != nil {
		t.Fatalf("sqlite.NewStore (phase 1): %v", err)
	}

	custom := config.StoreConfig{
		PathTopology:  config.PathTopologyFlat,
		ContentHasher: config.HashBLAKE3,
	}
	s1, kit, err := store.InitStore(context.Background(), drv1,
		store.WithConfig(custom),
		store.WithStoreIndex(idx1),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if err != nil {
		t.Fatalf("InitStore: %v", err)
	}
	if kit != nil {
		t.Errorf("RecoveryKit on Plain Store should be nil, got %d bytes", len(kit))
	}
	if s1.State() != domain.StateUnlocked {
		t.Errorf("phase 1 state: got %v, want %v", s1.State(), domain.StateUnlocked)
	}

	descCellPath2, _ := named.CellPath(descriptor.Name)
	descPath := filepath.Join(location, descCellPath2)
	if _, err := os.Stat(descPath); err != nil {
		t.Fatalf("descriptor not on disk after Init: %v", err)
	}
	cfgVersion, err := named.VersionPath("store.config", 1)
	if err != nil {
		t.Fatalf("VersionPath: %v", err)
	}
	if _, err := os.Stat(filepath.Join(location, filepath.FromSlash(cfgVersion))); err != nil {
		t.Fatalf("system config version not on disk after Init: %v", err)
	}
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("index not on disk after Init: %v", err)
	}

	desc1, err := descriptor.Read(context.Background(), drv1, storefx.Hashes())
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	if desc1.StoreID == "" {
		t.Fatal("StoreID is empty in descriptor")
	}
	if desc1.SchemaVersion != descriptor.CurrentSchemaVersion {
		t.Errorf("SchemaVersion: got %d, want %d", desc1.SchemaVersion, descriptor.CurrentSchemaVersion)
	}
	if desc1.Sequence != 1 {
		t.Errorf("Sequence: got %d, want 1", desc1.Sequence)
	}

	cfg1 := s1.Config()
	if cfg1.PathTopology != "Flat" {
		t.Errorf("active PathTopology: got %q, want Flat", cfg1.PathTopology)
	}
	if cfg1.ContentHasher != "blake3" {
		t.Errorf("active ContentHasher: got %q, want blake3", cfg1.ContentHasher)
	}

	// --- Phase 2: exercise the open Store ---
	info, err := s1.Capacity(context.Background())
	if err != nil {
		t.Fatalf("Capacity (phase 2): %v", err)
	}
	if info.ArtifactCount != 0 || info.BlobCount != 0 {
		t.Errorf("Capacity on fresh Store: %+v", info)
	}

	var walked int
	if err := s1.Walk(context.Background(), func(m domain.Manifest) error {
		walked++
		return nil
	}); err != nil {
		t.Fatalf("Walk (phase 2): %v", err)
	}
	if walked != 0 {
		t.Errorf("Walk on fresh Store yielded %d manifests", walked)
	}

	if err := s1.SetMaintenanceMode(context.Background(), domain.MaintenanceModeReadOnly); err != nil {
		t.Errorf("SetMaintenanceMode ReadOnly: %v", err)
	}
	if err := s1.SetMaintenanceMode(context.Background(), domain.MaintenanceModeNone); err != nil {
		t.Errorf("SetMaintenanceMode None: %v", err)
	}

	// --- Phase 3: seed index content directly, then close ---
	addr := domain.PhysicalAddress{Path: "blobs/aa/bb/blob-test"}
	manifest := domain.Manifest{
		ArtifactID:   "art-test",
		Digest:       domain.ManifestDigest("a47e57000000000000000000000000000000000000000000000000000000dead"),
		ContentHash:  domain.ContentHash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		BlobRefs:     []domain.BlobRef{"b10b7e57"},
		OriginalSize: 1024,
	}
	if err := idx1.IndexManifest(ctx, manifest, addr); err != nil {
		t.Fatalf("seed index: %v", err)
	}
	if err := idx1.Close(); err != nil {
		t.Fatalf("close index (phase 3): %v", err)
	}

	// --- Phase 4: reopen ---
	drv2, err := localfs.New(location, localfs.WithFsync(false))
	if err != nil {
		t.Fatalf("localfs.New (phase 4): %v", err)
	}
	idx2, err := sqliteindex.NewStore(context.Background(), indexPath)
	if err != nil {
		t.Fatalf("sqlite.NewStore (phase 4): %v", err)
	}

	s2, err := store.OpenStore(context.Background(), drv2,
		store.WithStoreIndex(idx2),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if s2.State() != domain.StateUnlocked {
		t.Errorf("phase 4 state: got %v, want %v", s2.State(), domain.StateUnlocked)
	}

	desc2, err := descriptor.Read(context.Background(), drv2, storefx.Hashes())
	if err != nil {
		t.Fatalf("read descriptor (phase 4): %v", err)
	}
	if desc2.StoreID != desc1.StoreID {
		t.Errorf("StoreID changed across reopen: %q -> %q", desc1.StoreID, desc2.StoreID)
	}

	cfg2 := s2.Config()
	if cfg2.PathTopology != cfg1.PathTopology {
		t.Errorf("PathTopology changed across reopen: %q -> %q", cfg1.PathTopology, cfg2.PathTopology)
	}
	if cfg2.ContentHasher != cfg1.ContentHasher {
		t.Errorf("ContentHasher changed across reopen: %q -> %q", cfg1.ContentHasher, cfg2.ContentHasher)
	}

	gotAddr, err := idx2.Resolve(ctx, "b10b7e57")
	if err != nil {
		t.Fatalf("Resolve after reopen: %v", err)
	}
	if gotAddr.Path != "blobs/aa/bb/blob-test" {
		t.Errorf("blob path after reopen: got %q, want %q", gotAddr.Path, "blobs/aa/bb/blob-test")
	}

	if err := idx2.Close(); err != nil {
		t.Fatalf("close index (phase 4): %v", err)
	}

	// --- Phase 5: ErrConfigMismatch on a conflicting reopen ---
	drv3, err := localfs.New(location, localfs.WithFsync(false))
	if err != nil {
		t.Fatalf("localfs.New (phase 5): %v", err)
	}
	idx3, err := sqliteindex.NewStore(context.Background(), indexPath)
	if err != nil {
		t.Fatalf("sqlite.NewStore (phase 5): %v", err)
	}
	defer idx3.Close()

	conflict := config.StoreConfig{PathTopology: config.PathTopologySharded} // active is Flat
	_, err = store.OpenStore(context.Background(), drv3,
		store.WithConfig(conflict),
		store.WithStoreIndex(idx3),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if !errors.Is(err, errs.ErrConfigMismatch) {
		t.Fatalf("expected errs.ErrConfigMismatch on conflicting reopen, got %v", err)
	}
}
