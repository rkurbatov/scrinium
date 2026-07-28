package storesuite

// The Paranoid × index guard through the front door (ADR-56/119).
//
// The unit table in engine/store covers the decision itself, including the
// case an outsider cannot reach — an index that declares nothing. What only
// this level can show is that the refusal actually happens where a user
// meets it: at InitStore and at OpenStore, before any payload is written,
// and that the admissible combination goes through untouched.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/index"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/artifactfx"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

const guardPass = "paranoid-guard"

func initWith(t *testing.T, drv driver.Driver, idx index.StoreIndex, crypto config.ManifestCrypto) error {
	t.Helper()
	_, _, err := store.InitStore(context.Background(), drv,
		store.WithConfig(config.StoreConfig{ManifestCrypto: crypto}),
		store.WithPassphrase(storefx.StaticPP(guardPass)),
		store.WithStoreIndex(idx),
		store.WithHashRegistry(storefx.Hashes()),
	)
	return err
}

// A file-backed index holds, in the clear, the very fields Paranoid encrypts
// on disk. The store refuses rather than quietly undoing the mode.
func TestGuard_ParanoidRefusesDiskIndexAtInit(t *testing.T) {
	drv := driverfx.LocalFS(t)
	idx := indexfx.Disk(t, filepath.Join(t.TempDir(), "store.idx"))

	err := initWith(t, drv, idx, config.ManifestCryptoParanoid)
	if !errors.Is(err, errs.ErrUnsupportedCombination) {
		t.Fatalf("InitStore: got %v, want ErrUnsupportedCombination", err)
	}
}

// The refusal must land before anything is created: a Location left with a
// descriptor would be a half-made store nobody asked for.
func TestGuard_RefusedInitLeavesNothingBehind(t *testing.T) {
	drv := driverfx.LocalFS(t)
	idx := indexfx.Disk(t, filepath.Join(t.TempDir(), "store.idx"))

	if err := initWith(t, drv, idx, config.ManifestCryptoParanoid); err == nil {
		t.Fatal("InitStore unexpectedly succeeded")
	}

	// Reopening must report an empty Location, not a damaged store.
	_, err := store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPassphrase(storefx.StaticPP(guardPass)),
	)
	if !errors.Is(err, errs.ErrStoreNotFound) {
		t.Fatalf("after a refused init: got %v, want ErrStoreNotFound", err)
	}
}

// The admissible combination is not merely accepted — it works.
func TestGuard_ParanoidAcceptsEphemeralIndex(t *testing.T) {
	drv := driverfx.LocalFS(t)

	if err := initWith(t, drv, indexfx.Memory(t), config.ManifestCryptoParanoid); err != nil {
		t.Fatalf("InitStore with an in-memory index: %v", err)
	}

	s, err := store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPassphrase(storefx.StaticPP(guardPass)),
		store.WithAutoUnlock(),
	)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer s.Close()

	if _, err := s.Put(context.Background(), artifactfx.Payload("guarded")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// The other modes are untouched: a disk index is the ordinary choice there,
// and the guard has no business refusing it.
func TestGuard_OtherModesAcceptDiskIndex(t *testing.T) {
	for _, crypto := range []config.ManifestCrypto{
		config.ManifestCryptoPlain,
		config.ManifestCryptoSealed,
	} {
		t.Run(string(crypto), func(t *testing.T) {
			drv := driverfx.LocalFS(t)
			idx := indexfx.Disk(t, filepath.Join(t.TempDir(), "store.idx"))
			if err := initWith(t, drv, idx, crypto); err != nil {
				t.Fatalf("InitStore: %v", err)
			}
		})
	}
}

// A store created Paranoid must not be reopened against a disk index either:
// the config is authoritative, and the index handed to OpenStore need not be
// the one the store was created with.
func TestGuard_ParanoidRefusesDiskIndexAtOpen(t *testing.T) {
	drv := driverfx.LocalFS(t)

	if err := initWith(t, drv, indexfx.Memory(t), config.ManifestCryptoParanoid); err != nil {
		t.Fatalf("InitStore: %v", err)
	}

	_, err := store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Disk(t, filepath.Join(t.TempDir(), "store.idx"))),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPassphrase(storefx.StaticPP(guardPass)),
		store.WithAutoUnlock(),
	)
	if !errors.Is(err, errs.ErrUnsupportedCombination) {
		t.Fatalf("OpenStore: got %v, want ErrUnsupportedCombination", err)
	}
}
