package storesuite

// End-to-end restarts: the cases where recovery is not an optimisation but
// the only thing between a reopen and an empty store (ADR-118, ADR-119).
//
// Paranoid is the sharp one — its index is ephemeral by construction, so
// every reopen goes through recovery. Plain is the control: the same restart
// with a persistent index must behave identically from the caller's side.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

// seedPayloads are written before each restart; their content doubles as the
// assertion that the artifact came back whole rather than merely present.
var seedPayloads = []string{"first artifact", "second artifact", "third artifact"}

// initParanoid brings up a Paranoid store over drv with a fresh in-memory
// index — the only index the mode admits (ADR-56).
func initParanoid(t *testing.T, drv driver.Driver, pass string) store.Store {
	t.Helper()
	s, _, err := store.InitStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPassphrase(storefx.StaticPP(pass)),
		store.WithConfig(config.StoreConfig{ManifestCrypto: config.ManifestCryptoParanoid}),
	)
	if err != nil {
		t.Fatalf("InitStore (paranoid): %v", err)
	}
	return s
}

// reopenParanoid opens the same Location in a new session with a NEW empty
// index. That is what a restart looks like when the index lives in memory,
// and it is the exact shape that used to wipe the corpus.
func reopenParanoid(t *testing.T, drv driver.Driver, pass string) (store.Store, error) {
	t.Helper()
	return store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithPassphrase(storefx.StaticPP(pass)),
		// Unwrap the DEK during open: recovery reads manifests, and in
		// Paranoid a manifest cannot be read without the key.
		store.WithAutoUnlock(),
	)
}

func seed(t *testing.T, s store.Store) []domain.ArtifactID {
	t.Helper()
	ids := make([]domain.ArtifactID, 0, len(seedPayloads))
	for _, p := range seedPayloads {
		id, err := s.Put(context.Background(), domain.Artifact{Payload: bytes.NewReader([]byte(p))})
		if err != nil {
			t.Fatalf("Put(%q): %v", p, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func mustReadAll(t *testing.T, s store.Store, id domain.ArtifactID) string {
	t.Helper()
	rh, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	defer rh.Close()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("read(%s): %v", id, err)
	}
	return string(got)
}

// TestE2E_Paranoid_RestartKeepsCorpus: the case that used to destroy a store.
// Reopening Paranoid means opening over an empty index; the pass at open used
// to delete every manifest it did not recognise. Now it reads them back in.
func TestE2E_Paranoid_RestartKeepsCorpus(t *testing.T) {
	drv := driverfx.LocalFS(t)
	const pass = "paranoid-restart"

	s := initParanoid(t, drv, pass)
	ids := seed(t, s)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := reopenParanoid(t, drv, pass)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	for i, id := range ids {
		if got := mustReadAll(t, s2, id); got != seedPayloads[i] {
			t.Errorf("artifact %d after restart: got %q, want %q", i, got, seedPayloads[i])
		}
	}
}

// TestE2E_Paranoid_RestartTwice: recovery must be repeatable, not a one-off.
// The second restart runs over an index rebuilt by the first.
func TestE2E_Paranoid_RestartTwice(t *testing.T) {
	drv := driverfx.LocalFS(t)
	const pass = "paranoid-twice"

	s := initParanoid(t, drv, pass)
	ids := seed(t, s)
	_ = s.Close()

	s2, err := reopenParanoid(t, drv, pass)
	if err != nil {
		t.Fatalf("first reopen: %v", err)
	}
	// A write in the second session must survive the third.
	extra, err := s2.Put(context.Background(), domain.Artifact{Payload: bytes.NewReader([]byte("added later"))})
	if err != nil {
		t.Fatalf("Put in second session: %v", err)
	}
	_ = s2.Close()

	s3, err := reopenParanoid(t, drv, pass)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer s3.Close()

	for i, id := range ids {
		if got := mustReadAll(t, s3, id); got != seedPayloads[i] {
			t.Errorf("artifact %d after two restarts: got %q, want %q", i, got, seedPayloads[i])
		}
	}
	if got := mustReadAll(t, s3, extra); got != "added later" {
		t.Errorf("artifact written in the second session: got %q", got)
	}
}

// TestE2E_Paranoid_DeleteSurvivesRestart: the delete-order case (ADR-30 as
// revised). A deleted artifact must NOT come back when the reconciliation
// reads unknown manifests into the index — which is why the file goes first
// and the row second.
func TestE2E_Paranoid_DeleteSurvivesRestart(t *testing.T) {
	drv := driverfx.LocalFS(t)
	const pass = "paranoid-delete"

	s := initParanoid(t, drv, pass)
	ids := seed(t, s)
	if err := s.Delete(context.Background(), ids[1]); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_ = s.Close()

	s2, err := reopenParanoid(t, drv, pass)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if _, err := s2.Get(context.Background(), ids[1]); !errors.Is(err, errs.ErrArtifactNotFound) {
		t.Fatalf("deleted artifact after restart: got %v, want ErrArtifactNotFound", err)
	}
	for _, i := range []int{0, 2} {
		if got := mustReadAll(t, s2, ids[i]); got != seedPayloads[i] {
			t.Errorf("artifact %d must be untouched by the deletion of another: got %q", i, got)
		}
	}
}

// TestE2E_Plain_RestartWithLostIndex_RefusesThenRebuilds: for a store whose
// index was supposed to survive, an empty index is a fault, not a routine.
// The open refuses; with explicit permission the same procedure runs and the
// corpus comes back.
func TestE2E_Plain_RestartWithLostIndex_RefusesThenRebuilds(t *testing.T) {
	s, re := storefx.InitPlain(t)
	ids := seed(t, s)
	_ = s.Close()

	drv := re.Driver()

	// The index "file" is gone: a fresh one over an existing Location.
	_, err := store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
	)
	if !errors.Is(err, errs.ErrIndexIncomplete) {
		t.Fatalf("open with a lost index: got %v, want ErrIndexIncomplete", err)
	}

	s2, err := store.OpenStore(context.Background(), drv,
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithAllowIndexRecovery(),
	)
	if err != nil {
		t.Fatalf("open with explicit permission: %v", err)
	}
	defer s2.Close()

	for i, id := range ids {
		if got := mustReadAll(t, s2, id); got != seedPayloads[i] {
			t.Errorf("artifact %d after an explicit rebuild: got %q, want %q", i, got, seedPayloads[i])
		}
	}
}

// TestE2E_Plain_RestartWithSameIndex_NoRebuild: the control. A persistent
// index that survived the close is trusted as it stands; the corpus reads
// back without any recovery at all.
func TestE2E_Plain_RestartWithSameIndex_NoRebuild(t *testing.T) {
	s, re := storefx.InitPlain(t)
	ids := seed(t, s)
	_ = s.Close()

	s2 := re.Open(t)
	defer s2.Close()

	for i, id := range ids {
		if got := mustReadAll(t, s2, id); got != seedPayloads[i] {
			t.Errorf("artifact %d after restart: got %q, want %q", i, got, seedPayloads[i])
		}
	}
}
