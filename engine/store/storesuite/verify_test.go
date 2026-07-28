// Verify: blob integrity scrubbing. Happy round-trips (target / inline /
// pipeline), corruption detection (tampered bytes, missing blob, tampered
// ciphertext under zstd/aes-gcm) folding into ErrCorruptedBlob and emitting
// EventScrubFailed, argument/state guards (not-found, offline, cancelled
// ctx — Verify is not in the cross-operation guard table), transparency to
// encrypted manifests, and Verify/Get consistency. Event capture goes
// through eventfx.Recorder.

package storesuite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/pipeline"
	"scrinium.dev/engine/pipeline/stage/aesgcm"
	zstdstage "scrinium.dev/engine/pipeline/stage/zstd"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
	"scrinium.dev/testutil/artifactfx"
	"scrinium.dev/testutil/driverfx"
	"scrinium.dev/testutil/eventfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
	"scrinium.dev/testutil/storekit"
)

// blobPathFor resolves an artifact's (sole) on-disk blob path via its
// manifest BlobRef, so tests don't hardcode the shard rule.
func blobPathFor(t *testing.T, s store.Store, root string, id domain.ArtifactID) string {
	t.Helper()
	ref := storekit.ReadBlobRef(t, s, id)
	return filepath.Join(root, storekit.BlobPathForRef(t, string(ref)))
}

// initPipelineStore builds a Store with the given Pipeline stages and a
// matching TransformerRegistry, returning the Store and the driver root.
func initPipelineStore(t *testing.T, reg pipeline.TransformerRegistry, stages []string, extra ...store.StoreOption) (store.Store, string) {
	t.Helper()
	cfg := config.StoreConfig{Pipeline: stages}
	drv := driverfx.LocalFS(t)
	idx := indexfx.Memory(t)
	opts := append([]store.StoreOption{
		store.WithStoreIndex(idx),
		store.WithReadRegistry(reg),
		store.WithConfig(cfg),
	}, extra...)
	s := storefx.InitOn(t, drv, opts...)
	return s, drv.Root()
}

// lastScrubPayload returns the most recent EventScrubFailed payload
// recorded, failing if none were seen.
func lastScrubPayload(t *testing.T, rec *eventfx.Recorder) event.ScrubFailedPayload {
	t.Helper()
	evts := rec.ByType(event.EventScrubFailed)
	if len(evts) == 0 {
		t.Fatalf("EventScrubFailed: no events seen")
	}
	p, ok := evts[len(evts)-1].Payload.(event.ScrubFailedPayload)
	if !ok {
		t.Fatalf("EventScrubFailed payload type: %T", evts[len(evts)-1].Payload)
	}
	return p
}

// TestVerify_Roundtrip: Verify passes on a freshly-written artifact for
// both the target and inline layouts.
func TestVerify_Roundtrip(t *testing.T) {
	cases := []struct {
		name string
		init func(t *testing.T) store.Store
	}{
		{"target", func(t *testing.T) store.Store { s, _ := storefx.InitWithRoot(t); return s }},
		{"inline", func(t *testing.T) store.Store { s, _ := storefx.InitInline(t, 1024); return s }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.init(t)
			id, err := s.Put(context.Background(), artifactfx.Payload("verify me"))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := s.Verify(context.Background(), id); err != nil {
				t.Errorf("Verify after fresh Put: %v", err)
			}
		})
	}
}

// TestVerify_CorruptBlobDetected: a tampered or missing target blob fails
// Verify with ErrCorruptedBlob and publishes one EventScrubFailed (the
// tampered case also pins the payload ArtifactID).
func TestVerify_CorruptBlobDetected(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(t *testing.T, path string)
		checkID bool
	}{
		{"tampered bytes", func(t *testing.T, p string) {
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read blob: %v", err)
			}
			if len(raw) == 0 {
				t.Fatalf("blob unexpectedly empty")
			}
			raw[0] ^= 0xff
			if err := os.WriteFile(p, raw, 0o644); err != nil {
				t.Fatalf("write tampered blob: %v", err)
			}
		}, true},
		{"missing blob", func(t *testing.T, p string) {
			if err := os.Remove(p); err != nil {
				t.Fatalf("remove blob: %v", err)
			}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := eventfx.New()
			s, root := storefx.InitWithRoot(t, store.WithPublisher(rec))
			id, err := s.Put(context.Background(), artifactfx.Payload("tamper target"))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			tc.mutate(t, blobPathFor(t, s, root, id))

			if err := s.Verify(context.Background(), id); !errors.Is(err, errs.ErrCorruptedBlob) {
				t.Fatalf("expected errs.ErrCorruptedBlob, got %v", err)
			}
			if n := rec.Count(event.EventScrubFailed); n != 1 {
				t.Fatalf("EventScrubFailed: got %d events, want 1", n)
			}
			if tc.checkID {
				if got := lastScrubPayload(t, rec).ArtifactID; got != id {
					t.Errorf("payload.ArtifactID: got %q, want %q", got, id)
				}
			}
		})
	}
}

// TestVerify_NotFound: Verify on an empty or unknown id reports
// ErrArtifactNotFound.
func TestVerify_NotFound(t *testing.T) {
	cases := []struct {
		name string
		id   domain.ArtifactID
	}{
		{"empty id", ""},
		{"unknown id", domain.ArtifactID("deadbeef")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := storefx.Init(t)
			if err := s.Verify(context.Background(), tc.id); !errors.Is(err, errs.ErrArtifactNotFound) {
				t.Errorf("got %v, want errs.ErrArtifactNotFound", err)
			}
		})
	}
}

// TestVerify_OfflineMode_Blocked: Verify is refused with ErrStoreOffline in
// Offline maintenance mode.
func TestVerify_OfflineMode_Blocked(t *testing.T) {
	s := storefx.Init(t)
	id, err := s.Put(context.Background(), artifactfx.Payload("offline test"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.SetMaintenanceMode(context.Background(), domain.MaintenanceModeOffline); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(context.Background(), id); !errors.Is(err, errs.ErrStoreOffline) {
		t.Errorf("got %v, want errs.ErrStoreOffline", err)
	}
}

// TestVerify_CancelledContext: a cancelled context surfaces as
// context.Canceled.
func TestVerify_CancelledContext(t *testing.T) {
	s := storefx.Init(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Verify(ctx, domain.ArtifactID("deadbeef")); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}

// TestVerify_EncryptedManifest_Succeeds: Verify is transparent to an
// encrypted manifest body — it succeeds under both Sealed and Paranoid.
func TestVerify_EncryptedManifest_Succeeds(t *testing.T) {
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

			id, err := s.Put(context.Background(), artifactfx.Payload("verify encrypted"))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := s.Verify(context.Background(), id); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

// TestVerify_Pipeline_Succeeds: Verify passes on a pipeline-bearing blob
// (zstd, aes-gcm, and the zstd→aes-gcm chain) — the on-disk bytes differ
// from the plaintext, yet the rehash through the inverse pipeline matches.
func TestVerify_Pipeline_Succeeds(t *testing.T) {
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i)
	}
	cases := []struct {
		name  string
		build func(t *testing.T) store.Store
	}{
		{"zstd", func(t *testing.T) store.Store {
			reg := pipeline.NewTransformerRegistry().
				Register("zstd", zstdstage.New(zstdstage.Options{}))
			s, _ := initPipelineStore(t, reg, []string{"zstd"})
			return s
		}},
		{"aes-gcm", func(t *testing.T) store.Store {
			aesFactory, err := aesgcm.New(dek)
			if err != nil {
				t.Fatalf("aesgcm.New: %v", err)
			}
			reg := pipeline.NewTransformerRegistry().Register("aes-gcm", aesFactory)
			s, _ := initPipelineStore(t, reg, []string{"aes-gcm"})
			return s
		}},
		{"zstd then aes-gcm", func(t *testing.T) store.Store {
			aesFactory, _ := aesgcm.New(dek)
			reg := pipeline.NewTransformerRegistry().
				Register("zstd", zstdstage.New(zstdstage.Options{})).
				Register("aes-gcm", aesFactory)
			s, _ := initPipelineStore(t, reg, []string{"zstd", "aes-gcm"})
			return s
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.build(t)
			original := bytes.Repeat([]byte("scrinium verify "), 1024)
			id, err := s.Put(context.Background(),
				domain.Artifact{Payload: bytes.NewReader(original)})
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := s.Verify(context.Background(), id); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

// TestVerify_Pipeline_TamperedCiphertext: a byte flipped in the middle of
// the transformed blob fails Verify with ErrCorruptedBlob and emits one
// EventScrubFailed — for AEAD (Open fails before the hasher) and for plain
// zstd (decode breaks or the rehash diverges). The aes-gcm case also pins
// the payload ArtifactID.
func TestVerify_Pipeline_TamperedCiphertext(t *testing.T) {
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i)
	}
	cases := []struct {
		name    string
		build   func(t *testing.T, rec *eventfx.Recorder) (store.Store, string)
		checkID bool
	}{
		{"aes-gcm", func(t *testing.T, rec *eventfx.Recorder) (store.Store, string) {
			aesFactory, _ := aesgcm.New(dek)
			reg := pipeline.NewTransformerRegistry().Register("aes-gcm", aesFactory)
			return initPipelineStore(t, reg, []string{"aes-gcm"}, store.WithPublisher(rec))
		}, true},
		{"zstd", func(t *testing.T, rec *eventfx.Recorder) (store.Store, string) {
			reg := pipeline.NewTransformerRegistry().
				Register("zstd", zstdstage.New(zstdstage.Options{}))
			return initPipelineStore(t, reg, []string{"zstd"}, store.WithPublisher(rec))
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := eventfx.New()
			s, root := tc.build(t, rec)
			original := bytes.Repeat([]byte("tamper-target "), 64)
			id, err := s.Put(context.Background(),
				domain.Artifact{Payload: bytes.NewReader(original)})
			if err != nil {
				t.Fatalf("Put: %v", err)
			}

			path := blobPathFor(t, s, root, id)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read blob: %v", err)
			}
			raw[len(raw)/2] ^= 0xff // mid-payload, clear of the trailing GCM tag
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatalf("rewrite blob: %v", err)
			}

			if err := s.Verify(context.Background(), id); !errors.Is(err, errs.ErrCorruptedBlob) {
				t.Fatalf("expected errs.ErrCorruptedBlob, got %v", err)
			}
			if n := rec.Count(event.EventScrubFailed); n != 1 {
				t.Fatalf("EventScrubFailed: got %d events, want 1", n)
			}
			if tc.checkID {
				if got := lastScrubPayload(t, rec).ArtifactID; got != id {
					t.Errorf("EventScrubFailed.ArtifactID: got %q, want %q", got, id)
				}
			}
		})
	}
}

// TestVerify_Pipeline_MissingBlob: a missing blob under a pipeline-bearing
// manifest surfaces as ErrCorruptedBlob, the same as the non-pipeline path.
func TestVerify_Pipeline_MissingBlob(t *testing.T) {
	reg := pipeline.NewTransformerRegistry().
		Register("zstd", zstdstage.New(zstdstage.Options{}))
	s, root := initPipelineStore(t, reg, []string{"zstd"})

	original := bytes.Repeat([]byte("gone "), 128)
	id, err := s.Put(context.Background(),
		domain.Artifact{Payload: bytes.NewReader(original)})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := os.Remove(blobPathFor(t, s, root, id)); err != nil {
		t.Fatalf("remove blob: %v", err)
	}
	if err := s.Verify(context.Background(), id); !errors.Is(err, errs.ErrCorruptedBlob) {
		t.Fatalf("expected errs.ErrCorruptedBlob, got %v", err)
	}
}

// TestVerify_Pipeline_ConsistentWithGet: after a successful Verify, Get
// returns the original plaintext — guards against Verify hashing one
// stream while Get returns a different one.
func TestVerify_Pipeline_ConsistentWithGet(t *testing.T) {
	reg := pipeline.NewTransformerRegistry().
		Register("zstd", zstdstage.New(zstdstage.Options{}))
	s, _ := initPipelineStore(t, reg, []string{"zstd"})

	original := bytes.Repeat([]byte("consistency "), 512)
	id, err := s.Put(context.Background(),
		domain.Artifact{Payload: bytes.NewReader(original)})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Verify(context.Background(), id); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	rh, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rh.Close()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("Get plaintext differs from original (len got=%d want=%d)", len(got), len(original))
	}
}
