package artifact_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/artifact"
	"scrinium.dev/errs"
	"scrinium.dev/testutil/artifactfx"
)

// --- Plain: encode/decode round-trip (full-field) ---

func TestEncodeDecode_PlainRoundTrip(t *testing.T) {
	m := artifactfx.Manifest()
	b, err := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := artifact.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.SessionID != m.SessionID {
		t.Errorf("session id lost: %+v", got)
	}
	if string(got.ContentHash) != string(m.ContentHash) || string(got.PrimaryBlobRef()) != string(m.PrimaryBlobRef()) {
		t.Errorf("content/blob ref lost: %+v", got)
	}
	if got.OriginalSize != m.OriginalSize {
		t.Errorf("OriginalSize lost: got %d want %d", got.OriginalSize, m.OriginalSize)
	}
	if !got.CreatedAt.Equal(m.CreatedAt) {
		t.Errorf("CreatedAt lost: got %v want %v", got.CreatedAt, m.CreatedAt)
	}
	if !bytes.Equal(got.Ext, m.Ext) {
		t.Errorf("Ext lost: got %s", got.Ext)
	}
	if !bytes.Equal(got.Usr, m.Usr) {
		t.Errorf("Usr lost: got %s", got.Usr)
	}
	if !bytes.Equal(got.InlineBlob, m.InlineBlob) {
		t.Errorf("InlineBlob lost: got %q", got.InlineBlob)
	}
}

// TestEncodeDecode_RetentionRoundTrip pins that RetentionUntil survives a
// Plain round-trip (the field is omitempty and was previously untested).
func TestEncodeDecode_RetentionRoundTrip(t *testing.T) {
	want := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	m := artifactfx.Manifest(func(m *domain.Manifest) { m.RetentionUntil = want })
	b, err := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := artifact.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RetentionUntil.Equal(want) {
		t.Errorf("RetentionUntil did not survive round-trip: got %v want %v", got.RetentionUntil, want)
	}
}

// TestEncode_PlainExtUsrVisibleOnDisk is the plaintext contrast to the
// Sealed/Paranoid hiding tests: Plain leaves ext and usr readable on disk.
func TestEncode_PlainExtUsrVisibleOnDisk(t *testing.T) {
	m := artifactfx.Manifest(func(m *domain.Manifest) {
		m.Ext = json.RawMessage(`{"m":"EXTMARKER_7f3a"}`)
		m.Usr = json.RawMessage(`{"m":"USRMARKER_91b2"}`)
	})
	b, err := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte("EXTMARKER_7f3a")) {
		t.Error("Plain should leave ext in plaintext on disk")
	}
	if !bytes.Contains(b, []byte("USRMARKER_91b2")) {
		t.Error("Plain should leave usr in plaintext on disk")
	}
}

func TestEncode_Deterministic(t *testing.T) {
	m := artifactfx.Manifest()
	a, _ := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	b, _ := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if !bytes.Equal(a, b) {
		t.Error("Encode is not deterministic for the same manifest")
	}
}

func TestEncode_RejectsNonPlain(t *testing.T) {
	if _, err := artifact.Encode(artifactfx.Manifest(), config.ManifestEncodingJSON, config.ManifestCryptoSealed); !errors.Is(err, errs.ErrUnsupportedCrypto) {
		t.Fatalf("want ErrUnsupportedCrypto, got %v", err)
	}
}

func TestDecode_RejectsEncrypted(t *testing.T) {
	_, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoSealed)
	if _, derr := artifact.Decode(b); !errors.Is(derr, errs.ErrUnsupportedCrypto) {
		t.Fatalf("Decode on encrypted file: want ErrUnsupportedCrypto, got %v", derr)
	}
}

// --- ComputeManifestDigest: stability + uniqueness + assignment ---

func TestComputeManifestDigest_StableAndAssigned(t *testing.T) {
	m := artifactfx.Manifest()
	dg1, b1, sm, err := artifact.ComputeManifestDigest(m, "sha256", artifactfx.Hashes(), config.ManifestEncodingJSON, config.ManifestCryptoPlain, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if dg1 == "" {
		t.Fatal("empty ManifestDigest")
	}
	if sm.Digest != dg1 {
		t.Errorf("returned manifest Digest=%q, want %q", sm.Digest, dg1)
	}
	dg2, b2, _, _ := artifact.ComputeManifestDigest(m, "sha256", artifactfx.Hashes(), config.ManifestEncodingJSON, config.ManifestCryptoPlain, nil, "")
	if dg1 != dg2 || !bytes.Equal(b1, b2) {
		t.Error("ComputeManifestDigest not stable across calls")
	}
}

func TestComputeManifestDigest_DifferentManifestsDifferentIDs(t *testing.T) {
	m1 := artifactfx.Manifest()
	m2 := artifactfx.Manifest(func(m *domain.Manifest) { m.SessionID = "other" })
	dg1, _, _, _ := artifact.ComputeManifestDigest(m1, "sha256", artifactfx.Hashes(), config.ManifestEncodingJSON, config.ManifestCryptoPlain, nil, "")
	dg2, _, _, _ := artifact.ComputeManifestDigest(m2, "sha256", artifactfx.Hashes(), config.ManifestEncodingJSON, config.ManifestCryptoPlain, nil, "")
	if dg1 == dg2 {
		t.Error("different manifests produced the same ManifestDigest")
	}
}

// TestComputeManifestDigest_RejectsEmptyDEKOnEncrypted: a non-Plain mode with
// no DEK is rejected before any encoding work.
func TestComputeManifestDigest_RejectsEmptyDEKOnEncrypted(t *testing.T) {
	if _, _, _, err := artifact.ComputeManifestDigest(artifactfx.Manifest(), "sha256", artifactfx.Hashes(), config.ManifestEncodingJSON, config.ManifestCryptoSealed, nil, ""); err == nil {
		t.Fatal("Sealed encode with empty DEK must be rejected")
	}
}

// --- VerifyManifestDigest: tamper detection ---

func TestVerifyManifestDigest_AcceptsUntampered(t *testing.T) {
	id, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoPlain)
	if err := artifact.VerifyManifestDigest(id, b, "sha256", artifactfx.Hashes()); err != nil {
		t.Fatalf("verify untampered: %v", err)
	}
}

func TestVerifyManifestDigest_DetectsTampering(t *testing.T) {
	id, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoPlain)
	b[len(b)-1] ^= 0xff
	if err := artifact.VerifyManifestDigest(id, b, "sha256", artifactfx.Hashes()); !errors.Is(err, errs.ErrCorruptedManifest) {
		t.Fatalf("want ErrCorruptedManifest, got %v", err)
	}
}

// --- Sealed: round-trip (full-field) + sys stays plaintext ---

func TestSealed_RoundTrip(t *testing.T) {
	m := artifactfx.Manifest()
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoSealed)
	got, err := artifact.DecodeEncrypted(b, artifactfx.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if string(got.PrimaryBlobRef()) != string(m.PrimaryBlobRef()) {
		t.Errorf("BlobRef lost: got %q", got.PrimaryBlobRef())
	}
	if !bytes.Equal(got.Ext, m.Ext) {
		t.Errorf("Ext lost: got %s", got.Ext)
	}
	if !bytes.Equal(got.Usr, m.Usr) {
		t.Errorf("Usr lost: got %s", got.Usr)
	}
	if !bytes.Equal(got.InlineBlob, m.InlineBlob) {
		t.Errorf("InlineBlob lost: got %q", got.InlineBlob)
	}
}

func TestSealed_SysFieldsArePlaintextOnDisk(t *testing.T) {
	m := artifactfx.Manifest()
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoSealed)
	if !bytes.Contains(b, []byte("sess-1")) {
		t.Error("Sealed should leave sys (session id) in plaintext on disk")
	}
	if bytes.Contains(b, []byte("inline-secret-bytes")) {
		t.Error("Sealed leaked inline_blob plaintext on disk")
	}
}

// TestSealed_EmptyExtAndUsr: empty ext/usr are omitted (not sealed empty) and
// come back empty, not as a decryption error.
func TestSealed_EmptyExtAndUsr(t *testing.T) {
	m := artifactfx.Manifest(func(m *domain.Manifest) {
		m.Ext = nil
		m.Usr = nil
	})
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoSealed)
	got, err := artifact.DecodeEncrypted(b, artifactfx.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ext) != 0 {
		t.Errorf("expected empty Ext, got %s", got.Ext)
	}
	if len(got.Usr) != 0 {
		t.Errorf("expected empty Usr, got %s", got.Usr)
	}
}

// --- Paranoid: round-trip (full-field) + everything hidden ---

func TestParanoid_RoundTrip(t *testing.T) {
	m := artifactfx.Manifest()
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoParanoid)
	got, err := artifact.DecodeEncrypted(b, artifactfx.Keys())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Ext, m.Ext) {
		t.Errorf("Ext lost: got %s", got.Ext)
	}
	if !bytes.Equal(got.Usr, m.Usr) {
		t.Errorf("Usr lost: got %s", got.Usr)
	}
	if !bytes.Equal(got.InlineBlob, m.InlineBlob) {
		t.Errorf("InlineBlob lost: got %q", got.InlineBlob)
	}
}

func TestParanoid_HidesSysOnDisk(t *testing.T) {
	m := artifactfx.Manifest()
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoParanoid)
	if bytes.Contains(b, []byte("sess-1")) {
		t.Error("Paranoid should encrypt the whole body including sys (session id)")
	}
}

// TestParanoid_HidesExtAndUsr: Paranoid encrypts ext and usr too, not just sys.
func TestParanoid_HidesExtAndUsr(t *testing.T) {
	m := artifactfx.Manifest(func(m *domain.Manifest) {
		m.Ext = json.RawMessage(`{"m":"EXTMARKER_7f3a"}`)
		m.Usr = json.RawMessage(`{"m":"USRMARKER_91b2"}`)
	})
	_, b := artifactfx.Encoded(t, m, config.ManifestCryptoParanoid)
	if bytes.Contains(b, []byte("EXTMARKER_7f3a")) {
		t.Error("Paranoid leaked ext plaintext on disk")
	}
	if bytes.Contains(b, []byte("USRMARKER_91b2")) {
		t.Error("Paranoid leaked usr plaintext on disk")
	}
}

// --- DecodeEncrypted: failure classes ---

func TestDecodeEncrypted_WrongKeyFails(t *testing.T) {
	_, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoSealed)
	wrong := bytes.Repeat([]byte{0x99}, 32)
	if _, err := artifact.DecodeEncrypted(b, artifactfx.Keys(wrong)); !errors.Is(err, errs.ErrDecryptionFailed) {
		t.Fatalf("want ErrDecryptionFailed, got %v", err)
	}
}

func TestDecodeEncrypted_NoResolverOnEncrypted(t *testing.T) {
	_, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoParanoid)
	if _, err := artifact.DecodeEncrypted(b, nil); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound, got %v", err)
	}
}

func TestDecodeEncrypted_EmptyCandidates(t *testing.T) {
	_, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoSealed)
	if _, err := artifact.DecodeEncrypted(b, emptyKeys{}); !errors.Is(err, errs.ErrKeyNotFound) {
		t.Fatalf("want ErrKeyNotFound, got %v", err)
	}
}

func TestDecodeEncrypted_PlainForwards(t *testing.T) {
	_, b := artifactfx.Encoded(t, artifactfx.Manifest(), config.ManifestCryptoPlain)
	if _, err := artifact.DecodeEncrypted(b, nil); err != nil {
		t.Fatalf("Plain via DecodeEncrypted should succeed: %v", err)
	}
}

// --- Decode guards ---

func TestDecode_ManifestTooLarge(t *testing.T) {
	oversized := make([]byte, domain.MaxManifestSize+1)
	if _, err := artifact.Decode(oversized); !errors.Is(err, errs.ErrManifestTooLarge) {
		t.Fatalf("Decode: got %v, want errs.ErrManifestTooLarge", err)
	}
	if _, err := artifact.DecodeEncrypted(oversized, nil); !errors.Is(err, errs.ErrManifestTooLarge) {
		t.Fatalf("DecodeEncrypted: got %v, want errs.ErrManifestTooLarge", err)
	}
}

func TestEncode_TooManyRefs(t *testing.T) {
	refs := make([]domain.BlobRef, domain.MaxBlobRefs+1)
	for i := range refs {
		refs[i] = domain.BlobRef("aabbccdd")
	}
	m := artifactfx.Manifest(func(m *domain.Manifest) { m.BlobRefs = refs })

	_, err := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if !errors.Is(err, errs.ErrTooManyRefs) {
		t.Fatalf("Encode: got %v, want errs.ErrTooManyRefs", err)
	}
}

// --- Pipeline: multi-stage round-trip incl. IV + KeyID co-presence ---
//
// A non-crypto stage must omit iv/key_id; a crypto stage must carry both, and
// the IV must survive base64 round-trip.

func TestEncodeDecode_PipelineMultiStageRoundTrip(t *testing.T) {
	m := artifactfx.Manifest(func(m *domain.Manifest) {
		m.Pipeline = []domain.PipelineStage{
			{Algorithm: "zstd", Hash: "cccc"},
			{Algorithm: "aes-gcm", Hash: "dddd", IV: []byte{1, 2, 3, 4}, KeyID: "tenant-42"},
		}
	})
	b, err := artifact.Encode(m, config.ManifestEncodingJSON, config.ManifestCryptoPlain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := artifact.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Pipeline) != 2 {
		t.Fatalf("expected 2 pipeline stages, got %d", len(got.Pipeline))
	}
	if got.Pipeline[0].Algorithm != "zstd" {
		t.Errorf("stage 0 algorithm: got %q", got.Pipeline[0].Algorithm)
	}
	if len(got.Pipeline[0].IV) != 0 || got.Pipeline[0].KeyID != "" {
		t.Errorf("non-crypto stage should omit iv/key_id, got iv=%x key_id=%q", got.Pipeline[0].IV, got.Pipeline[0].KeyID)
	}
	if got.Pipeline[1].Algorithm != "aes-gcm" {
		t.Errorf("stage 1 algorithm: got %q", got.Pipeline[1].Algorithm)
	}
	if !bytes.Equal(got.Pipeline[1].IV, []byte{1, 2, 3, 4}) {
		t.Errorf("stage 1 IV did not survive: got %x", got.Pipeline[1].IV)
	}
	if got.Pipeline[1].KeyID != "tenant-42" {
		t.Errorf("stage 1 KeyID did not survive: got %q", got.Pipeline[1].KeyID)
	}
}

// emptyKeys resolves to zero candidates, exercising the ErrKeyNotFound
// branch that artifactfx.Keys (which always returns at least one) cannot.
type emptyKeys struct{}

func (emptyKeys) GetKeys(string) ([][]byte, error) { return nil, nil }
