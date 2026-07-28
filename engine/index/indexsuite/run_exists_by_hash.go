package indexsuite

import (
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/testutil/manifestfx"
)

// --- ExistsByHash ---
//
// ADR-58: ExistsByHash now keys on the full dedup triple
// (ContentHash, OriginalSize, CryptoIdentity), like ExistsByContent.
// A chunk is anonymous in name but not in size or crypto-identity, so
// the conformance mirrors run_exists_by_content: size is strict, and
// crypto-identity splits the key.

func runExistsByHash(t *testing.T, f Factory) {
	t.Run("Hit", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		hash := manifestfx.SyntheticHash('a')
		m := manifestfx.BlobWithHash("art-1", "b10b0001", hash, 1024)
		if err := idx.IndexManifest(ctx, m, manifestfx.PhysAddr("p")); err != nil {
			t.Fatal(err)
		}

		status, err := idx.ExistsByHash(ctx, hash, 1024, "")
		if err != nil {
			t.Fatalf("ExistsByHash: %v", err)
		}
		if status != domain.BlobExists {
			t.Errorf("status: got %d, want BlobExists", status)
		}
	})

	t.Run("Miss", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		status, err := idx.ExistsByHash(ctx, "deadbeef", 1024, "")
		if err != nil {
			t.Fatalf("ExistsByHash: %v", err)
		}
		if status != domain.BlobNotFound {
			t.Errorf("status: got %d, want BlobNotFound", status)
		}
	})

	t.Run("SizeStrict", func(t *testing.T) {
		ctx := t.Context()
		// A chunk's length is known to the chunker, so the composite
		// key is strict on size: same content_hash, different size is
		// a distinct entry, not a match (ADR-58 revises the earlier
		// "ExistsByHash ignores size" behaviour).
		idx := f.New(t)
		hash := manifestfx.SyntheticHash('x')
		if err := idx.IndexManifest(ctx, manifestfx.BlobWithHash("art-1k", "b10b001c", hash, 1024), manifestfx.PhysAddr("p1")); err != nil {
			t.Fatal(err)
		}

		status, err := idx.ExistsByHash(ctx, hash, 2048, "")
		if err != nil {
			t.Fatalf("ExistsByHash: %v", err)
		}
		if status != domain.BlobNotFound {
			t.Errorf("size mismatch must miss: got %d, want BlobNotFound", status)
		}
	})

	t.Run("CryptoIdentitySplitsKey", func(t *testing.T) {
		ctx := t.Context()
		idx := f.New(t)
		hash := manifestfx.SyntheticHash('c')

		// A Plain chunk: empty crypto-identity.
		if err := idx.IndexManifest(ctx, manifestfx.BlobWithHash("art-plain", "b10b71a0", hash, 2048), manifestfx.PhysAddr("p-plain")); err != nil {
			t.Fatal(err)
		}
		// An encrypted chunk of the same plaintext under k1.
		if err := idx.IndexManifest(ctx, manifestfx.EncryptedBlobWithHash("art-enc", "b10b0ec0", hash, 2048, "aes-gcm", "k1"), manifestfx.PhysAddr("p-enc")); err != nil {
			t.Fatal(err)
		}

		// Plain probe matches the Plain chunk only.
		if status, err := idx.ExistsByHash(ctx, hash, 2048, ""); err != nil || status != domain.BlobExists {
			t.Fatalf("plain probe: status=%d err=%v, want BlobExists", status, err)
		}
		// Encrypted probe under k1 matches the encrypted chunk.
		if status, err := idx.ExistsByHash(ctx, hash, 2048, "aes-gcm/k1"); err != nil || status != domain.BlobExists {
			t.Fatalf("k1 probe: status=%d err=%v, want BlobExists", status, err)
		}
		// A different KeyID never matches — distinct ciphertext.
		if status, err := idx.ExistsByHash(ctx, hash, 2048, "aes-gcm/k2"); err != nil || status != domain.BlobNotFound {
			t.Fatalf("k2 probe: status=%d err=%v, want BlobNotFound", status, err)
		}
	})
}
