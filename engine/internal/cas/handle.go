package cas

// handle.go — the domain.ReadHandle implementations returned to store's Get:
//
//   - inlineReadHandle    — in-memory reader over an inline manifest's payload.
//   - targetReadHandle    — lazily opens the physical blob, composes the
//                           inverse Pipeline; holds a *pipeline.Runner
//                           rather than a *store.
//   - verifyingReadHandle — decorator that rehashes plaintext on read and
//                           reports a mismatch as ErrCorruptedBlob. It does
//                           NOT publish events; store publishes
//                           EventScrubFailed when it sees the error.
//
// They satisfy domain.ReadHandle, which lives in domain, so cas
// returns them without importing store (no cycle).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"

	"scrinium.dev/config"
	"scrinium.dev/domain"
	"scrinium.dev/engine/artifact"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/layout"
	"scrinium.dev/engine/pipeline"
	"scrinium.dev/errs"
)

// --- inlineReadHandle: payload is embedded in the manifest ---

type inlineReadHandle struct {
	manifest domain.Manifest
	reader   *bytes.Reader
}

// NewInlineHandle builds a read handle over an inline manifest's payload.
// Used for content-addressed inline blobs and for name-addressed system
// artifacts.
func NewInlineHandle(m domain.Manifest) domain.ReadHandle {
	return &inlineReadHandle{manifest: m, reader: bytes.NewReader(m.InlineBlob)}
}

// NewInlinePayloadHandle builds a read handle that streams payload while
// carrying m as its manifest. It is for the systemstore envelope (ADR-104):
// m.InlineBlob is the envelope JSON (already verify-on-read'd), and payload is
// the unwrapped inline_payload the caller actually wants — so the manifest
// stays the honest on-disk record while the reader exposes the unwrapped
// content.
func NewInlinePayloadHandle(m domain.Manifest, payload []byte) domain.ReadHandle {
	return &inlineReadHandle{manifest: m, reader: bytes.NewReader(payload)}
}

func (h *inlineReadHandle) Read(p []byte) (int, error) { return h.reader.Read(p) }
func (h *inlineReadHandle) ReadAt(p []byte, off int64) (int, error) {
	return h.reader.ReadAt(p, off)
}
func (h *inlineReadHandle) ReadAtCtx(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return h.reader.ReadAt(p, off)
}
func (h *inlineReadHandle) SupportsRandomAccess() bool { return true }
func (h *inlineReadHandle) Close() error               { return nil } // in-memory; idempotent
func (h *inlineReadHandle) Manifest() domain.Manifest  { return h.manifest }

// --- targetReadHandle: blob is a separate file under /blobs/ ---
//
// Opening is deferred to the first Read. ReadAt always opens a fresh range
// descriptor through Driver.ReadAt and closes it before returning — the
// simplest way to layer random access over a streaming Driver.Get without
// sharing state with the linear Read cursor.

type targetReadHandle struct {
	manifest domain.Manifest
	drv      driver.Driver
	runner   *pipeline.Runner
	blobPath string
	// ctx captured at open time; used for non-Ctx Read/ReadAt that have no
	// ambient context per the io contracts. ReadAtCtx uses its own.
	ctx context.Context

	mu     sync.Mutex
	rc     io.ReadCloser // lazily opened on first Read
	closed bool
}

func (h *targetReadHandle) Read(p []byte) (int, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return 0, os.ErrClosed
	}
	if h.rc == nil {
		raw, err := h.drv.Get(h.ctx, h.blobPath)
		if err != nil {
			h.mu.Unlock()
			if errors.Is(err, os.ErrNotExist) {
				return 0, errs.ErrCorruptedBlob
			}
			return 0, err
		}
		decoded, err := h.runner.BuildGet(h.manifest.Pipeline, raw)
		if err != nil {
			h.mu.Unlock()
			return 0, err
		}
		h.rc = decoded
	}
	rc := h.rc
	h.mu.Unlock()
	return rc.Read(p)
}

func (h *targetReadHandle) ReadAt(p []byte, off int64) (int, error) {
	if !h.SupportsRandomAccess() {
		return 0, errs.ErrRandomAccessNotSupported
	}
	return h.readAt(h.ctx, p, off)
}

func (h *targetReadHandle) ReadAtCtx(ctx context.Context, p []byte, off int64) (int, error) {
	if !h.SupportsRandomAccess() {
		return 0, errs.ErrRandomAccessNotSupported
	}
	return h.readAt(ctx, p, off)
}

func (h *targetReadHandle) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return 0, os.ErrClosed
	}
	h.mu.Unlock()

	rc, err := h.drv.ReadAt(ctx, h.blobPath, off, int64(len(p)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, errs.ErrCorruptedBlob
		}
		return 0, err
	}
	defer rc.Close()
	n, readErr := io.ReadFull(rc, p)
	// io.ReaderAt contract: a short read returns a non-nil error, typically
	// io.EOF. ReadFull returns ErrUnexpectedEOF; convert to EOF.
	if errors.Is(readErr, io.ErrUnexpectedEOF) {
		readErr = io.EOF
	}
	return n, readErr
}

func (h *targetReadHandle) SupportsRandomAccess() bool {
	// A non-empty Pipeline transforms the on-disk bytes; ReadAt would have
	// to replay the inverse chain from the start to reach an arbitrary
	// offset. Advertise random access only for verbatim-stored bytes.
	return len(h.manifest.Pipeline) == 0
}

func (h *targetReadHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if h.rc == nil {
		return nil
	}
	err := h.rc.Close()
	h.rc = nil
	return err
}

func (h *targetReadHandle) Manifest() domain.Manifest { return h.manifest }

// OpenHandle builds the layout-appropriate read handle for a manifest:
// an inline handle for LayoutInline, or a lazily-opening target handle for
// LayoutTarget. The returned handle composes the inverse Pipeline on read.
//
// ctx is captured for the deferred open of a Target blob (the io contracts
// give Read/ReadAt no context of their own); ReadAtCtx still uses its own.
func (e *IO) OpenHandle(ctx context.Context, m domain.Manifest) (domain.ReadHandle, error) {
	switch m.LayoutHeader.BlobStorage {
	case domain.LayoutInline:
		return NewInlineHandle(m), nil

	case domain.LayoutTarget:
		addr, err := e.index.Resolve(ctx, string(m.PrimaryBlobRef()))
		if err != nil {
			return nil, fmt.Errorf("cas: resolve blob path: %w", err)
		}
		return &targetReadHandle{
			manifest: m,
			drv:      e.drv,
			runner:   e.runner(),
			blobPath: addr.Path,
			ctx:      ctx,
		}, nil

	default:
		return nil, fmt.Errorf("cas: unknown BlobStorage %q", m.LayoutHeader.BlobStorage)
	}
}

// OpenHandleWithoutIndex is OpenHandle for the one caller that cannot ask the
// index anything: index recovery (ADR-118). Restoring a checkpoint means
// reading a blob whose address would normally come from the index — the very
// thing being rebuilt — so the address is DERIVED from the layout instead.
//
// That derivation is exactly why a checkpoint is never packed: a packed
// member is addressed through the placement map, which lives in the index,
// and the loop would close again.
//
// Not a general fallback: the ordinary read path must keep following where a
// blob was actually written rather than where the current topology would put
// it. This entry point is for bootstrap only.
func (e *IO) OpenHandleWithoutIndex(ctx context.Context, m domain.Manifest, topology config.PathTopology) (domain.ReadHandle, error) {
	switch m.LayoutHeader.BlobStorage {
	case domain.LayoutInline:
		return NewInlineHandle(m), nil

	case domain.LayoutTarget:
		path, err := layout.BlobPath(topology, domain.BlobTypeRegular, string(m.PrimaryBlobRef()))
		if err != nil {
			return nil, fmt.Errorf("cas: derive blob path: %w", err)
		}
		return &targetReadHandle{
			manifest: m,
			drv:      e.drv,
			runner:   e.runner(),
			blobPath: path,
			ctx:      ctx,
		}, nil

	default:
		return nil, fmt.Errorf("cas: unknown BlobStorage %q", m.LayoutHeader.BlobStorage)
	}
}

// --- verifyingReadHandle: rehash plaintext as it streams ---
//
// On a clean EOF (every byte consumed) the running hash is compared to
// manifest.ContentHash; a mismatch surfaces as ErrCorruptedBlob. ReadAt is
// intentionally not verified (a slice read makes a streaming hash
// meaningless). The wrapper does NOT publish events — it returns the error,
// and store publishes EventScrubFailed when it observes ErrCorruptedBlob.

type verifyingReadHandle struct {
	inner domain.ReadHandle

	algo   string
	want   []byte
	hasher hash.Hash
	read   int64
	limit  int64 // declared OriginalSize; -1 if unknown

	// onMismatch is invoked once, the first time finalize detects
	// corruption, with the artifact's ID and the failure error. It lets
	// the store publish EventScrubFailed without cas importing the
	// event bus: the mechanism (rehash) lives here, the consequence
	// (publish) is injected. nil is allowed — the error still propagates
	// through Read; only the side-effect is skipped.
	onMismatch func(domain.ArtifactID, error)

	mu      sync.Mutex
	closed  bool
	eofSeen bool
}

// WrapVerifying decorates inner so that a full streaming Read rehashes the
// plaintext and fails with ErrCorruptedBlob on a ContentHash or length
// mismatch. Returns inner unchanged when the manifest has no ContentHash
// to check. Parses the hash up front so a malformed ContentHash fails here
// (the open boundary), not at first Read.
//
// onMismatch is invoked once when streaming detects corruption (it fires
// inside the caller's Read, after Get has returned, so the store cannot
// observe the error directly — the callback is how EventScrubFailed gets
// published). It may be nil.
func (e *IO) WrapVerifying(inner domain.ReadHandle, onMismatch func(domain.ArtifactID, error)) (domain.ReadHandle, error) {
	m := inner.Manifest()
	if m.ContentHash == "" {
		return inner, nil
	}
	algo := m.HashAlgo
	want, hasher, err := artifact.ParseContentHash(e.hashes, algo, m.ContentHash)
	if err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("cas: wrap verifying: %w", err)
	}
	limit := int64(-1)
	if m.OriginalSize > 0 {
		limit = m.OriginalSize
	}
	return &verifyingReadHandle{inner: inner, algo: algo, want: want, hasher: hasher, limit: limit, onMismatch: onMismatch}, nil
}

func (h *verifyingReadHandle) Read(p []byte) (int, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return 0, os.ErrClosed
	}
	h.mu.Unlock()

	n, err := h.inner.Read(p)
	if n > 0 {
		_, _ = h.hasher.Write(p[:n])
		h.mu.Lock()
		h.read += int64(n)
		h.mu.Unlock()
	}
	if errors.Is(err, io.EOF) {
		if vErr := h.finalize(); vErr != nil {
			return n, vErr
		}
	}
	return n, err
}

// finalize compares the running hash with manifest.ContentHash. Called
// once on the first EOF; later calls are a no-op.
func (h *verifyingReadHandle) finalize() error {
	h.mu.Lock()
	if h.eofSeen {
		h.mu.Unlock()
		return nil
	}
	h.eofSeen = true
	read := h.read
	limit := h.limit
	h.mu.Unlock()

	// Length cross-check: a short stream that hashed to the expected value
	// would still be corrupt. OriginalSize is authoritative when populated.
	if limit >= 0 && read != limit {
		err := fmt.Errorf("%w: read %d bytes, manifest declares %d", errs.ErrCorruptedBlob, read, limit)
		h.reportMismatch(err)
		return err
	}
	if !bytes.Equal(h.hasher.Sum(nil), h.want) {
		err := fmt.Errorf("%w: ContentHash mismatch (algo=%s)", errs.ErrCorruptedBlob, h.algo)
		h.reportMismatch(err)
		return err
	}
	return nil
}

// reportMismatch fires the injected onMismatch callback (if any) with the
// artifact's ID and the corruption error, so the store can publish
// EventScrubFailed. Called from finalize, outside the mutex.
func (h *verifyingReadHandle) reportMismatch(err error) {
	if h.onMismatch != nil {
		h.onMismatch(h.inner.Manifest().ArtifactID, err)
	}
}

func (h *verifyingReadHandle) ReadAt(p []byte, off int64) (int, error) {
	return h.inner.ReadAt(p, off)
}
func (h *verifyingReadHandle) ReadAtCtx(ctx context.Context, p []byte, off int64) (int, error) {
	return h.inner.ReadAtCtx(ctx, p, off)
}
func (h *verifyingReadHandle) SupportsRandomAccess() bool { return h.inner.SupportsRandomAccess() }
func (h *verifyingReadHandle) Manifest() domain.Manifest  { return h.inner.Manifest() }

func (h *verifyingReadHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()
	return h.inner.Close()
}

// Compile-time interface conformance.
var (
	_ domain.ReadHandle = (*inlineReadHandle)(nil)
	_ domain.ReadHandle = (*targetReadHandle)(nil)
	_ domain.ReadHandle = (*verifyingReadHandle)(nil)
)
