package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"syscall"
	"time"

	"golang.org/x/text/unicode/norm"

	"scrinium.dev/domain"
	"scrinium.dev/domain/extpocket"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/engine/driver"
)

// sweep is one pass over the source: enumerate, decide, capture, advance the
// watermark. Everything it does is a consequence of ADR-115, and the shape is
// deliberately linear — a pass has no state of its own beyond the counters.
func (a *ingester) sweep(ctx context.Context) (Stats, error) {
	var stats Stats

	since, err := a.cursor.since(ctx, a.cfg.SourcePath)
	if err != nil {
		return stats, err
	}
	session := a.cfg.Session
	if session == "" {
		session = domain.NewMountSessionID()
	}

	// highWater tracks the newest element actually dealt with. It advances only
	// past elements the pass finished with — a deferred or failed element must
	// not be jumped over, or the next sweep would never see it again.
	var highWater time.Time
	settleCutoff := time.Now().Add(-a.cfg.SettleWindow)

	handle := func(el Element) error {
		stats.Seen++

		switch a.cfg.Policy.Decide(el) {
		case Skip:
			stats.Skipped++
			a.note(&highWater, el.ModTime)
			return nil
		case Defer:
			stats.Deferred++
			return nil
		}

		// Not settled yet: too fresh, or the driver says it is still being
		// written. Postponed, not failed — the next sweep will find it.
		ready, err := a.ready(ctx, el, settleCutoff)
		if err != nil {
			return err
		}
		if !ready {
			stats.Deferred++
			return nil
		}

		known, err := a.alreadyCaptured(el)
		if err != nil {
			return err
		}
		if known {
			stats.Known++
			a.note(&highWater, el.ModTime)
			return nil
		}

		if err := a.capture(ctx, el, session); err != nil {
			// A failed element stays in the source and is reported; the pass
			// continues. Writing failures as artifacts is the pipeline's
			// business, not the capture's.
			stats.Failed++
			a.Logger().Warn("ingest element failed", "path", el.Path, "err", err)
			return nil
		}
		stats.Captured++
		a.note(&highWater, el.ModTime)

		if a.cfg.Disposal == Remove {
			// Only now: the artifact is written and indexed. The reverse order
			// loses data on a crash between the two.
			if err := a.src.Remove(ctx, el.Path); err != nil {
				a.Logger().Warn("source element not removed after capture",
					"path", el.Path, "err", err)
			}
		}
		return nil
	}

	if err := a.enumerate(ctx, since, handle); err != nil {
		return stats, err
	}

	if !highWater.IsZero() {
		if err := a.cursor.advance(ctx, a.cfg.SourcePath, highWater); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// note advances the high-water mark monotonically.
func (a *ingester) note(highWater *time.Time, t time.Time) {
	if t.After(*highWater) {
		*highWater = t
	}
}

// enumerate walks the source through the driver, in the form the configuration
// asks for: with directories when they are being captured, objects only
// otherwise. Either way the agent never touches a filesystem itself, so the
// same pass works over a local folder and over an object store.
func (a *ingester) enumerate(ctx context.Context, since time.Time, cb func(Element) error) error {
	if a.cfg.CaptureDirs {
		lister, ok := a.src.(driver.TreeLister)
		if !ok {
			// New() refuses this combination; reaching here means the driver
			// changed its mind about its own capability.
			return fmt.Errorf("ingester: driver declares CapDirEntries but implements no TreeLister")
		}
		return lister.ListTree(ctx, a.cfg.SourcePath, since, func(e driver.TreeEntry) error {
			kind := KindFile
			if e.IsDir {
				kind = KindDir
			}
			return cb(Element{
				Path:    normalizePath(e.Path),
				Kind:    kind,
				Size:    e.Size,
				ModTime: e.LastModified,
				Mode:    e.Mode,
			})
		})
	}

	return a.src.ListObjectsWithModTime(ctx, a.cfg.SourcePath, since, func(o driver.ObjectMeta) error {
		return cb(Element{
			Path:    normalizePath(o.Path),
			Kind:    KindFile,
			Size:    o.Size,
			ModTime: o.LastModified,
		})
	})
}

// ready decides whether an element may be read now. The settle window is the
// unconditional guard; the driver's probe, when it has one, is a veto on top —
// it can postpone earlier and more precisely, but it cannot release an element
// the window still holds. Advisory locks are advisory, and most writers take
// none, so correctness cannot rest on the probe.
func (a *ingester) ready(ctx context.Context, el Element, settleCutoff time.Time) (bool, error) {
	if el.Kind == KindDir {
		// A directory has no content to catch half-written.
		return true, nil
	}
	if el.ModTime.After(settleCutoff) {
		return false, nil
	}
	if a.probe == nil {
		return true, nil
	}
	ok, err := a.probe.ReadyToRead(ctx, el.Path)
	if err != nil {
		return false, fmt.Errorf("ingester: readiness probe %q: %w", el.Path, err)
	}
	return ok, nil
}

// alreadyCaptured reports whether this path already holds an artifact with this
// modification time.
//
// The probe is mandatory, not an optimisation: identity is unique per Put
// (ADR-73), so a second sweep without it would write second artifacts over the
// same bytes — the blob dedups, the manifests multiply.
//
// Size is not compared: the filesystem schema does not carry it (it lives in the
// manifest), so checking it would mean reading a manifest per already-captured
// element — that is, per element on every re-sweep. A content change that
// preserves the modification time is caught by a full re-sweep, not by making
// the hot path expensive.
func (a *ingester) alreadyCaptured(el Element) (bool, error) {
	ids, err := a.paths.Lookup(el.Path)
	if err != nil {
		return false, fmt.Errorf("ingester: path lookup %q: %w", el.Path, err)
	}
	for _, id := range ids {
		raw, ok, err := a.paths.GetByID(id)
		if err != nil {
			return false, fmt.Errorf("ingester: path metadata %s: %w", id, err)
		}
		if !ok {
			continue
		}
		captured, ok, err := vfsmeta.Decode(raw)
		if err != nil || !ok {
			continue
		}
		if captured.ModTime.Equal(el.ModTime) {
			return true, nil
		}
	}
	return false, nil
}

// capture writes one element as an artifact: the agent's own vfsmeta block plus
// whatever the policy attaches. A directory carries no content — that is the
// whole of what a directory artifact is (ADR-114).
func (a *ingester) capture(ctx context.Context, el Element, session domain.SessionID) error {
	ext, usr, opts, err := a.cfg.Policy.Enrich(el)
	if err != nil {
		return fmt.Errorf("ingester: enrich %q: %w", el.Path, err)
	}

	stamped, err := a.stampPath(ext, el)
	if err != nil {
		return err
	}

	art := domain.Artifact{Ext: stamped, Usr: usr}
	if el.Kind == KindFile {
		body, err := a.src.Get(ctx, el.Path)
		if err != nil {
			return fmt.Errorf("ingester: open %q: %w", el.Path, err)
		}
		defer func() { _ = body.Close() }()
		art.Payload = body
	} else {
		art.Payload = emptyReader{}
	}

	putOpts := append([]domain.PutOption{domain.WithSession(session)}, opts...)
	if _, err := a.target.Put(ctx, art, putOpts...); err != nil {
		return fmt.Errorf("ingester: put %q: %w", el.Path, err)
	}
	return nil
}

// stampPath writes the agent's own half of the metadata into the policy's Ext:
// the normalised path, the mode (type bits included, so a directory is
// recognisable) and the modification time. It merges rather than replaces, so
// the policy's own schemas survive.
func (a *ingester) stampPath(ext json.RawMessage, el Element) (json.RawMessage, error) {
	mode := el.Mode
	if el.Kind == KindDir && mode&syscall.S_IFDIR == 0 {
		// A driver that reports directories without type bits still gets a
		// directory artifact that is recognisable as one.
		mode |= syscall.S_IFDIR
	}
	fsBlock := vfsmeta.FileSystem{
		Path:    el.Path,
		Mode:    mode,
		ModTime: el.ModTime,
	}
	value, err := vfsmeta.Embed(nil, fsBlock)
	if err != nil {
		return nil, fmt.Errorf("ingester: vfsmeta %q: %w", el.Path, err)
	}
	// vfsmeta.Embed returns a whole Ext object; lift its block out and merge it
	// into the policy's, so neither side overwrites the other.
	block, ok, err := extpocket.Get(value, vfsmeta.Key)
	if err != nil || !ok {
		return nil, fmt.Errorf("ingester: vfsmeta %q: unexpected block shape", el.Path)
	}
	merged, err := extpocket.Put(ext, vfsmeta.Key, block)
	if err != nil {
		return nil, fmt.Errorf("ingester: ext %q: %w", el.Path, err)
	}
	return merged, nil
}

// normalizePath brings a path to NFC and slash form. It happens here, at
// capture, because the path becomes the key everything is later found by:
// two spellings of the same letter must collapse before the write, not after.
func normalizePath(p string) string {
	return norm.NFC.String(path.Clean(p))
}

// emptyReader is the payload of a directory artifact: nothing to read.
type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
