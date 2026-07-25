package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"scrinium.dev/engine/systemstore"
	"scrinium.dev/errs"
)

// systemNamedArtifact is an alias so the narrow cursorStore port in
// ingester.go can be declared without importing the systemstore package into
// its signature list twice.
type systemNamedArtifact = systemstore.NamedArtifact

// cursorName is the watermark's name inside the extension's scoped
// SystemStore. The scope prefix is added by the store, never by us.
const cursorName = "ingester.watermark"

// cursorOverlap is how far back a sweep starts relative to the stored
// watermark. Modification time has one-second resolution on many filesystems,
// so a write that happened in the same second as the mark would otherwise be
// missed entirely. Overlapping costs a few repeated elements, which the
// already-captured probe filters out anyway.
const cursorOverlap = 2 * time.Second

// watermark is the sweep cursor: the modification time up to which the source
// has been seen, per source path.
//
// It answers "what to look at", not "what has been captured". Correctness rests
// on the already-captured probe (ADR-115 П-4), so losing the watermark costs a
// slow full sweep and nothing else — which is why it is allowed to live in a
// single overwritten cell rather than a versioned history.
type watermark struct {
	store cursorStore
}

func newWatermark(s cursorStore) *watermark { return &watermark{store: s} }

// cursorDoc is the stored form. Keyed by source path so one extension scope can
// serve several ingesters — one instance is one source, but they share the
// scope.
type cursorDoc struct {
	V       int               `json:"v"`
	Sources map[string]string `json:"sources"` // source path → RFC3339 UTC
}

const cursorVersion = 1

// since reports where the next sweep should start for this source: the stored
// mark minus the overlap, or the zero time when nothing is stored (a full
// sweep).
func (w *watermark) since(ctx context.Context, source string) (time.Time, error) {
	doc, err := w.load(ctx)
	if err != nil {
		return time.Time{}, err
	}
	raw, ok := doc.Sources[source]
	if !ok || raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		// An unparseable mark is treated as absent: a full sweep is always
		// safe, and refusing to run because a cursor is corrupt would be the
		// wrong trade.
		return time.Time{}, nil
	}
	return t.Add(-cursorOverlap), nil
}

// advance stores mark for this source, leaving other sources untouched.
func (w *watermark) advance(ctx context.Context, source string, mark time.Time) error {
	doc, err := w.load(ctx)
	if err != nil {
		return err
	}
	if doc.Sources == nil {
		doc.Sources = map[string]string{}
	}
	doc.Sources[source] = mark.UTC().Format(time.RFC3339)

	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("ingester: encode watermark: %w", err)
	}
	// A cell, not a version: history of a cursor has no readers, and the cell
	// form is the one that overwrites in place.
	return w.store.Put(ctx, systemstore.NamedArtifact{
		Name:    cursorName,
		Payload: bytes.NewReader(body),
		Keep:    systemstore.KeepCell(),
	})
}

func (w *watermark) load(ctx context.Context) (cursorDoc, error) {
	rh, err := w.store.Get(ctx, cursorName)
	if err != nil {
		if errors.Is(err, errs.ErrArtifactNotFound) {
			return cursorDoc{V: cursorVersion}, nil
		}
		return cursorDoc{}, fmt.Errorf("ingester: read watermark: %w", err)
	}
	defer func() { _ = rh.Close() }()

	body, err := io.ReadAll(rh)
	if err != nil {
		return cursorDoc{}, fmt.Errorf("ingester: read watermark: %w", err)
	}
	var doc cursorDoc
	if err := json.Unmarshal(body, &doc); err != nil || doc.V != cursorVersion {
		// Same reasoning as an unparseable timestamp: an unreadable cursor
		// degrades to a full sweep instead of stopping the agent.
		return cursorDoc{V: cursorVersion}, nil
	}
	return doc, nil
}
