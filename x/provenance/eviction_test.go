package provenance

import (
	"context"
	"errors"
	"strings"
	"testing"

	"scrinium.dev/domain"
)

// --- index side ---

func TestIndex_ReceiptLookup(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	src, text, rid := aid("a"), aid("b"), aid("r")

	indexAll(t, idx, sub,
		record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(src), Rel: "text"}),
	)
	if has, _ := idx.HasReceipt(ctx, src); has {
		t.Fatal("receipt reported before any eviction")
	}

	indexAll(t, idx, sub, receiptManifest(rid, src, Config{}))

	got, has, err := idx.Receipt(ctx, src)
	if err != nil || !has || got != rid {
		t.Fatalf("Receipt = (%s, %v, %v), want %s", got, has, err, rid)
	}
	// The receipt is recognisable as such — the guard needs that.
	if is, err := idx.IsReceipt(ctx, rid); err != nil || !is {
		t.Fatalf("IsReceipt(receipt) = (%v, %v)", is, err)
	}
	if is, _ := idx.IsReceipt(ctx, text); is {
		t.Error("an ordinary derivative was taken for a receipt")
	}
	// An eviction edge is not a supersede edge: currency must not follow it.
	if head, err := idx.Head(ctx, src); err != nil || head != src {
		t.Fatalf("eviction leaked into the currency chain: (%s, %v)", head, err)
	}
}

// An evicted artifact keeps the rows where it is a parent, so the hole query has
// to skip it — otherwise a stage is offered work on bytes that are gone.
func TestIndex_HolesSkipEvicted(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	bookA, bookB := aid("a"), aid("b")

	indexAll(t, idx, sub,
		record(aid("c"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookA), Rel: "text"}),
		record(aid("d"), "ocr", OutcomeOK, Input{Ref: domain.HandleRef(bookB), Rel: "text"}),
	)

	count := func() int {
		n := 0
		if err := idx.Holes(ctx, HoleQuery{Has: "text", Missing: "thumb"}, func(domain.ArtifactID) error {
			n++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count() != 2 {
		t.Fatalf("setup: both books should owe a thumbnail")
	}

	indexAll(t, idx, sub, receiptManifest(aid("r"), bookA, Config{}))
	if got := count(); got != 1 {
		t.Fatalf("holes = %d, want only the un-evicted book", got)
	}
}

// Effective reproducibility: the declared flag is not enough once a source is
// gone, and a chain stays recoverable while some living ancestor can regenerate
// it (ADR-113 П-11).
func TestIndex_CleanableFollowsAvailability(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	pdf, text, chunk := aid("a"), aid("b"), aid("c")

	indexAll(t, idx, sub,
		record(text, "ocr", OutcomeOK, Input{Ref: domain.HandleRef(pdf), Rel: "text"}),
		record(chunk, "chunk", OutcomeOK, Input{Ref: domain.HandleRef(text), Rel: "chunk"}),
	)

	aliveSet := func(ids ...domain.ArtifactID) func(context.Context, domain.ArtifactID) (bool, error) {
		set := map[domain.ArtifactID]bool{}
		for _, v := range ids {
			set[v] = true
		}
		return func(_ context.Context, v domain.ArtifactID) (bool, error) { return set[v], nil }
	}

	// Everything alive: the text is a cache, its source is on disk.
	if ok, err := idx.Cleanable(ctx, text, aliveSet(pdf, text, chunk)); err != nil || !ok {
		t.Fatalf("text should be cleanable while the pdf lives: (%v, %v)", ok, err)
	}
	// Chunks are cleanable too — their input is alive.
	if ok, err := idx.Cleanable(ctx, chunk, aliveSet(pdf, text, chunk)); err != nil || !ok {
		t.Fatalf("chunk should be cleanable: (%v, %v)", ok, err)
	}

	// The pdf is evicted: the text became data, even though its flag still says
	// reproducible. This is the mistake the whole rule exists to prevent.
	if ok, err := idx.Cleanable(ctx, text, aliveSet(text, chunk)); err != nil || ok {
		t.Fatalf("text must not be cleanable once its source is gone: (%v, %v)", ok, err)
	}
	// But the chunk is still cleanable: the text is alive and can rebuild it.
	if ok, err := idx.Cleanable(ctx, chunk, aliveSet(text, chunk)); err != nil || !ok {
		t.Fatalf("chunk should stay cleanable while the text lives: (%v, %v)", ok, err)
	}
	// Both source and text gone: nothing upstream can rebuild the chunk.
	if ok, err := idx.Cleanable(ctx, chunk, aliveSet(chunk)); err != nil || ok {
		t.Fatalf("chunk must not be cleanable with the whole chain gone: (%v, %v)", ok, err)
	}

	// An origin has no record and is never a cache.
	if ok, err := idx.Cleanable(ctx, pdf, aliveSet(pdf)); err != nil || ok {
		t.Fatalf("an origin reported as cleanable: (%v, %v)", ok, err)
	}
	if _, err := idx.Cleanable(ctx, chunk, nil); err == nil {
		t.Error("Cleanable without an existence probe should refuse")
	}
}

// A non-reproducible derivative is never a cache, however alive its inputs are.
func TestIndex_CleanableRefusesNonRepro(t *testing.T) {
	idx, sub := newIndex(t, Config{})
	ctx := t.Context()
	src, judgement := aid("a"), aid("b")

	indexAll(t, idx, sub, production(judgement, "review", OutcomeOK, false,
		Input{Ref: domain.HandleRef(src), Rel: "note"}))

	alive := func(context.Context, domain.ArtifactID) (bool, error) { return true, nil }
	if ok, err := idx.Cleanable(ctx, judgement, alive); err != nil || ok {
		t.Fatalf("a human judgement reported as cleanable: (%v, %v)", ok, err)
	}
}

// --- guard side ---

func TestGuard_ReceiptUnlocksDeleteAndProtectsItself(t *testing.T) {
	ctx := t.Context()
	src, rid := domain.ArtifactID("source"), domain.ArtifactID("receipt")

	lookup := &evictLookup{
		children:  map[domain.ArtifactID]bool{src: true},
		receipts:  map[domain.ArtifactID]bool{},
		isReceipt: map[domain.ArtifactID]bool{rid: true},
	}
	inner := &fakeDataStore{}
	s := wrapStore(t, Config{GuardDeletes: true}, lookup, inner)

	// No receipt: refused, and the message says why.
	err := s.Delete(ctx, src)
	if !errors.Is(err, ErrHasDerivatives) {
		t.Fatalf("want ErrHasDerivatives, got %v", err)
	}
	if !strings.Contains(err.Error(), "receipt") {
		t.Errorf("refusal does not mention the receipt: %v", err)
	}

	// Receipt written: ordinary deletion proceeds.
	lookup.receipts[src] = true
	if err := s.Delete(ctx, src); err != nil {
		t.Fatalf("explained eviction refused: %v", err)
	}
	if len(inner.deleted) != 1 || inner.deleted[0] != src {
		t.Fatalf("inner deletes = %v", inner.deleted)
	}

	// The receipt itself is not deletable — it is the only account left.
	if err := s.Delete(ctx, rid); !errors.Is(err, ErrReceiptProtected) {
		t.Fatalf("want ErrReceiptProtected, got %v", err)
	}
	if len(inner.deleted) != 1 {
		t.Error("a receipt delete reached the inner store")
	}
}

// evictLookup answers the guard's three questions.
type evictLookup struct {
	children  map[domain.ArtifactID]bool
	receipts  map[domain.ArtifactID]bool
	isReceipt map[domain.ArtifactID]bool
}

func (l *evictLookup) HasChildOf(_ context.Context, id domain.ArtifactID, _ string) (bool, error) {
	return l.children[id], nil
}
func (l *evictLookup) HasReceipt(_ context.Context, id domain.ArtifactID) (bool, error) {
	return l.receipts[id], nil
}
func (l *evictLookup) IsReceipt(_ context.Context, id domain.ArtifactID) (bool, error) {
	return l.isReceipt[id], nil
}

var _ derivativeLookup = (*evictLookup)(nil)

// --- receipt document ---

func TestReceiptSpec_RequiresReasonAndDecider(t *testing.T) {
	for _, spec := range []ReceiptSpec{
		{DecidedBy: "roman"},
		{Reason: "ocr-complete"},
	} {
		if err := spec.validate(); !errors.Is(err, ErrBadReceipt) {
			t.Errorf("spec %+v accepted: %v", spec, err)
		}
	}
	if err := (ReceiptSpec{Reason: "ocr-complete", DecidedBy: "roman"}).validate(); err != nil {
		t.Errorf("valid spec refused: %v", err)
	}
}

func TestReceipt_RoundTripAndVersioning(t *testing.T) {
	m := domain.Manifest{
		ArtifactID:   aid("a"),
		ContentHash:  "abc123",
		OriginalSize: 148921344,
		Ext:          []byte(`{"vfsmeta":{"path":"books/foo.pdf","mime":"application/pdf"}}`),
	}
	doc, err := receiptFor(ReceiptSpec{
		Retained:  []domain.ArtifactID{aid("b")},
		Reason:    "ocr-complete",
		Rule:      "pdf over 100MB",
		DecidedBy: "roman",
	}, m).encode()
	if err != nil {
		t.Fatal(err)
	}

	r, err := DecodeReceipt(doc)
	if err != nil {
		t.Fatalf("DecodeReceipt: %v", err)
	}
	if r.Evicted.Artifact != m.ArtifactID || r.Evicted.Size != m.OriginalSize {
		t.Errorf("evicted description lost: %+v", r.Evicted)
	}
	// The path and MIME are lifted from the filesystem schema for a human reader.
	if r.Evicted.Path != "books/foo.pdf" || r.Evicted.MIME != "application/pdf" {
		t.Errorf("path/mime not carried: %+v", r.Evicted)
	}
	if r.DecidedBy != "roman" {
		t.Errorf("attribution lost: %+v", r)
	}
	if len(r.Retained) != 1 {
		t.Errorf("retained list lost: %+v", r.Retained)
	}

	// An unreadable explanation is not an explanation.
	if _, err := DecodeReceipt([]byte(`{"v":99}`)); !errors.Is(err, ErrBadReceipt) {
		t.Errorf("future version accepted: %v", err)
	}
	if _, err := DecodeReceipt([]byte(`{"v":1}`)); !errors.Is(err, ErrBadReceipt) {
		t.Errorf("receipt naming no artifact accepted: %v", err)
	}
	if _, err := DecodeReceipt([]byte(`not json`)); !errors.Is(err, ErrBadReceipt) {
		t.Errorf("garbage accepted: %v", err)
	}
}

// An artifact with no path carries none — that is not an error.
func TestReceipt_PathIsOptional(t *testing.T) {
	doc, err := receiptFor(ReceiptSpec{Reason: "r", DecidedBy: "d"},
		domain.Manifest{ArtifactID: aid("a")}).encode()
	if err != nil {
		t.Fatal(err)
	}
	r, err := DecodeReceipt(doc)
	if err != nil {
		t.Fatal(err)
	}
	if r.Evicted.Path != "" || r.Evicted.MIME != "" {
		t.Errorf("invented a path: %+v", r.Evicted)
	}
}
