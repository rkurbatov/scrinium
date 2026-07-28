package bundler

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
)

// fakeSubstrate is an in-memory customindex.Substrate used to test
// the bundler index-custom index in isolation, without a sqlite backend.
type fakeSubstrate struct {
	data map[string][]byte // key: table + "\x00" + key
}

func newFakeSubstrate() *fakeSubstrate { return &fakeSubstrate{data: map[string][]byte{}} }

var _ customindex.Substrate = (*fakeSubstrate)(nil)

func (f *fakeSubstrate) compositeKey(table, key string) string { return table + "\x00" + key }

func (f *fakeSubstrate) Put(table, key string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	f.data[f.compositeKey(table, key)] = cp
	return nil
}

func (f *fakeSubstrate) Get(table, key string) ([]byte, bool, error) {
	v, ok := f.data[f.compositeKey(table, key)]
	return v, ok, nil
}

func (f *fakeSubstrate) Delete(table, key string) error {
	delete(f.data, f.compositeKey(table, key))
	return nil
}

func (f *fakeSubstrate) DeletePrefix(table, prefix string) error {
	if prefix == "" {
		return customindex.ErrEmptyPrefix
	}
	p := f.compositeKey(table, prefix)
	for k := range f.data {
		if strings.HasPrefix(k, p) {
			delete(f.data, k)
		}
	}
	return nil
}

func (f *fakeSubstrate) Scan(table, prefix string, cb func(key string, value []byte) error) error {
	tablePrefix := table + "\x00"
	keyPrefix := f.compositeKey(table, prefix)
	for k, v := range f.data {
		if !strings.HasPrefix(k, tablePrefix) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(k, keyPrefix) {
			continue
		}
		if err := cb(strings.TrimPrefix(k, tablePrefix), v); err != nil {
			if errors.Is(err, customindex.ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (f *fakeSubstrate) Inc(table, key string, delta int64) (int64, error) {
	var cur int64
	if v, ok := f.data[f.compositeKey(table, key)]; ok && len(v) == 8 {
		cur = int64(binary.BigEndian.Uint64(v))
	}
	cur += delta
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(cur))
	f.data[f.compositeKey(table, key)] = b
	return cur, nil
}

// newTestCustomIndex returns a bundler index-custom index wired to a fresh
// in-memory store (Setup already run, db-mode equivalent).
func newTestCustomIndex(t *testing.T) (*customIndex, *fakeSubstrate) {
	t.Helper()
	e := &customIndex{}
	store := newFakeSubstrate()
	if err := e.Setup(context.Background(), store, 0); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return e, store
}

func TestCustomIndex_RecordResolveDelete(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestCustomIndex(t)

	container := domain.Manifest{BlobRefs: []domain.BlobRef{"fac0b10b0001"}}
	entries := []PackedEntry{
		{
			ArtifactID:     "art-p1",
			BlobRef:        "b10b00f1",
			ManifestOffset: 0,
			ManifestSize:   200,
			BlobOffset:     200,
			BlobSize:       1024,
			PipelineParams: []byte("pp1"),
		},
		{
			ArtifactID:     "art-p2",
			BlobRef:        "b10b00f2",
			ManifestOffset: 1224,
			ManifestSize:   200,
			BlobOffset:     1424,
			BlobSize:       2048,
		},
	}
	if err := e.RecordPack(ctx, container, entries); err != nil {
		t.Fatalf("RecordPack: %v", err)
	}

	// Hit: placement reflects the container's body blob and the slice.
	ov, ok, err := e.ResolvePacked(ctx, "art-p1")
	if err != nil {
		t.Fatalf("ResolvePacked(art-p1): %v", err)
	}
	if !ok {
		t.Fatal("art-p1: expected found")
	}
	if ov.PackBlobRef != "fac0b10b0001" {
		t.Errorf("PackBlobRef: got %q, want pack-blob-1", ov.PackBlobRef)
	}
	if ov.ManifestOffset != 0 || ov.ManifestSize != 200 {
		t.Errorf("manifest slice: got off=%d size=%d, want 0/200", ov.ManifestOffset, ov.ManifestSize)
	}
	if ov.BlobOffset != 200 || ov.BlobSize != 1024 {
		t.Errorf("blob slice: got off=%d size=%d, want 200/1024", ov.BlobOffset, ov.BlobSize)
	}
	if string(ov.PipelineParams) != "pp1" {
		t.Errorf("PipelineParams: got %q, want pp1", ov.PipelineParams)
	}

	// Miss: an unpacked artifact is not owned here.
	if _, ok, err := e.ResolvePacked(ctx, "not-packed"); err != nil || ok {
		t.Errorf("ResolvePacked(not-packed): got ok=%v err=%v, want false/nil", ok, err)
	}

	// DeletePack drops every member of the volume.
	if err := e.DeletePack(ctx, "fac0b10b0001"); err != nil {
		t.Fatalf("DeletePack: %v", err)
	}
	for _, id := range []domain.ArtifactID{"art-p1", "art-p2"} {
		if _, ok, _ := e.ResolvePacked(ctx, id); ok {
			t.Errorf("%s: still resolvable after DeletePack", id)
		}
	}
}

func TestCustomIndex_DeletePackIsVolumeScoped(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestCustomIndex(t)

	if err := e.RecordPack(ctx, domain.Manifest{BlobRefs: []domain.BlobRef{"4010a0"}},
		[]PackedEntry{{ArtifactID: "a1", BlobSize: 1}}); err != nil {
		t.Fatalf("RecordPack vol-A: %v", err)
	}
	if err := e.RecordPack(ctx, domain.Manifest{BlobRefs: []domain.BlobRef{"4010b0"}},
		[]PackedEntry{{ArtifactID: "b1", BlobSize: 1}}); err != nil {
		t.Fatalf("RecordPack vol-B: %v", err)
	}

	if err := e.DeletePack(ctx, "4010a0"); err != nil {
		t.Fatalf("DeletePack vol-A: %v", err)
	}
	if _, ok, _ := e.ResolvePacked(ctx, "a1"); ok {
		t.Error("a1: should be gone after DeletePack(vol-A)")
	}
	if _, ok, _ := e.ResolvePacked(ctx, "b1"); !ok {
		t.Error("b1: should survive DeletePack(vol-A)")
	}
}

// TestCustomIndex_SatisfiesResolver pins the capability contract:
// the value returned by the constructor must be assertable to
// customindex.Resolver (the core overlay probes by assertion, ADR-88).
func TestCustomIndex_SatisfiesResolver(t *testing.T) {
	var ext customindex.CustomIndex = NewCustomIndex()
	if _, ok := ext.(customindex.Resolver); !ok {
		t.Fatal("bundler index-custom index does not satisfy customindex.Resolver")
	}
}

// TestCustomIndex_RecordBeforeSetup guards the store-capture
// precondition.
func TestCustomIndex_RecordBeforeSetup(t *testing.T) {
	e := &customIndex{}
	if err := e.RecordPack(context.Background(), domain.Manifest{BlobRefs: []domain.BlobRef{"x"}}, nil); err == nil {
		t.Fatal("RecordPack before Setup: expected error")
	}
}
