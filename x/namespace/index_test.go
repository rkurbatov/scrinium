package namespace

import (
	"context"
	"encoding/json"
	"testing"

	"scrinium.dev/domain"
)

func TestIndex_ProjectsNSID(t *testing.T) {
	e := NewIndex(nil)
	m := domain.Manifest{
		ArtifactID: "art-1",
		Ext:        json.RawMessage(`{"nsid":"ns-uuid-1"}`),
	}
	projs, err := e.Index(context.Background(), nil, m)
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	if len(projs) != 1 {
		t.Fatalf("projections = %d, want 1", len(projs))
	}
	p := projs[0]
	if p.Field != "nsid" {
		t.Errorf("Field = %q, want nsid", p.Field)
	}
	if p.Value != "ns-uuid-1" {
		t.Errorf("Value = %q, want ns-uuid-1", p.Value)
	}
}

func TestIndex_SkipsWithoutNSID(t *testing.T) {
	e := NewIndex(nil)
	cases := map[string]json.RawMessage{
		"nil ext":      nil,
		"empty object": json.RawMessage(`{}`),
		"foreign ext":  json.RawMessage(`{"vfsmeta":{"version":1,"path":"a"}}`),
		"empty nsid":   json.RawMessage(`{"nsid":""}`),
	}
	for name, ext := range cases {
		t.Run(name, func(t *testing.T) {
			projs, err := e.Index(context.Background(), nil, domain.Manifest{ArtifactID: "x", Ext: ext})
			if err != nil {
				t.Fatalf("Index: %v", err)
			}
			if projs != nil {
				t.Errorf("projections = %v, want nil (no namespace stamp)", projs)
			}
		})
	}
}

func TestIndex_CoexistsWithOtherExtKeys(t *testing.T) {
	e := NewIndex(nil)
	// A manifest carrying both the vfsmeta schema (under its own "vfsmeta"
	// key) and the namespace stamp: the index reads only its own "nsid"
	// key and ignores the rest.
	m := domain.Manifest{
		ArtifactID: "art-2",
		Ext:        json.RawMessage(`{"vfsmeta":{"version":1,"path":"docs/a.txt"},"nsid":"ns-uuid-2"}`),
	}
	projs, err := e.Index(context.Background(), nil, m)
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	if len(projs) != 1 || projs[0].Value != "ns-uuid-2" {
		t.Fatalf("projections = %v, want one nsid=ns-uuid-2", projs)
	}
}

func TestIndex_InvalidExtErrors(t *testing.T) {
	e := NewIndex(nil)
	m := domain.Manifest{ArtifactID: "bad", Ext: json.RawMessage(`{"nsid":123}`)}
	if _, err := e.Index(context.Background(), nil, m); err == nil {
		t.Error("Index with malformed nsid: want error, got nil")
	}
}

func TestIndex_UnindexNoop(t *testing.T) {
	e := NewIndex(nil)
	m := domain.Manifest{ArtifactID: "art-1", Ext: json.RawMessage(`{"nsid":"ns-uuid-1"}`)}
	if err := e.Unindex(context.Background(), nil, m); err != nil {
		t.Errorf("Unindex: %v", err)
	}
}

func TestIndex_Contract(t *testing.T) {
	e := NewIndex(nil)
	if e.Name() != "scrinium.namespace" {
		t.Errorf("Name = %q, want scrinium.namespace", e.Name())
	}
	if err := e.Setup(context.Background(), nil, 0); err != nil {
		t.Errorf("Setup(0): %v", err)
	}
	if err := e.Setup(context.Background(), nil, 99); err == nil {
		t.Error("Setup(99): want unsupported-version error")
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestIndex_ProvidedViews_ByNamespace(t *testing.T) {
	mem := newMemSysStore()
	ctx := context.Background()
	ext, _ := New(mem)
	id, err := ext.Registry().Create(ctx, "docs")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	views := NewIndex(ext.Registry()).ProvidedViews()
	if len(views) != 1 || views[0].Root != "by-namespace" {
		t.Fatalf("ProvidedViews = %+v, want one by-namespace view", views)
	}
	pv := views[0]

	// Path: a stamped manifest lands under its registry label, id-sharded.
	stamped := domain.Manifest{
		ArtifactID: "sha256-aabbccdd",
		Ext:        json.RawMessage(`{"nsid":"` + string(id) + `"}`),
	}
	if path, ok := pv.Path(stamped); !ok || path != "docs/aa/bb/sha256-aabbccdd" {
		t.Errorf("Path(stamped) = (%q,%v), want (docs/aa/bb/sha256-aabbccdd,true)", path, ok)
	}
	// An unstamped manifest still gets placed, under _default.
	unstamped := domain.Manifest{ArtifactID: "sha256-aabbccdd"}
	if path, ok := pv.Path(unstamped); !ok || path != "_default/aa/bb/sha256-aabbccdd" {
		t.Errorf("Path(unstamped) = (%q,%v), want (_default/aa/bb/sha256-aabbccdd,true)", path, ok)
	}

	// CountKey: stamped → the label; unstamped → not counted.
	if key, ok := pv.CountKey(stamped); !ok || key != "docs" {
		t.Errorf("CountKey(stamped) = (%q,%v), want (docs,true)", key, ok)
	}
	if _, ok := pv.CountKey(unstamped); ok {
		t.Error("CountKey(unstamped) = ok, want not ok")
	}
}

func TestIndex_ProvidedViews_NoRegistryLabelsVerbatim(t *testing.T) {
	views := NewIndex(nil).ProvidedViews()
	if len(views) != 1 || views[0].Root != "by-namespace" {
		t.Fatalf("registry-less index must still provide one by-namespace view")
	}
	pv := views[0]

	// No registry ⇒ the verbatim nsid is the segment (no human label).
	nsid := "11111111-2222-3333-4444-555555555555"
	m := domain.Manifest{
		ArtifactID: "sha256-aabbccdd",
		Ext:        json.RawMessage(`{"nsid":"` + nsid + `"}`),
	}
	want := nsid + "/aa/bb/sha256-aabbccdd"
	if path, ok := pv.Path(m); !ok || path != want {
		t.Errorf("Path = (%q,%v), want (%q,true)", path, ok, want)
	}
}
