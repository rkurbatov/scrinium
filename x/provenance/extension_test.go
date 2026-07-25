package provenance

import (
	"testing"

	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/extension"
)

// The unit is three parts of one whole (ADR-112 П-3): the index, the behavioral
// wrapper, and the presenter of its Ext schema. It occupies no background axis
// and needs no late-bound environment.
func TestExtension_OccupiesTheRightAxes(t *testing.T) {
	idx := NewIndex(Config{GuardDeletes: true})
	ext := ExtensionFor(idx)

	if got := ext.Descriptor().Name; got != extensionName {
		t.Errorf("descriptor name = %q, want %q", got, extensionName)
	}

	ci, ok := ext.CustomIndex()
	if !ok {
		t.Fatal("extension occupies no index axis")
	}
	// The SAME instance, so a host that keeps a reference to ask it questions is
	// asking the index that is actually installed.
	if ci != customindex.CustomIndex(idx) {
		t.Error("extension installed a different index instance")
	}
	f, ok := ext.Wrapper()
	if !ok {
		t.Fatal("extension occupies no behavior axis")
	}
	if d := f.Descriptor(); d.Name != extensionName || d.Class != wrapper.Behavioral {
		t.Errorf("wrapper descriptor = %+v", d)
	}

	if agents := ext.Agents(); len(agents) != 0 {
		t.Errorf("extension brought agents: %+v", agents)
	}
	if _, isReceiver := ext.(extension.Receiver); isReceiver {
		t.Error("extension asks for a late-bound environment it does not need")
	}
}

// New builds both halves from one configuration, so the guard is armed with an
// index that shares it: a wrapper asking for the guard without an index cannot
// assemble, and here it must.
func TestExtension_NewArmsTheGuard(t *testing.T) {
	ext := New(Config{GuardDeletes: true})
	f, ok := ext.Wrapper()
	if !ok {
		t.Fatal("no behavior axis")
	}
	if _, err := f.Wrap(&fakeDataStore{}, wrapper.Deps{}); err != nil {
		t.Fatalf("guarded wrapper failed to assemble: %v", err)
	}
}

// The configuration lives in one place. An index built with a custom eviction
// kind must hand that same kind to the wrapper it is wrapped into — otherwise the
// writing and reading sides could disagree about which edge means what.
func TestExtension_ConfigComesFromTheIndex(t *testing.T) {
	idx := NewIndex(Config{EvictRel: "kicked-out", SupersedeRel: "replaces"})
	ext := ExtensionFor(idx)
	f, _ := ext.Wrapper()

	fac, ok := f.(factory)
	if !ok {
		t.Fatalf("unexpected factory type %T", f)
	}
	if fac.cfg.evictRel() != "kicked-out" || fac.cfg.supersedeRel() != "replaces" {
		t.Errorf("wrapper got a different configuration: %+v", fac.cfg)
	}
}
