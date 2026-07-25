package provenance

import (
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/wrapper"
	"scrinium.dev/extension"
)

// New returns the provenance extension as one whole: the index that keeps the
// graph and answers questions about it, the behavioral wrapper that stamps
// production records on the way in and guards deletion, and the presenter of its
// own Ext schema. It occupies no background axis and needs no late-bound
// environment — every row it keeps is derived from manifests, so it has no
// durable state of its own to place anywhere.
func New(cfg Config) extension.Extension { return ExtensionFor(NewIndex(cfg)) }

// ExtensionFor wraps an existing index as the extension, so a host that keeps its
// own reference to the index — to ask it questions directly — installs the same
// instance rather than a second one. The configuration comes from the index: one
// place, so the writing and reading sides cannot disagree about which relation
// kind means what.
func ExtensionFor(idx *Index) extension.Extension { return provExtension{idx: idx} }

type provExtension struct{ idx *Index }

func (e provExtension) Descriptor() extension.Descriptor {
	return extension.Descriptor{Name: extensionName}
}

func (e provExtension) CustomIndex() (customindex.CustomIndex, bool) { return e.idx, true }

// Wrapper hands over the behavioral half, with the index bound as the guard's
// lookup. The guard is armed only when the configuration asks for it; otherwise
// the wrapper stamps and nothing else.
func (e provExtension) Wrapper() (wrapper.Factory, bool) {
	return factory{cfg: e.idx.cfg, lookup: e.idx}, true
}

func (e provExtension) Agents() []extension.Agent { return nil }

var _ extension.Extension = provExtension{}
