package namespace

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"scrinium.dev/domain"
	"scrinium.dev/domain/extpocket"
	"scrinium.dev/engine/customindex"
)

const (
	// indexName is the custom-index identifier and the proj_ext ext_name
	// the nsid lands under: Walk(ns) reads ext_name="scrinium.namespace",
	// field="nsid" (09 §9.2, ADR-79).
	indexName = "scrinium.namespace"

	// nsidField is the projected field name in proj_ext and the Ext JSON
	// key the nsid is stamped under on Put.
	nsidField = "nsid"

	// indexSchemaVersion is the projection layout version. The index keeps
	// no own tables (it projects into the core proj_ext store), so a bump
	// here only ever signals a change in WHAT it projects.
	indexSchemaVersion = 1

	// byNamespaceView is the RootView this extension backs (ADR-98): the
	// by-namespace tree, keyed by nsid and labelled with human names.
	byNamespaceView = "by-namespace"
)

// Index is the namespace custom index (ADR-79/88; 09 §9.2). It occupies
// the Indexer (write-side) capability and the ViewProvider capability: on
// each Put it projects the artifact's nsid (read from Manifest.Ext) into
// the core-maintained proj_ext equality store under ext_name="scrinium.namespace",
// field="nsid"; and it backs the by-namespace projection view (ADR-98),
// keyed by nsid with human-name labels from the registry. It keeps NO own
// tables and exposes no Accessor — Walk(ns) is the core's proj_ext equality
// query on the resolved nsid, not an own-tree lookup.
type Index struct {
	// reg backs the by-namespace view's nsid→name Label. nil ⇒ the view
	// labels each node with the verbatim nsid (the Indexer path never uses
	// it, so a registry-less Index is still a valid write-side index).
	reg *Registry
}

// NewIndex returns a fresh namespace index. reg is used only by the
// by-namespace view's Label (pass nil for a write-only index). Register it
// via *sqlite.Index.CustomIndexes().Register, or install it as part of the
// namespace Extension.
func NewIndex(reg *Registry) *Index { return &Index{reg: reg} }

// Name returns the stable identifier; it is also the proj_ext ext_name.
func (e *Index) Name() string { return indexName }

// SchemaVersion returns the projection layout version.
func (e *Index) SchemaVersion() int { return indexSchemaVersion }

// Setup runs once per registration. The index keeps no own tables, so
// there is nothing to create or migrate; it only rejects an unknown
// stored version.
func (e *Index) Setup(ctx context.Context, sub customindex.Substrate, oldVersion int) error {
	switch oldVersion {
	case 0, 1:
		return nil
	default:
		return fmt.Errorf("namespace index: unsupported old schema version: %d", oldVersion)
	}
}

// Close releases index-side resources. The index holds none.
func (e *Index) Close() error { return nil }

// --- Indexer (write-side, ADR-78/88; 09 §9.2) ---

// Index projects the artifact's nsid into the core proj_ext store. It
// reads the "nsid" key from Manifest.Ext; when present and non-empty it
// returns a single PocketExt projection (field "nsid"), which the core
// stamps with the manifest digest and ext_name="scrinium.namespace". A manifest
// with no nsid (most artifacts, system artifacts, nil Ext) is skipped —
// it simply belongs to no namespace. It writes nothing to its own store.
func (e *Index) Index(ctx context.Context, sub customindex.Substrate, m domain.Manifest) ([]customindex.Projection, error) {
	id, ok, err := nsidOf(m.Ext)
	if err != nil {
		return nil, fmt.Errorf("namespace index: decode ext for %q: %w", m.ArtifactID, err)
	}
	if !ok {
		return nil, nil // no namespace stamp — not our concern
	}
	return []customindex.Projection{{
		Pocket: customindex.PocketExt,
		Field:  nsidField,
		Value:  string(id),
	}}, nil
}

// Unindex is a no-op: the index keeps no own tables, and the core removes
// a manifest's proj_ext rows by digest on delete (09 §9.2). It exists to
// satisfy the symmetric Indexer contract and stays idempotent.
func (e *Index) Unindex(ctx context.Context, sub customindex.Substrate, m domain.Manifest) error {
	return nil
}

// --- ViewProvider (read-side view contribution, ADR-98) ---

// ProvidedViews backs the by-namespace projection view (ADR-98), mirroring
// how fspath backs by-path. The tree is keyed by nsid — stable across a
// rename — and Label maps each nsid to its current human name via the
// registry, so renaming a namespace re-labels the directory without moving
// any artifact. Resolve reads the nsid from a manifest's Ext during
// backfill (the full manifest is available there, unlike the index-only
// Walk row). No Metadata source: the index keeps no own ext blocks (it
// projects into the shared proj_ext), so the backfill reads the manifest
// via the store.
//
// The registry is snapshotted once here (at view-build time); a rename
// after the view is built re-labels on the next rebuild, not live —
// live relabelling needs the system-artifact version event still deferred
// (see the System-Artifact-Events rationale). A label miss falls back to
// the verbatim nsid rather than dropping the artifact.
func (e *Index) ProvidedViews() []customindex.ProvidedView {
	// label maps an nsid to its current registry name; a miss falls back
	// to the verbatim nsid. Snapshotted once here (at view-build time).
	label := func(nsid string) (string, bool) { return "", false }
	if e.reg != nil {
		if view, err := e.reg.Load(context.Background()); err == nil {
			label = func(key string) (string, bool) {
				return view.Name(NamespaceID(key))
			}
		}
	}
	// segment picks the directory a manifest occupies: the registry name,
	// else the verbatim nsid, else "_default" (manifest carries no nsid).
	segment := func(m domain.Manifest) string {
		nsid, ok := nsidKey(m)
		if !ok {
			return "_default"
		}
		if name, lok := label(nsid); lok {
			return name
		}
		return nsid
	}
	return []customindex.ProvidedView{{
		Root: byNamespaceView,
		Path: func(m domain.Manifest) (string, bool) {
			return namespaceTreePath(segment(m), string(m.ArtifactID)), true
		},
		CountKey: func(m domain.Manifest) (string, bool) {
			nsid, ok := nsidKey(m)
			if !ok {
				return "", false
			}
			if name, lok := label(nsid); lok {
				return name, true
			}
			return nsid, true
		},
	}}
}

// namespaceTreePath lays out the by-namespace tree:
// <segment>/<aa>/<bb>/<id>, id-sharded so a namespace's artifacts fan out
// under its directory and listings stay bounded. Ids whose hash part is
// shorter than four chars bucket under <segment>/_short/<id>. This layout
// belongs to the namespace extension (ADR-89): the projection only stores
// whatever path this returns.
func namespaceTreePath(segment, id string) string {
	hash := id
	if i := strings.IndexByte(id, '-'); i >= 0 {
		hash = id[i+1:]
	}
	if len(hash) < 4 {
		return segment + "/_short/" + id
	}
	return segment + "/" + hash[:2] + "/" + hash[2:4] + "/" + id
}

// nsidKey is the by-namespace view's Resolve: the artifact's nsid is its
// key. A manifest with no nsid stamp belongs to no namespace (key absent).
func nsidKey(m domain.Manifest) (string, bool) {
	id, ok, err := nsidOf(m.Ext)
	if err != nil || !ok {
		return "", false
	}
	return string(id), true
}

// nsidOf extracts the namespace stamp from an artifact's Ext. The nsid is
// one key in the shared Ext JSON object (other extensions keep their own
// keys alongside it); an absent or empty "nsid" means "no namespace".
func nsidOf(ext json.RawMessage) (NamespaceID, bool, error) {
	raw, ok, err := extpocket.Get(ext, nsidField)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	var id NamespaceID
	if err := json.Unmarshal(raw, &id); err != nil {
		return "", false, err
	}
	if id == "" {
		return "", false, nil
	}
	return id, true, nil
}

// Compile-time conformance: the namespace index is a CustomIndex that
// occupies the Indexer (write) and ViewProvider (by-namespace) capabilities
// and exposes no Accessor.
var (
	_ customindex.CustomIndex  = (*Index)(nil)
	_ customindex.Indexer      = (*Index)(nil)
	_ customindex.ViewProvider = (*Index)(nil)
)
