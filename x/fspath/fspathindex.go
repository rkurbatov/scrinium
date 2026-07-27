package fspath

import (
	"context"
	"encoding/json"
	"fmt"

	"scrinium.dev/domain"
	"scrinium.dev/domain/vfsmeta"
	"scrinium.dev/engine/customindex"
)

// Tables under the custom index namespace. Two K/V groups:
//
//   - byID:   artifactID → vfsmeta JSON (primary, source of truth;
//     the bulk Metadata source the View backfill consults)
//   - byPath: "<path>\x00<artifactID>" → artifactID (the path tree
//     the Accessor scans — KeyLookup for an exact path, PrefixScan
//     for a directory listing / subtree)
//
// The trailing "\x00<artifactID>" suffix in byPath keys lets two
// artifacts share a path (rare but legal — e.g. transient
// duplicates during reindex) without colliding on the same key,
// and keeps same-path entries contiguous in lexicographic order
// (\x00 sorts before any path byte), so the Accessor can coalesce
// them into one callback.
const (
	tableByID   = "byID"
	tableByPath = "byPath"
)

// Name is the stable custom index identifier.
const Name = "scrinium.fspath"

// schemaVersion is the on-disk layout version. Bump and add a
// migration switch in Setup whenever the table layout changes.
const schemaVersion = 1

// CustomIndex is the vfsmeta-aware path index (07 Projection §7.5).
// It implements customindex.CustomIndex plus the Indexer (write-side)
// and Accessor (read-side: KeyLookup + PrefixScan) capabilities, and
// — through viewprovider.go — the ViewProvider capability that backs
// the by-path projection view. Construct via NewIndex, register via
// *sqlite.Index.CustomIndexes().Register.
//
// Population is via the Indexer capability, NOT the Subscribe/Apply
// event path: the core runs Index/Unindex inside the index-write and
// delete transactions (09 §9.2). fspathindex keeps an OWN path tree
// (queried through the Accessor) and projects nothing into the standard
// proj_ext table — "namespace projects nsid; fspathindex
// writes an own path tree" (09 §9.2).
type CustomIndex struct {
	// sub is captured during Setup and used by the read-side
	// API (Metadata, GetByID, LookupByPath, WalkAll, and the
	// Accessor methods) for the lifetime of the StoreIndex. The
	// backend swaps the underlying executor from tx-mode to
	// db-mode atomically after Register commits; the captured
	// reference stays valid throughout.
	sub customindex.Substrate
}

// NewIndex returns a fresh customindex. The instance is not registered
// — caller passes it to *sqlite.Index.CustomIndexes().Register(ctx, ext).
func NewIndex() *CustomIndex {
	return &CustomIndex{}
}

// Name returns the stable identifier.
func (e *CustomIndex) Name() string { return Name }

// SchemaVersion returns the current data layout version.
func (e *CustomIndex) SchemaVersion() int { return schemaVersion }

// Setup runs once per registration. v1 is initial; nothing to
// migrate on a fresh DB or on an existing v1.
func (e *CustomIndex) Setup(ctx context.Context, sub customindex.Substrate, oldVersion int) error {
	switch oldVersion {
	case 0, 1:
		// Nothing to do — schema is implicit (the K/V tables
		// exist by virtue of ext_data being created at
		// migration v2 in the sqlite backend).
	default:
		return fmt.Errorf("fspathindex: unsupported old schema version: %d", oldVersion)
	}
	e.sub = sub
	return nil
}

// Close releases custom index-side resources. Backend storage stays
// owned by the StoreIndex.
func (e *CustomIndex) Close() error {
	e.sub = nil
	return nil
}

// --- Indexer (write-side capability, ADR-78/88; 09 §9.2) ---

// Index persists the vfsmeta payload of m into fspathindex's OWN tables,
// inside the index-write transaction. It writes:
//
//   - byID:   artifactID → raw vfsmeta JSON (verbatim — we don't
//     re-encode the vfsmeta payload, so future schema versions that add fields flow
//     through without an fspathindex migration);
//   - byPath: "<path>\x00<artifactID>" → artifactID (the path tree).
//
// It returns NO standard projections: path lookup is served by the
// Accessor over the own path tree, not by proj_ext equality (09 §9.2).
//
// Manifests that carry no vfsmeta payload (foreign schema, system
// artifacts, nil Ext) are silently skipped — the index only stores what
// it understands. A vfsmeta payload that is present but structurally
// broken returns an error, which rolls the whole write back
// (strict consistency, ADR-49).
func (e *CustomIndex) Index(ctx context.Context, sub customindex.Substrate, m domain.Manifest) ([]customindex.Projection, error) {
	fs, ok, err := vfsmeta.Decode(m.Ext)
	if err != nil {
		return nil, fmt.Errorf("fspathindex: decode ext for %q: %w", m.ArtifactID, err)
	}
	if !ok {
		// Foreign schema or no vfsmeta — not our concern.
		return nil, nil
	}

	id := string(m.ArtifactID)

	if err := sub.Put(tableByID, id, []byte(m.Ext)); err != nil {
		return nil, fmt.Errorf("fspathindex: put byID: %w", err)
	}
	if err := sub.Put(tableByPath, fs.Path+"\x00"+id, []byte(id)); err != nil {
		return nil, fmt.Errorf("fspathindex: put byPath: %w", err)
	}
	return nil, nil
}

// Unindex removes the entries Index wrote, inside the delete
// transaction. The delete path has no manifest body (Ext is not
// indexed), so Unindex consults only m.ArtifactID and recovers the path
// from fspathindex's OWN byID entry — the same self-recovering pattern a
// rebuild would use. Symmetric with Index; idempotent: a missing byID
// entry is a clean no-op (the artifact was never indexed — foreign
// schema or no vfsmeta).
//
// A corrupt own byID payload does not fail the delete: byID is dropped
// and the (now unreachable) byPath entry is left for an offline
// reconciler. Blocking a manifest deletion on a damaged derived row
// would be the wrong trade — the manifest, not the index, is the source
// of truth.
func (e *CustomIndex) Unindex(ctx context.Context, sub customindex.Substrate, m domain.Manifest) error {
	id := string(m.ArtifactID)

	raw, ok, err := sub.Get(tableByID, id)
	if err != nil {
		return fmt.Errorf("fspathindex: get byID for unindex: %w", err)
	}
	if !ok {
		// Not indexed. Nothing to do.
		return nil
	}

	if err := sub.Delete(tableByID, id); err != nil {
		return fmt.Errorf("fspathindex: delete byID: %w", err)
	}

	fs, ok, err := vfsmeta.Decode(raw)
	if err != nil || !ok {
		// Persisted bytes unreadable (shouldn't happen — we only ever
		// stored payloads vfsmeta.Decode accepted). byID is already gone;
		// leave the orphaned byPath entry to an offline reconciler rather
		// than fail the delete.
		return nil
	}
	if err := sub.Delete(tableByPath, fs.Path+"\x00"+id); err != nil {
		return fmt.Errorf("fspathindex: delete byPath: %w", err)
	}
	return nil
}

// --- Accessor (read-side capability, ADR-78/88; 09 §9.3) ---

// Lookup implements customindex.KeyLookup: every ArtifactID indexed at
// the exact virtual path. The many-to-many shape (0..N ids) is intrinsic
// — two artifacts may briefly share a path during reindex; what that
// multiplicity MEANS (a collision vs a normal bucket) is the consuming
// view's call (07 Projection), not the accessor's. Order is the stable
// lexicographic order of the byPath suffix (the artifactID).
func (e *CustomIndex) Lookup(key string) ([]domain.ArtifactID, error) {
	if e.sub == nil {
		return nil, fmt.Errorf("fspathindex: not registered")
	}
	var ids []domain.ArtifactID
	// key+"\x00" pins the match to the exact path: a longer path that
	// shares `key` as a byte-prefix (e.g. key="a/b", "a/bc") cannot match,
	// because its key byte after `key` is a path byte, never \x00.
	err := e.sub.Scan(tableByPath, key+"\x00", func(_ string, value []byte) error {
		ids = append(ids, domain.ArtifactID(value))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fspathindex: Lookup: %w", err)
	}
	return ids, nil
}

// ScanPrefix implements customindex.PrefixScan: it streams the by-path
// subtree under prefix as (path, []ArtifactID) pairs in lexicographic
// path order — the structural stream the by-path view materialises
// (07 Projection §7.1/§7.5). The caller controls directory semantics by
// the prefix it passes: a trailing "/" scopes to a directory's subtree,
// an empty prefix streams the whole tree. Entries that share a path are
// coalesced into a single callback (their ids batched). Returning an
// error from cb stops the scan and propagates that error.
func (e *CustomIndex) ScanPrefix(prefix string, cb func(customindex.Key, []domain.ArtifactID) error) error {
	if e.sub == nil {
		return fmt.Errorf("fspathindex: not registered")
	}
	var (
		curPath string
		curIDs  []domain.ArtifactID
		have    bool
		cbErr   error
	)
	scanErr := e.sub.Scan(tableByPath, prefix, func(k string, value []byte) error {
		path, _, ok := splitPathKey(k)
		if !ok {
			// Malformed key (no separator) — skip defensively.
			return nil
		}
		if have && path != curPath {
			// Group boundary: flush the completed group.
			if err := cb(customindex.Key(curPath), curIDs); err != nil {
				cbErr = err
				return customindex.ErrStopScan
			}
			curIDs = nil
		}
		curPath = path
		have = true
		curIDs = append(curIDs, domain.ArtifactID(value))
		return nil
	})
	if scanErr != nil {
		return fmt.Errorf("fspathindex: ScanPrefix: %w", scanErr)
	}
	if cbErr != nil {
		return cbErr
	}
	// Flush the final group (the scan ran to completion).
	if have {
		if err := cb(customindex.Key(curPath), curIDs); err != nil {
			return err
		}
	}
	return nil
}

// splitPathKey splits a byPath key "<path>\x00<id>" into its path and id
// halves. ok is false when the key carries no separator (malformed).
func splitPathKey(k string) (path, id string, ok bool) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:], true
		}
	}
	return "", "", false
}

// --- Read API (host-facing; not part of a capability interface) ---

// Metadata implements source.Metadata (declared in the projection
// package): the bulk vfsmeta-JSON source the View backfill consults to
// materialise FilesystemFacet without round-tripping Source.Get per
// manifest. Returns the bytes verbatim — callers decode with
// vfsmeta.Decode (or any future schema decoder). (nil, false, nil) when
// the artifact is not indexed here.
func (e *CustomIndex) Metadata(id domain.ArtifactID) (json.RawMessage, bool, error) {
	return e.GetByID(id)
}

// GetByID returns the persisted vfsmeta JSON for the given artifact id,
// or (nil, false, nil) if not indexed. Same bytes as the manifest
// carried at write time.
func (e *CustomIndex) GetByID(id domain.ArtifactID) (json.RawMessage, bool, error) {
	if e.sub == nil {
		return nil, false, fmt.Errorf("fspathindex: not registered")
	}
	value, ok, err := e.sub.Get(tableByID, string(id))
	if err != nil {
		return nil, false, fmt.Errorf("fspathindex: GetByID: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	return json.RawMessage(value), true, nil
}

// LookupByPath returns the first ArtifactID indexed at path (lexicographic
// tiebreak when more than one artifact shares it — rare, usually a
// transient). A host that wants every id calls Lookup (KeyLookup); this
// is the single-result convenience the FUSE Stat / WebDAV PROPFIND
// hot-paths use. (\"\", false, nil) when no artifact is indexed at path.
func (e *CustomIndex) LookupByPath(path string) (domain.ArtifactID, bool, error) {
	ids, err := e.Lookup(path)
	if err != nil {
		return "", false, err
	}
	if len(ids) == 0 {
		return "", false, nil
	}
	return ids[0], true, nil
}

// WalkAll iterates every (artifactID, vfsmeta JSON) pair in lexicographic
// id order. Returning customindex.ErrStopScan from cb ends the walk
// cleanly. A convenience for tooling that wants the whole path index in
// one pass (the View takes the bulk path through Metadata).
func (e *CustomIndex) WalkAll(cb func(id domain.ArtifactID, raw json.RawMessage) error) error {
	if e.sub == nil {
		return fmt.Errorf("fspathindex: not registered")
	}
	return e.sub.Scan(tableByID, "", func(key string, value []byte) error {
		return cb(domain.ArtifactID(key), json.RawMessage(value))
	})
}

// Compile-time conformance. Catches signature drift early: fspathindex is
// a CustomIndex that additionally occupies the Indexer (write) and
// KeyLookup+PrefixScan (read) capabilities. The ViewProvider assertion
// lives in viewprovider.go alongside ProvidedViews.
var (
	_ customindex.CustomIndex = (*CustomIndex)(nil)
	_ customindex.Indexer     = (*CustomIndex)(nil)
	_ customindex.KeyLookup   = (*CustomIndex)(nil)
	_ customindex.PrefixScan  = (*CustomIndex)(nil)
)
