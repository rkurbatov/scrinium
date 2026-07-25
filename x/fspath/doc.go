// Package fspath is an index custom index that persists the Ext block
// of every artifact that carries a vfsmeta payload (Manifest.Ext
// under the "vfsmeta" key). It hangs off StoreIndex via the index
// custom indexes infrastructure (see 3. Reference/09 CustomIndex and Search.md).
//
// The custom index serves two roles:
//
//   - Backfill source for view.View. After a process
//     restart the View needs to rebuild its filesystem trees
//     from indexed metadata; without fspathindex it would fall
//     back to N+1 round-trips through Source.Get to re-read
//     each manifest's Ext. fspathindex persists those bytes
//     once at write time, so backfill is a single bulk scan.
//
//   - Direct path lookup. Hosts that want to translate a
//     virtual path to an ArtifactID without standing up a
//     full View (FUSE Stat hot-path, WebDAV PROPFIND on a
//     specific resource) call LookupByPath. The custom index
//     keeps a reverse index for O(log N) lookups.
//
// The paired agents live beside this package: x/fspath/ingester captures a
// source tree into the store, and the extractor lays a tree back out. Both
// depend on this index — the ingester to tell an already-captured path from a
// new appearance, the extractor to resolve paths — so they belong to the
// extension rather than to the engine's agent tree (ADR-115). Neither is a
// factory-built agent: their source or target is a second driver the assembler
// knows nothing about, so the host constructs them explicitly.
//
// fspathindex stores the Ext JSON as-is rather than pre-decoded
// columns. The schema is versioned (a "version" field in the vfsmeta
// payload; future versions…); keeping the bytes verbatim lets newer
// schemas flow through without an fspathindex migration whenever
// vfsmeta adds a field.
package fspath
