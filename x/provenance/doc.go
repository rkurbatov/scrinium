// Package provenance records where artifacts come from (ADR-112).
//
// A production record says: this artifact was obtained by operation op, with
// parameters params, from these input artifacts. The record lives in two
// halves of one manifest. The edge targets go into the core's HandleRefs —
// the content-addressed artifact→artifact DAG (ADR-92) — written publicly by
// domain.WithParentRefs. The meaning of those edges goes into this
// extension's Ext block under the "provenance" key, parallel to the edge
// array by position: the relation kind per edge, the operation, its
// parameters, whether the operation is reproducible, and the outcome.
//
// Direction is fixed by WORM and is not a design choice: a parent is frozen
// before its child exists, so only the child can carry the reference.
// "Children of X" is never stored — it is a reverse traversal, served by this
// extension's index.
//
// # What this package does not know
//
// Relation kinds and parameters are opaque. This package neither defines nor
// validates the vocabulary of a host application: "ocr", "thumbnail",
// "distilled", the shape of params — all of it is the host's. The single
// exception is the supersede relation, whose name is configurable and whose
// only mechanical use is resolving a chain of replacements to its head.
// Reproducibility is likewise declared, not deduced: the host states it, the
// extension stores it and can answer questions by it, and the policy it
// implies (clean reproducible derivatives, back up the rest) belongs to the
// host.
//
// # Deployment note
//
// WithProduction resolves into two things: the core edge option and a
// per-call hint this extension's wrapper turns into the Ext stamp. Without
// the wrapper installed the edges are still written — the core accepts
// them — but the manifest carries no provenance block, and a manifest
// without the block is skipped by this extension (as fspath skips a manifest
// without vfsmeta). Since manifests are immutable, such an artifact cannot
// be given its record later: install the extension before writing
// production records.
package provenance
