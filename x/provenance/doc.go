// Package provenance records where artifacts come from, and lets them be parted
// with deliberately (ADR-112, ADR-113).
//
// A production record says: this artifact was obtained by operation op, with
// parameters params, from these input artifacts. The record lives in two halves
// of one manifest. The edge targets go into the core's HandleRefs — the
// content-addressed artifact→artifact DAG (ADR-92) — written publicly by
// domain.WithParentRefs. The meaning of those edges goes into this extension's
// Ext block under the "provenance" key, parallel to the edge array by position:
// the relation kind per edge, the operation, its parameters, whether the
// operation is reproducible, and the outcome.
//
// Direction is fixed by WORM and is not a design choice: a parent is frozen
// before its child exists, so only the child can carry the reference. "Children
// of X" is never stored — it is a reverse traversal, served by this extension's
// index.
//
// # What it answers
//
// Where did this come from, and what was made of it (Parents, Children, Results,
// WalkUp, WalkDown). What work is still owed (Holes) and what has already been
// done with these exact parameters (Done, Failures). What is current after a
// chain of replacements (Head). Whether an artifact may be dropped as a cache
// (Cleanable). None of it is stored twice: every answer is derived from the
// manifests, so the index can be deleted and rebuilt.
//
// # Eviction
//
// A derivative pins its source, which is the right default and a wrong absolute:
// after recognising a PDF the scan may no longer be wanted. Deleting a pinned
// source therefore requires a receipt — an artifact stating what disappeared,
// what was kept instead, why, and on whose decision (Evictor, Receipt). Ordinary
// deletion rules are untouched; eviction only adds the precondition and the
// explanation. The graph is not rewritten: the derivative still names the source
// it came from, and only reachability changes — which is why the declared
// reproducibility flag must be read through Cleanable rather than on its own.
//
// # What this package does not know
//
// Relation kinds and parameters are opaque. This package neither defines nor
// validates the vocabulary of a host application: "ocr", "thumbnail",
// "distilled", the shape of params — all of it is the host's. Two kinds are
// mechanical and both are configurable: the supersede kind, whose chains resolve
// to a head, and the eviction kind, which a receipt carries. Reproducibility is
// likewise declared, not deduced: the host states it, the extension stores it and
// answers questions by it, and the policy it implies belongs to the host.
//
// # Deployment note
//
// The extension is load-bearing: without it the guard is gone, a pinned source
// deletes with no receipt, and new writes get edges from the core but no meaning
// — and manifests are immutable, so such artifacts cannot be given their record
// later. Install it before writing production records, and keep it installed. The
// general mechanism for requiring load-bearing extensions is still a draft
// (6. Drafts/Load-Bearing-Extensions-Rationale.md).
package provenance
