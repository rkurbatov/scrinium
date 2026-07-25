// Package ingester captures a source tree into a store (ADR-115).
//
// It is the forward half of the Ingester↔Extractor pair and the paired agent of
// the filesystem-path extension: it writes the vfsmeta block that extension
// indexes, and it needs that index to answer "have I captured this path
// already" — without which a second sweep would write second artifacts over the
// same bytes, because identity is unique per Put.
//
// # Why it lives here and not under engine/agent
//
// A core agent may not depend on an extension; this one does, on the path index
// above. So it lives with the extension it belongs to, and the dependency is a
// fact of the package tree rather than a promise in prose.
//
// It is also NOT registered as a factory-built agent. The standard factory gets
// the store, its driver and its index — and an ingester's source is a SECOND
// driver the assembler knows nothing about, plus the extension's own index and
// scoped system store. So the host constructs it explicitly (which is what
// "user-managed" in the agent contract means) and runs it through the scheduler
// or by hand.
//
// Everything about it is agnostic by construction. The source is a driver, so
// the same pass ingests a local folder and an object store. The domain half is a
// Policy: what to take, and what meaning to attach at capture. The agent itself
// adds only what it knows — the normalised path, the mode, the modification
// time — and knows no vocabulary of any application.
//
// # What guards a capture
//
// A file being written must not be read half-way. The unconditional guard is a
// settle window on modification time; a driver that can tell a finished file
// from one being appended to (driver.ReadinessProber) vetoes on top of it,
// earlier and more precisely, but never releases what the window still holds —
// advisory locks are advisory, and most writers take none.
//
// # Where progress lives
//
// A watermark in the extension's scoped SystemStore, per source. It answers
// "what to look at", not "what has been captured": losing it costs a slow full
// sweep and nothing else. It advances only past elements the pass finished with,
// so a deferred or failed element stays reachable next time.
//
// # Directories
//
// Optional (Config.CaptureDirs, ADR-114) and only over a driver that reports
// directory entries: an empty directory yields no object path, so an object
// store has nothing to offer here. Asking for directories over such a driver is
// refused at construction rather than silently dropped — a tree that looks
// complete and is not would be worse than an error.
//
// Watch mode is deferred: deletions, renames and a lost subscription each need
// their own answer (ADR-115 П-10).
package ingester
