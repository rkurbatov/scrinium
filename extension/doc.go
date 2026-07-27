// Package extension is the client-level umbrella over Scrinium's
// extension axes. An Extension is one whole unit a host installs; the
// parts it carries are routed to their levels — a CustomIndex into the
// StoreIndex (Tier 2), a behavior wrapper onto the store's data plane
// (Tier 3), a paired background agent onto the scheduler (the optional
// "background add-on"). The host reasons about extensions as wholes, not
// about the axes underneath (ADR-88, "Client = descriptor-driven
// auto-wiring").
//
// The additive axes here follow ADR-88's per-store descriptor: behavior
// (wrapper), CustomIndex, and the optional background agent. (The
// multistore plane — custom routing, MultistoreIndex, namespace-sync —
// is a separate plane wired later; the capability-surface axis is a
// host type-assert and needs no umbrella plumbing.)
//
// Manual per-axis wiring (storeIndex.Register, store-level wrap,
// scheduler.Add) yields the identical result (ADR-88, Principle 13); Use
// is the one-call convenience over the same primitives, for a live
// target whose store and scheduler already exist. The assembler applies
// the parts directly at their construction phases (index before store
// open, wrapper at open, agent after the scheduler exists), which is the
// same per-part path.
package extension
