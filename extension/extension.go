package extension

import (
	"context"
	"fmt"

	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/wrapper"
)

// Descriptor identifies an Extension as a whole and is the unit the
// client surfaces (e.g. on a stats page). It is deliberately free of
// the axis detail the extension occupies underneath.
type Descriptor struct {
	// Name is the extension's stable identifier (e.g. "fs").
	Name string
}

// Agent is one paired background worker an extension brings along (the
// "background add-on"): an agent Kind that must be registered in the agent
// registry (blank-import its package, as with drivers), optionally
// scheduled. Schedule is "" for manual-only (run via RunMaintenance), an
// interval string ("6h"), or a cron expression (requires cron.Enable).
// Config is handed to the agent's factory.
type Agent struct {
	Kind     string
	Schedule string
	Config   any
}

// Extension is one whole, client-level unit composed of axis parts. Each
// part accessor returns (part, true) when the extension occupies that
// axis and (zero, false / nil) otherwise.
type Extension interface {
	// Descriptor reports the extension's identity.
	Descriptor() Descriptor

	// CustomIndex is the index-axis part (Tier 2), registered into the
	// StoreIndex. Returns (nil, false) when the extension has none.
	CustomIndex() (customindex.CustomIndex, bool)

	// Wrapper is the behavior-axis part (Tier 3): a decorator factory
	// applied to the store's data plane. Returns (nil, false) when the
	// extension adds no behavior.
	Wrapper() (wrapper.Factory, bool)

	// Agents are the optional paired background workers the extension
	// brings (durable state + worker). Empty when none.
	Agents() []Agent
}

// Target is the seam a live host implements so Use can route an
// extension's parts to their levels without this package importing the
// engine wiring. Each method installs one axis.
type Target interface {
	// RegisterCustomIndex installs ci into the backing StoreIndex.
	RegisterCustomIndex(ctx context.Context, ci customindex.CustomIndex) error
	// Wrap applies f to the store's data plane.
	Wrap(f wrapper.Factory) error
	// AddAgent registers a paired background agent.
	AddAgent(a Agent) error
}

// Use installs e as one whole over a live target: it routes each
// occupied axis part to its level through t. The result is identical to
// wiring the parts by hand (ADR-88, Principle 13). It is for targets
// whose store and scheduler already exist; the assembler, which builds
// those in phases, applies the parts directly at each phase instead.
func Use(ctx context.Context, t Target, e Extension) error {
	name := e.Descriptor().Name
	if ci, ok := e.CustomIndex(); ok {
		if err := t.RegisterCustomIndex(ctx, ci); err != nil {
			return fmt.Errorf("extension %q: register custom index: %w", name, err)
		}
	}
	if f, ok := e.Wrapper(); ok {
		if err := t.Wrap(f); err != nil {
			return fmt.Errorf("extension %q: apply wrapper: %w", name, err)
		}
	}
	for _, a := range e.Agents() {
		if err := t.AddAgent(a); err != nil {
			return fmt.Errorf("extension %q: add agent %q: %w", name, a.Kind, err)
		}
	}
	return nil
}

// Env carries the late-bound handles an extension may need, delivered
// once after the store has opened (ADR-100/101 §4). It is delivered at a
// post-assembly hook rather than at CustomIndex.Setup: the index axis is
// registered before the store opens (the first IndexManifest must
// dispatch into it), so a store-backed handle like the scoped
// SystemStore does not yet exist at Setup time. The struct is
// deliberately extensible — a new handle is a new field, and the
// Receiver signature does not change.
type Env struct {
	// SystemStore is the extension's own scoped SystemStore: an
	// "extension.<name>." slice of the store's System(), built and
	// confined by the assembler from the extension's Descriptor name so
	// a plugin can address only its own slice (ADR-101). nil only if the
	// extension declared no need for one. Durable extension state (e.g.
	// a registry) lives here and survives an index rebuild.
	SystemStore *ScopedSystemStore
}

// Receiver is the optional capability an extension implements when it
// needs the late-bound Env (a scoped SystemStore, …). The assembler
// detects it by assertion — exactly like the CustomIndex / Wrapper /
// Agent axes — and calls UseEnv once, after the store is open, before
// the projection is built. An extension that needs no late handle does
// not implement it and is skipped. UseEnv must be idempotent.
type Receiver interface {
	UseEnv(env Env) error
}
