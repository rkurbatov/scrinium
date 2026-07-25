package provenance

// Config tunes the extension. The zero value is valid.
//
// It lives on the Index, and every other part reads it from there: the wrapper,
// the extension unit and the evictor are all constructed from an index and take
// its configuration. Passing the configuration twice would let the writing side
// and the reading side disagree about which relation kind means what, and a
// divergence of that shape would only show up as a graph that answers the wrong
// question months later.
type Config struct {
	// SupersedeRel is one of the two relation kinds this extension interprets:
	// the chains it resolves to a head. Empty means DefaultSupersedeRel. A host
	// with its own word for replacement names it here; every other kind stays
	// opaque either way.
	SupersedeRel string

	// EvictRel is the other: the kind of edge a receipt carries to the artifact
	// whose disappearance it explains (ADR-113). Empty means DefaultEvictRel.
	// Deliberately distinct from SupersedeRel — "what is current" and "where did
	// the bytes go" are different questions, and merging them would make a
	// receipt for a deletion the head of the artifact's currency chain.
	EvictRel string

	// GuardDeletes arms the interim protection of sources: a Delete of an
	// artifact that still has derivatives is refused unless a receipt explains
	// the eviction, and a receipt itself is never deletable. It requires the
	// index, and it exists because the core does not yet account for
	// artifact→artifact edges; when it does, the core's accounting supersedes
	// this guard and the flag goes away.
	GuardDeletes bool
}

func (c Config) supersedeRel() string {
	if c.SupersedeRel == "" {
		return DefaultSupersedeRel
	}
	return c.SupersedeRel
}

func (c Config) evictRel() string {
	if c.EvictRel == "" {
		return DefaultEvictRel
	}
	return c.EvictRel
}
