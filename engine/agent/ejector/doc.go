// Package ejector implements the Scrinium ejector agent:
// content-addressable materialisation of artifacts to a local cache.
//
// It is a plugin behind the agent registry (ADR-68): a blank import of
// this package registers its factory via register.go, after which the
// assembler builds it through agent.Build. The agent embeds
// agent.BaseState and satisfies the agent.Agent contract.
//
// The bytes-onto-disk write is not this package's own: it is
// internal/materialize, shared with the Extractor (ADR-97 INV-97-5).
// What stays here is the ejector's dialect around it — content-hash naming, the
// volatile table of materialisations, reuse verification, size-cap eviction, and
// disk errors mapped onto this package's sentinels.
package ejector
