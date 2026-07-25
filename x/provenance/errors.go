package provenance

import "errors"

// Sentinel errors of the extension. They are its own domain — the meaning of a
// production record and of an eviction — and are deliberately separate from the
// core's shape errors (errs.ErrTooManyRefs for the edge count,
// errs.ErrInvalidHandleRef for an empty or repeated edge).

// ErrBadProduction — a production record is malformed and the artifact is not
// written: relation kinds that do not match the inputs, an empty kind, a
// duplicate input, an operation missing where there are inputs, params that are
// not JSON. It fires in the wrapper, before the core Put, so a bad record never
// reaches a manifest — an artifact is immutable, and a half-meant record would
// be permanent. The same error surfaces during indexing if a stored manifest is
// found whose block does not line up with its edges (a foreign or corrupt one).
var ErrBadProduction = errors.New("provenance: bad production record")

// ErrHasDerivatives — a Delete was refused because the artifact still has
// derivatives (incoming consumption edges) and no receipt explains its
// eviction. Until the core accounts for artifact→artifact edges this guard is
// the only protection a source has, and it lives in the wrapper: a deployment
// without this extension does not have it.
var ErrHasDerivatives = errors.New("provenance: artifact has derivatives")

// ErrForked — a supersede chain branches: more than one artifact claims to
// replace the same one. Detecting the fork is mechanical; choosing the winner is
// a policy this extension does not have.
var ErrForked = errors.New("provenance: superseded by more than one artifact")

// ErrBadReceipt — an eviction receipt is malformed: no reason stated, no
// decider, an undecodable or wrongly-versioned document, or one that names no
// evicted artifact. An eviction that cannot be attributed is indistinguishable
// from data loss, so the receipt is refused before anything is written.
var ErrBadReceipt = errors.New("provenance: bad eviction receipt")

// ErrReceiptProtected — an attempt to delete a receipt. A receipt is the only
// surviving explanation of bytes that are already gone; deleting it would leave
// a dangling reference with no account of why, which is the state eviction
// exists to avoid.
var ErrReceiptProtected = errors.New("provenance: receipt cannot be deleted")
