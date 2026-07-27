package errs

import "errors"

// Error classification for retry logic. Two predicates classify any
// error — a sentinel from this package, a wrapped chain, or a backend
// error that opts in through the marker methods described below:
//
//   - IsTransient reports a temporary failure whose cause may clear on
//     its own (a backend briefly unavailable, lock contention, a store
//     still warming up, a network blip once the remote driver lands).
//     The operation is worth attempting again later.
//   - IsRetriable reports that retrying is safe: the request provably
//     did not take effect, or the operation is idempotent. Every
//     retriable error is transient, but not every transient one is
//     retriable — an "uncertain" failure (request sent, response lost)
//     is transient yet unsafe to blindly retry, because the write may
//     already have landed.
//
// Locally these rarely fire; the contract exists now so the future
// network client can absorb transport faults transparently
// (Principle 13) without callers rewriting their error handling.
//
// # Opting in
//
// An error outside this package joins the classification by exposing
// either marker method (no import of errs needed — matching is by
// method set):
//
//	func (e *myErr) Transient() bool { return true }  // temporary
//	func (e *myErr) Retriable() bool { return false } // e.g. uncertain
//
// Transient() declares the temporary/permanent nature; Retriable()
// overrides the default "retriable == transient" rule, chiefly to mark
// an uncertain failure as transient-but-not-safe-to-retry. The methods
// are honoured anywhere in the wrapped chain (errors.As semantics); the
// innermost declaring error wins.

// transient is the opt-in marker an error implements to declare its
// temporary/permanent nature. Mirrors the net.Error.Temporary() idiom.
type transient interface{ Transient() bool }

// retriable is the opt-in marker an error implements to override the
// default rule that a transient error is safe to retry.
type retriable interface{ Retriable() bool }

// transientSentinels are the package sentinels that are transient by
// nature: the same operation, retried later, may succeed. All of them
// are also safe to retry, so IsRetriable defaults to true for them.
var transientSentinels = []error{
	ErrSourceUnavailable,     // projection source briefly unavailable
	ErrLeaseHeld,             // another owner holds the lease — retry later
	ErrLeaseLost,             // lease lost in flight — re-acquire and retry
	ErrMaintenanceInProgress, // an agent holds the maintenance lease
	ErrStoreNotReady,         // store still bootstrapping
	ErrStoreOffline,          // store in Offline maintenance mode
}

// IsTransient reports whether err (or any error it wraps) is a
// temporary failure worth attempting again. An explicit Transient()
// marker anywhere in the chain decides the verdict; otherwise the
// curated set of transient sentinels is consulted. Returns false for
// nil and for terminal/denied errors.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var t transient
	if errors.As(err, &t) {
		return t.Transient()
	}
	for _, s := range transientSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// IsRetriable reports whether it is safe to retry the operation that
// produced err. An explicit Retriable() marker anywhere in the chain
// decides the verdict; otherwise retriable defaults to transient — so
// an uncertain failure opts out by exposing Retriable() == false while
// keeping Transient() == true. Returns false for nil.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}
	var r retriable
	if errors.As(err, &r) {
		return r.Retriable()
	}
	return IsTransient(err)
}
