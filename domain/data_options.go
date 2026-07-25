package domain

import "time"

// PutOption configures a Put. Options apply left to right; the zero
// set means: default namespace, no session, regular blob, no
// retention, no routing hints.
//
// The options live in domain (not the store) so the projection layer
// and other store clients can construct Put/Get calls without
// importing the engine. The store applies them via ApplyPut.
type PutOption func(*PutOptions)

// WithSession ties the Put to a session for RollbackSession.
func WithSession(id SessionID) PutOption {
	return func(o *PutOptions) { o.SessionID = id }
}

// WithBlobType sets the blob type (default: regular).
func WithBlobType(bt BlobType) PutOption {
	return func(o *PutOptions) { o.BlobType = bt }
}

// WithRetention sets a retention deadline for the artifact.
func WithRetention(t time.Time) PutOption {
	return func(o *PutOptions) { o.RetentionUntil = t }
}

// WithRouting attaches routing hints (e.g. for a multistore router).
func WithRouting(h RoutingHints) PutOption {
	return func(o *PutOptions) { o.Routing = h }
}

// WithExtHint attaches an opaque, extension-scoped hint to a Put under key
// (an extension's stable name). The core never reads it — it carries the
// hint through to the behavior wrappers, which alone interpret their own
// key. It is the generic per-call channel by which a client hands an
// extension a value (e.g. a target namespace name) without the core
// learning that extension's vocabulary. A repeated key overwrites.
func WithExtHint(key, value string) PutOption {
	return func(o *PutOptions) {
		if o.ExtHints == nil {
			o.ExtHints = map[string]string{}
		}
		o.ExtHints[key] = value
	}
}

// WithParentRefs declares this artifact's source artifacts — edges into the
// content-addressed DAG (ADR-92/112). The refs land in Manifest.HandleRefs in
// the given order; position is significant and is preserved by the index.
//
// Direction is fixed by the WORM model: a parent is frozen before its child
// exists, so only the child can carry the reference. "Children of X" is never
// stored — it is a reverse traversal, which the provenance extension provides.
//
// The core validates shape only (count against MaxHandleRefs, non-empty, no
// duplicates) and never resolves the targets: an edge is a declaration, not a
// join. Repeated calls replace the previous set rather than appending, so a
// caller assembling refs incrementally builds its own slice.
func WithParentRefs(refs ...HandleRef) PutOption {
	return func(o *PutOptions) {
		if len(refs) == 0 {
			o.ParentRefs = nil
			return
		}
		o.ParentRefs = append([]HandleRef(nil), refs...)
	}
}

// ApplyPut folds options into a PutOptions. The store calls this to
// resolve a Put; decorators and test doubles that wrap a Store and
// need the resolved values (namespace, session, …) use it too.
func ApplyPut(opts ...PutOption) PutOptions {
	var o PutOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// GetOption configures a Get. The zero set means a normal read.
type GetOption func(*GetOptions)

// WithColdRead permits fetching from cold/expensive backing storage
// (e.g. a multistore tier normally skipped on reads).
func WithColdRead() GetOption {
	return func(o *GetOptions) { o.AllowColdRead = true }
}

// ApplyGet folds options into a GetOptions. Decorators (e.g.
// multistore) that wrap a Store and branch on the result use it.
func ApplyGet(opts ...GetOption) GetOptions {
	var o GetOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
