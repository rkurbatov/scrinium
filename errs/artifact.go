package errs

import (
	"errors"
	"io/fs"
)

// Artifact-level operations: lookup, namespace policy, retention,
// deletion.

// ErrArtifactNotFound — no manifest with the given ArtifactID
// exists in the Store, or it is a headless pack container (empty
// identity slot — not a client-visible artifact). Bridges to
// fs.ErrNotExist for host code that handles missing artifacts
// the same way as missing files.
var ErrArtifactNotFound = newBridgedSentinel(
	"scrinium: artifact not found", fs.ErrNotExist,
)

// ErrUnavailable — the artifact (or a pack/composite member) exists and
// is accounted for, but cannot be resolved because its owning extension
// (e.g. the bundler/chunker Resolver) is not registered — for instance a
// rebuild performed without the owner. Distinct from ErrArtifactNotFound:
// "present but unreachable" is not "does not exist", so this sentinel
// deliberately does NOT bridge to fs.ErrNotExist. The anchor manifest is
// always scattered, so the container is found; only the packed member is
// unreachable without its overlay. ADR-92/86/87 (rebuild-safety invariant).
var ErrUnavailable = errors.New("scrinium: artifact unavailable")

// ErrDeletionForbidden — Delete on a Store with
// DeletionPolicy: NoDelete.
var ErrDeletionForbidden = errors.New("scrinium: deletion forbidden")

// ErrRetentionNotExpired — Delete or RollbackSession on an artifact
// with an active RetentionUntil.
var ErrRetentionNotExpired = errors.New("scrinium: retention not expired")

// ErrArchivedArtifact — the artifact is reachable only through a
// Backup with ReadPolicy: Never; AllowColdRead is required.
var ErrArchivedArtifact = errors.New("scrinium: archived artifact")

// ErrReservedNamespace — an attempt to use the "*" wildcard namespace
// where a concrete namespace is required.
var ErrReservedNamespace = errors.New("scrinium: reserved namespace")

// ErrNamespaceTooLong — namespace > 255 bytes.
var ErrNamespaceTooLong = errors.New("scrinium: namespace too long")

// ErrSessionIDTooLong — SessionID > 255 bytes.
var ErrSessionIDTooLong = errors.New("scrinium: session ID too long")

// ErrEmptySessionID — RollbackSession called with an empty string;
// guards against a mass deletion of sessionless artifacts.
var ErrEmptySessionID = errors.New("scrinium: empty session ID")

// ErrExtTooLarge — Artifact.Ext > MaxExtSize (64 KB). Ext is the
// engine-readable extension block (vfsmeta and friends).
var ErrExtTooLarge = errors.New("scrinium: ext metadata too large")

// ErrArtifactTooLarge — the payload crossed StoreConfig.MaxArtifactSize
// (class II governance limit; 0 = unlimited). Raised by the streaming
// guard on the Put paths, so undeclared-length streams are caught too.
var ErrArtifactTooLarge = errors.New("scrinium: artifact exceeds MaxArtifactSize")

// ErrUsrTooLarge — Artifact.Usr > MaxUsrSize (64 KB). Usr is the
// opaque host-application block.
var ErrUsrTooLarge = errors.New("scrinium: usr metadata too large")

// ErrInvalidHandleRef — a handle_refs entry is malformed on write: an empty
// ref, or the same target repeated in one manifest. Shape-only: the core
// deliberately does NOT verify that a target exists (an edge is declarative,
// ADR-112 — resolving it would put an index read on the write path and break
// restore, where the target's replica may arrive later). Semantic validation
// of a production record (relation kinds, operation, parameters) belongs to
// the provenance extension and surfaces as its own error.
var ErrInvalidHandleRef = errors.New("scrinium: invalid handle ref")

// ErrTooManyRefs — a manifest reference array (blob_refs or handle_refs)
// exceeds MaxBlobRefs/MaxHandleRefs (65535). ADR-93: the on-disk list is
// 16-bit length-counted, so it cannot hold more.
var ErrTooManyRefs = errors.New("scrinium: too many references")

// ErrManifestTooLarge — a manifest file read from storage exceeds
// MaxManifestSize. Read-time allocation guard: a corrupt or hostile file
// is rejected before it is fully buffered or parsed. The encode path is
// bounded by the per-field limits, not by this.
var ErrManifestTooLarge = errors.New("scrinium: manifest too large")
