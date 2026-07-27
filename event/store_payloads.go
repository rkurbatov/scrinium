package event

import (
	"time"

	"scrinium.dev/config"
	"scrinium.dev/domain"
)

// store_payloads.go — event-type name constants and their payload
// structs. Pure data: every field is a domain type or stdlib. No
// dependency on *store or the event bus, so this file is the unit
// that moves to the event package wholesale — the
// publish method that consumes them stays behind in events.go.

const (
	EventManifestSaved         = "store.manifest_saved"
	EventArtifactDeleted       = "store.artifact_deleted"
	EventBlobPhysicallyDeleted = "store.blob_physically_deleted"
	EventPackCompacted         = "store.pack_compacted"
	EventPackSealed            = "store.pack_sealed"
	EventArtifactMigrated      = "store.artifact_migrated"
	EventCapacityWarning       = "store.capacity_warning"
	EventScrubFailed           = "store.scrub_failed"
	EventStoreDegraded         = "store.store_degraded"
	EventKEKRotated            = "store.kek_rotated"
	EventStaleLeaseTakeover    = "store.stale_lease_takeover"
	EventOrphanScanCompleted   = "store.orphan_scan_completed"
	EventRollbackCompleted     = "store.rollback_completed"
	EventConfigUpdated         = "store.config_updated"
	EventStoreRecovered        = "store.store_recovered"
)

// --- Payload structs ---

// ManifestSavedPayload is the payload of EventManifestSaved.
type ManifestSavedPayload struct {
	Manifest domain.Manifest
}

// PackSealedPayload is the payload of EventPackSealed. Emitted by
// the bundler when an open batch is sealed into a .pack volume in
// the transit store, before it is delivered to its destination.
type PackSealedPayload struct {
	PackRef    string
	BlobCount  int
	TotalBytes int64
}

// ArtifactMigratedPayload is the payload of EventArtifactMigrated.
// Emitted by the MigrationAgent when an artifact accumulated in the
// transit store has been packed and delivered to its destination
// store, reaching its permanent placement.
type ArtifactMigratedPayload struct {
	ArtifactID  domain.ArtifactID
	DestStoreID string
}

// ArtifactDeletedPayload is the payload of EventArtifactDeleted.
// Emitted only when the logical deletion actually happens. If the
// deletion is rejected by retention or policy, the event is not
// emitted.
type ArtifactDeletedPayload struct {
	ArtifactID domain.ArtifactID
}

// RollbackCompletedPayload is the payload of EventRollbackCompleted.
// Emitted after Store.RollbackSession successfully removes the
// artifacts written under a session. Not emitted when the session
// had nothing to roll back (zero artifacts). SessionID is the
// client-chosen correlation tag; RolledBack is the number of
// artifacts removed.
type RollbackCompletedPayload struct {
	SessionID  string
	RolledBack int
}

// ConfigUpdatedPayload is the payload of EventConfigUpdated. Emitted
// after Store.UpdateConfig swaps the active StoreConfig (mutable
// fields only). Config is a snapshot of the new active config; a
// subscriber that needs the previous value caches the prior
// snapshot from an earlier event.
type ConfigUpdatedPayload struct {
	Config config.StoreConfig
}

// OrphanScanCompletedPayload is the payload of
// EventOrphanScanCompleted. Emitted by the bootstrap recovery
// after every transition into Unlocked, summarising what the scan
// found and removed. Counts are physical files removed; non-fatal
// I/O errors during the scan (per-file Remove failures, individual
// path-parse glitches) are aggregated into NonFatalErrors. The
// scan never refuses to open a Store — operators read the count
// here and dig into engine logs for details.
type OrphanScanCompletedPayload struct {
	StagingRemoved int
	// BlobsRemoved is zero for the open-time pass, which never deletes a
	// blob (ADR-118); the field carries the count of the media
	// reconciliation, which runs as maintenance.
	BlobsRemoved int
	// ManifestsRemoved counts fragments of interrupted writes — files whose
	// bytes do not hash to their own name. A whole manifest the index did
	// not know is never removed.
	ManifestsRemoved int
	// ManifestsIndexed counts whole manifests read back into the index.
	ManifestsIndexed int
	NonFatalErrors   int
	Duration         time.Duration
}

// BlobPhysicallyDeletedPayload is the payload of
// EventBlobPhysicallyDeleted. Emitted by the GC Agent when the
// Sweep phase completes (physical removal of the file after the
// Grace Period).
type BlobPhysicallyDeletedPayload struct {
	BlobRef string
}

// PackCompactedPayload is the payload of EventPackCompacted.
// Emitted on a successful compaction of a partially dead pack
// volume.
type PackCompactedPayload struct {
	OldPackRef  string
	NewPackRef  string
	LiveEntries int
	DeadEntries int
	OldSize     int64
	NewSize     int64
	FreedBytes  int64
	Duration    time.Duration
}

// CapacityWarningPayload is the payload of EventCapacityWarning.
// UsedPct is the share of used space (0..100).
type CapacityWarningPayload struct {
	UsedPct float64
}

// ScrubFailedPayload is the payload of EventScrubFailed. Emitted
// by the Scrub Agent or by Store.Verify on a hash divergence.
type ScrubFailedPayload struct {
	ArtifactID domain.ArtifactID
	Err        error
}

// StoreDegradedPayload is the payload of EventStoreDegraded.
// Emitted on a transition into StateDegraded (descriptor-replica
// divergence).
type StoreDegradedPayload struct {
	Reason string
}

// StoreRecoveredPayload is the payload of EventStoreRecovered:
// the liveness sentinel (ADR-111) healed a sentinel-set Offline —
// the descriptor is readable again with the same store_id.
type StoreRecoveredPayload struct {
	Reason string
}

// LeaseTakeoverPayload is the payload of two events:
// EventStaleLeaseTakeover (Store-level lease — Open under a stale
// location.lock) and EventAgentStaleLease (an agent took over from a
// previous holder that stopped renewing). The stale-lease concept is
// layer-agnostic, so a single struct in this package describes both.
type LeaseTakeoverPayload struct {
	LeaseKey       string
	PreviousHolder string
	ExpiredAt      time.Time
	TakenBy        string
}
