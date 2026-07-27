package errs

import "errors"

// StoreIndex and maintenance: schema migrations, integrity of the
// SQLite/Postgres backing store, the maintenance-agent lease,
// agent-specific preconditions (checkpoint availability, recovery
// kit).

// ErrIndexCorrupted — the StoreIndex is missing or its checksum
// does not match.
var ErrIndexCorrupted = errors.New("scrinium: index corrupted")

// ErrIndexSchemaMismatch — the StoreIndex schema version is
// incompatible with the running binary.
var ErrIndexSchemaMismatch = errors.New("scrinium: index schema mismatch")

// ErrMaintenanceInProgress — another Maintenance Agent holds the
// lease.
var ErrMaintenanceInProgress = errors.New("scrinium: maintenance in progress")

// ErrNoCheckpoint — RebuildIndexAgent.Validate with
// Source: Checkpoint when no valid checkpoints are available.
var ErrNoCheckpoint = errors.New("scrinium: no valid checkpoint for Source=Checkpoint")

// ErrRecoveryKitRequired — RebuildIndexAgent.Validate with the
// Store in Corrupted after every descriptor replica has been lost
// and RecoveryKit is nil in the configuration.
var ErrRecoveryKitRequired = errors.New("scrinium: RecoveryKit required (descriptor lost, encrypted store)")

// ErrIndexIncomplete — the index was expected to survive the previous close
// but is empty (or short) while the Location holds manifests (ADR-118).
// OpenStore refuses instead of rebuilding silently: a silent rebuild would
// hide the real fault — the wrong path, another store's index, a disk that
// lost the file. Recovery is available explicitly; the procedure is the same
// one an ephemeral index runs at every open.
var ErrIndexIncomplete = errors.New("scrinium: index incomplete")

// ErrIndexDamaged — the index could not be read at all (ADR-118). The
// damaged file is neither repaired in place nor deleted: recovery populates a
// fresh index and the old one is left for inspection.
var ErrIndexDamaged = errors.New("scrinium: index damaged")
