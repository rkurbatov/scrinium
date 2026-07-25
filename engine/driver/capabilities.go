package driver

// CapabilityMask is a bitmask of optional driver abilities.
// A driver declares its capabilities statically; the result of
// Capabilities() is stable for the lifetime of the instance.
type CapabilityMask uint16

const (
	// CapSlowRead indicates high latency or monetary cost of reads
	// (S3, Glacier, network shares over WAN). Used to recommend
	// wrapping the Target with a bundler over a transit store.
	CapSlowRead CapabilityMask = 1 << iota

	// CapBlockAlign512 indicates an optimal alignment of 512 bytes
	// (HDDs, classic SSDs).
	CapBlockAlign512

	// CapBlockAlign4096 indicates an optimal alignment of 4096 bytes
	// (NVMe, 4K-sector drives). Declaring this together with
	// CapBlockAlign512 is incorrect.
	CapBlockAlign4096

	// CapWatch indicates support for native new-file notifications
	// (inotify, FSEvents, S3 Event Notifications). Used by the
	// Continuous Watch Ingester.
	CapWatch

	// CapNativeChecksum indicates that the backend verifies data on
	// every read (S3 ETag, Btrfs/ZFS block checksums). Silent bit rot
	// is impossible; the Scrub Agent reduces the rate of explicit
	// verification accordingly.
	CapNativeChecksum

	// CapDirEntries indicates that a directory is an ENTRY on this
	// backend rather than a prefix of a name, so the tree can be walked
	// with its directories. A driver declaring it implements
	// TreeLister (optional.go). Object stores do not declare it: they
	// have prefixes, and an empty prefix does not exist at all.
	//
	// It earns a bit because a configuration's VALIDITY depends on it —
	// capturing directories over a driver without them is an assembly
	// error, and that must be caught before anything is built
	// (ADR-114/115). A capability that is merely used, not planned
	// around, carries no bit; see optional.go.
	CapDirEntries
)

// Has reports whether the given flag is set in the mask.
func (m CapabilityMask) Has(c CapabilityMask) bool {
	return m&c != 0
}
