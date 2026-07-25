// Package localfs implements driver.Driver on top of a local POSIX
// filesystem. It is the reference and default driver for embedded
// scenarios.
//
// Atomicity guarantees:
//   - Put writes to a sibling temp file, fsyncs it, renames into
//     place, and fsyncs the parent directory. A parallel Get either
//     observes the previous content (or os.ErrNotExist) or the new
//     content after Put has fully committed; partial reads are
//     impossible.
//   - Rename and MarkTombstone use the underlying rename(2) atomic
//     contract. Both are safe for concurrent observers.
//   - Clone uses an atomic temp+rename copy. CoW optimisations
//     (clonefile on APFS, ioctl FICLONE on btrfs/xfs) are deferred
//     until they are needed for performance.
//
// Path conventions:
//   - All paths passed to driver methods are relative to the root
//     and use forward-slash separators regardless of OS.
//   - Absolute paths and paths containing ".." segments are
//     rejected with an error.
//
// Capabilities:
//   - CapBlockAlign4096: modern SSDs and NVMe drives use 4 KiB
//     sectors.
//   - CapWatch: declared because fsnotify-based observation works on
//     the local filesystem. The Driver interface itself does not
//     expose Watch — the Ingester (TODO M6.3) consumes fsnotify directly,
//     guided by this flag.
//   - CapDirEntries: on a filesystem a directory is an entry, so the tree
//     can be walked with its directories (ListTree). That is what makes an
//     empty directory capturable at all: it yields no object path, and an
//     object store has no such entry to report.
//
// Optional interfaces implemented without a mask bit:
//   - driver.ReadinessProber (ReadyToRead): a POSIX backend can tell a
//     finished file from one being appended to, by asking for a
//     non-blocking advisory lock. Advisory means advisory — a writer that
//     takes no lock is invisible — so the consumer keeps its own
//     unconditional guard and treats this as a veto on top.
//
// Not declared:
//   - CapSlowRead: irrelevant for local disks.
//   - CapNativeChecksum: ext4/xfs do not provide block-level
//     checksums. ZFS/btrfs do but we do not detect them at runtime;
//     a future variant of the driver may expose this through an
//     option.
//
// DAG: localfs imports driver (the contract) and the standard
// library. It does not import store or higher layers.
package localfs
