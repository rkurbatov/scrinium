package driver

// optional.go — contracts a Driver MAY implement beyond the mandatory
// interface in driver.go.
//
// Two rules govern this file. A contract lands here rather than in Driver when
// some backends cannot honestly honour it: put in the mandatory interface, it
// would become a stub returning "unsupported" on half of them, which is a false
// promise dressed as a method. And a contract earns a bit in CapabilityMask
// only when a configuration's VALIDITY depends on it — something the assembler
// must check before building anything. Everything else is taken by type
// assertion at the point of use, the way index hosting, listing, the pack
// resolver and index synchronisation already are.

import (
	"context"
	"time"
)

// ReadinessProber answers whether a path can be read in full right now — that
// is, tells a finished file from one being appended to this very second. It is
// named by intent rather than by mechanism: a POSIX driver answers with a
// non-blocking advisory lock, another backend might use a different signal.
//
// No mask bit: nothing plans around it, it is simply asked when present.
//
// Its consumer (the ingester, ADR-115) treats it as a VETO on top of an
// unconditional settle window, never as a replacement for it. Advisory locks
// are advisory: a writer that takes none is invisible to the probe, and most
// writers take none — the common uploader writes to a temporary name and
// renames. So the probe may postpone an element earlier and more precisely than
// the window, but it may not release one the window still holds.
//
// A driver whose locks exist but cannot be trusted (network shares) simply does
// not implement this, and loses nothing.
type ReadinessProber interface {
	ReadyToRead(ctx context.Context, path string) (ok bool, err error)
}

// TreeEntry is one entry of a tree walk: a file or a directory, with the mode
// bits the backend reports. Mode carries the POSIX type bits, so a directory is
// recognisable by them as well as by IsDir. It is the ObjectMeta of a walk that
// knows about directories.
type TreeEntry struct {
	Path         string
	Size         int64
	LastModified time.Time
	Mode         uint32
	IsDir        bool
}

// TreeLister is the contract behind CapDirEntries: a walk that reports
// directories as entries. The mandatory ListObjectsWithModTime enumerates
// objects and omits directories, which is correct for an object store and
// insufficient for a faithful tree capture — an empty directory yields no object
// path, and empty directories are precisely what a tree restore or a snapshot
// would otherwise lose.
//
// This one does carry a mask bit, because capturing directories over a driver
// without it is a configuration error rather than a missing optimisation.
//
// Early termination is fs.SkipAll, the same idiom as ListObjectsWithModTime.
type TreeLister interface {
	ListTree(ctx context.Context, prefix string, since time.Time, cb func(TreeEntry) error) error
}
