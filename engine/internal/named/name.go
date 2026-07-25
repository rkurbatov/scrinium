package named

// name.go — the system-artifact name contract, shared by both layouts
// (versioned files in version.go, cells in cell.go). A name is the logical
// address; the layout files turn a validated name into driver paths under a
// single planar root.

import (
	"fmt"
	"strings"

	"scrinium.dev/errs"
)

// root is the single driver-side root for all system artifacts. The name's
// first segment ("config", "scrub", "index_checkpoint", ...) categorises the
// artifact (ADR-85: category is a name prefix, not a separate namespace):
// everything is a flat
// file directly under this one root: "named/<name>.<seq>" for a version,
// "named/<name>.cell" for a keep=0 cell. No per-artifact subdirectory; the
// layout is planar.
const (
	root      = "named"
	rootSlash = "named/"
)

// Root is that root, exported for engine code that has to sweep the WHOLE
// system plane rather than address one artifact by name. A purging
// re-initialisation is the only such caller today: it removes every system
// artifact — descriptor replicas, config versions, agent cursors, checkpoint
// pointers — and spelling the literal "named" a second time over there is how
// the two would silently drift apart.
const Root = root

// ValidateName enforces the name contract. Names are slash-separated,
// path-like strings: non-empty, no leading or trailing slash, no empty
// segments, no "." or ".." traversal segments. The first segment
// categorises the artifact; subsequent segments are caller-defined. A
// validated name cannot escape the system root.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", errs.ErrInvalidSystemName)
	}
	if name[0] == '/' || name[len(name)-1] == '/' {
		return fmt.Errorf("%w: %q has leading or trailing slash", errs.ErrInvalidSystemName, name)
	}
	if strings.Contains(name, "//") {
		return fmt.Errorf("%w: %q has empty segment", errs.ErrInvalidSystemName, name)
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("%w: %q has traversal segment", errs.ErrInvalidSystemName, name)
		}
	}
	return nil
}
