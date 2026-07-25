package layout

import (
	"errors"
	"fmt"
	"strings"

	"scrinium.dev/config"
	"scrinium.dev/domain"
)

func rootFor(t domain.BlobType) (string, error) {
	switch t {
	case "", domain.BlobTypeRegular:
		return "blobs", nil
	case domain.BlobTypeChunk:
		return "chunks", nil
	case domain.BlobTypePack:
		return "packs", nil
	}
	return "", fmt.Errorf("artifact: unknown blob type %q", t)
}

// shardOf extracts the two two-character shards from a bare-hex ref
// (ADR-93: no "<algo>-" prefix). Hex is case-folded to lowercase so the
// on-disk layout stays stable across hashers that might emit uppercase. A
// ref shorter than four chars errors: such a ref could only come from a
// misconfigured HashRegistry, and short shard names are collision-prone.
func shardOf(ref string) (string, string, error) {
	low := strings.ToLower(ref)
	if len(low) < 4 {
		return "", "", fmt.Errorf("artifact: ref too short to shard: %q", ref)
	}
	return low[0:2], low[2:4], nil
}

// BlobPath returns the driver-side path for a blob with the given ref
// under the given topology and type. The result is forward-slash
// separated and root-relative — exactly what driver.Driver expects.
//
// An empty topology is treated as Sharded (the InitStore default), and an
// empty BlobType as Regular.
func BlobPath(topology config.PathTopology, blobType domain.BlobType, ref string) (string, error) {
	if ref == "" {
		return "", errors.New("artifact: empty ref")
	}
	root, err := rootFor(blobType)
	if err != nil {
		return "", err
	}
	switch topology {
	case config.PathTopologyFlat:
		return root + "/" + ref, nil
	case "", config.PathTopologySharded:
		s1, s2, err := shardOf(ref)
		if err != nil {
			return "", err
		}
		return root + "/" + s1 + "/" + s2 + "/" + ref, nil
	}
	return "", fmt.Errorf("artifact: unknown topology %q", topology)
}

// ManifestPath returns the driver-side path of a manifest file by its
// ManifestDigest. Manifests live under "manifests/" and are always
// Sharded; there is no Flat manifest layout. The path is keyed by digest
// (the current physical form), not by the floating ArtifactID.
func ManifestPath(digest domain.ManifestDigest) (string, error) {
	if digest == "" {
		return "", errors.New("artifact: empty manifest digest")
	}
	s1, s2, err := shardOf(string(digest))
	if err != nil {
		return "", fmt.Errorf("artifact: manifest %w", err)
	}
	return "manifests/" + s1 + "/" + s2 + "/" + string(digest), nil
}

// RefFromBlobPath extracts the blob ref (bare hex per ADR-93, no "<algo>-"
// prefix) from a driver-side blob path produced by BlobPath. Both topologies
// are supported (the ref is always the last path segment). The check is
// purely structural — last-segment is bare hex of at least four chars. It
// does NOT cross-check the segment against the topology shards; a caller that
// needs that paranoia re-runs BlobPath on the result and compares. Used by the
// Orphan Scan to map a driver-listed file back to a StoreIndex key.
func RefFromBlobPath(p string) (string, error) {
	last, err := lastSegment(p)
	if err != nil {
		return "", err
	}
	if err := validateRefShape(last); err != nil {
		return "", fmt.Errorf("artifact: %w (path %q)", err, p)
	}
	return last, nil
}

// DigestFromManifestPath is the manifests-side counterpart of
// RefFromBlobPath; manifest paths are always Sharded and the structural
// validation is identical. It returns the ManifestDigest (the file's
// physical name), not the handle.
func DigestFromManifestPath(p string) (domain.ManifestDigest, error) {
	last, err := lastSegment(p)
	if err != nil {
		return "", err
	}
	if err := validateRefShape(last); err != nil {
		return "", fmt.Errorf("artifact: manifest %w (path %q)", err, p)
	}
	return domain.ManifestDigest(last), nil
}

// lastSegment returns the final '/'-separated component of p, erroring on
// an empty path.
func lastSegment(p string) (string, error) {
	if p == "" {
		return "", errors.New("artifact: empty path")
	}
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:], nil
	}
	return p, nil
}

// validateRefShape checks the bare-hex structural form (ADR-93: no
// "<algo>-" prefix) — at least four chars, all lowercase hex.
func validateRefShape(s string) error {
	if len(s) < 4 {
		return fmt.Errorf("ref %q shorter than 4 chars", s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return fmt.Errorf("ref %q has non-hex char at position %d", s, i)
		}
	}
	return nil
}

// BlobRoots returns every driver-side root that holds blob payload, one per
// blob type. It serves a sweep that must cover ALL physical payload rather
// than address a single blob — a purging re-initialisation is the one caller —
// and it derives the names from rootFor so such a sweep cannot drift from what
// BlobPath writes.
func BlobRoots() []string {
	types := []domain.BlobType{
		domain.BlobTypeRegular,
		domain.BlobTypeChunk,
		domain.BlobTypePack,
	}
	out := make([]string, 0, len(types))
	for _, t := range types {
		root, err := rootFor(t)
		if err != nil {
			// Unreachable: every constant above is one rootFor knows. If a new
			// blob type ever arrives without a root, skipping is the safe half
			// of the bug — a caller sweeps less than it meant to, never
			// something it never meant to touch.
			continue
		}
		out = append(out, root)
	}
	return out
}
