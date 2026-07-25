package vfsmeta

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/domain/extpocket"
	"scrinium.dev/errs"
)

// Key is this schema's key in the Manifest.Ext map. Manifest.Ext is a
// JSON object keyed by extension/schema name — each extension owns its
// own key. The key is the schema discriminator: an artifact carries
// filesystem metadata iff Ext["vfsmeta"] is present. Other schemas
// (namespace's "nsid", a host's "email"/"archive") live under their own
// keys in the same object and coexist with this one.
const Key = "vfsmeta"

// Version is the current vfsmeta schema version. Encode writes it into
// the payload; Decode treats a payload of any other version as "not
// this schema's v1" (ok=false) so a future v2 decoder can claim it.
const Version = 1

// FileSystem is the parsed payload of Manifest.Ext["vfsmeta"] — the
// filesystem schema. Only Path is mandatory; the rest fall back to
// FSOps defaults (or Manifest.CreatedAt for ModTime) when zero.
type FileSystem struct {
	// Path is the virtual path of the artifact, slash-separated,
	// no leading slash, no "..", no NUL bytes, no empty segments.
	// Example: "photos/2024/01/sunrise.jpg".
	Path string

	// Mode are POSIX mode bits. Zero means "use default".
	Mode uint32

	// UID is the POSIX user id. Zero means "use default".
	UID uint32

	// GID is the POSIX group id. Zero means "use default".
	GID uint32

	// ModTime is the POSIX mtime. Zero means "use Manifest.CreatedAt".
	ModTime time.Time

	// MIME is the artifact's MIME type. Empty means
	// "application/octet-stream" or transport-side detection.
	MIME string
}

// wireFormat is the on-disk shape of the vfsmeta payload (the value
// under Ext["vfsmeta"]). version is the schema-version discriminator;
// mtime is a pointer so encoding/json's omitempty drops it when zero
// (a zero time.Time is not "empty" to the standard marshaller).
type wireFormat struct {
	Version int        `json:"version"`
	Path    string     `json:"path"`
	Mode    uint32     `json:"mode,omitempty"`
	UID     uint32     `json:"uid,omitempty"`
	GID     uint32     `json:"gid,omitempty"`
	ModTime *time.Time `json:"mtime,omitempty"`
	MIME    string     `json:"mime,omitempty"`
}

func (fs FileSystem) toWire() wireFormat {
	w := wireFormat{
		Version: Version,
		Path:    fs.Path,
		Mode:    fs.Mode,
		UID:     fs.UID,
		GID:     fs.GID,
		MIME:    fs.MIME,
	}
	if !fs.ModTime.IsZero() {
		t := fs.ModTime
		w.ModTime = &t
	}
	return w
}

func (w wireFormat) toFileSystem() FileSystem {
	fs := FileSystem{
		Path: w.Path,
		Mode: w.Mode,
		UID:  w.UID,
		GID:  w.GID,
		MIME: w.MIME,
	}
	if w.ModTime != nil {
		fs.ModTime = *w.ModTime
	}
	return fs
}

// encodeValue validates a FileSystem and serialises it as the vfsmeta
// payload value (the object stored under Ext["vfsmeta"]). The schema
// version is filled in automatically. Callers place it into a
// Manifest.Ext map via Embed.
//
// Returns an error wrapping errs.ErrInvalidPath when Path fails
// validation.
func encodeValue(fs FileSystem) (json.RawMessage, error) {
	if err := ValidatePath(fs.Path); err != nil {
		return nil, err
	}
	out, err := json.Marshal(fs.toWire())
	if err != nil {
		// Marshalling a struct of basic types should not fail; if
		// it does, the failure is a programming error in the
		// schema definition rather than runtime data.
		return nil, fmt.Errorf("vfsmeta.Embed: %w", err)
	}
	return out, nil
}

// Embed merges the vfsmeta payload into an existing Manifest.Ext map
// under Key, preserving any other schemas' keys. ext may be nil/empty
// (a fresh map is created). The returned bytes are the full Ext object.
//
// Returns an error wrapping errs.ErrInvalidPath when Path fails
// validation, or an error if ext is non-empty but not a JSON object.
func Embed(ext json.RawMessage, fs FileSystem) (json.RawMessage, error) {
	payload, err := encodeValue(fs)
	if err != nil {
		return nil, err
	}
	out, err := extpocket.Put(ext, Key, payload)
	if err != nil {
		return nil, fmt.Errorf("vfsmeta.Embed: %w", err)
	}
	return out, nil
}

// Decode reads the vfsmeta payload from a Manifest.Ext map. The triple
// return (FileSystem, ok, error) distinguishes three outcomes:
//
//   - (zero, false, nil) — Ext is empty, has no "vfsmeta" key, carries a
//     payload of a different schema version, or is not a JSON object.
//     Common for artifacts written by other schemas; not an error.
//   - (fs, true, nil) — the "vfsmeta" payload is present, current
//     version, and Path is valid. fs is usable.
//   - (zero, false, err) — the "vfsmeta" payload is present and current
//     version but broken (malformed body, invalid Path). The artifact
//     intended this schema and got it wrong; surface the error.
//
// Decode never panics on adversarial input.
func Decode(ext json.RawMessage) (FileSystem, bool, error) {
	if len(ext) == 0 {
		return FileSystem{}, false, nil
	}
	// Pull our key out of the Ext map; ignore foreign keys. A non-object Ext,
	// or no "vfsmeta" key, means "not us" — this schema is deliberately
	// permissive there, so the extpocket error is dropped rather than surfaced.
	raw, ok, _ := extpocket.Get(ext, Key)
	if !ok {
		return FileSystem{}, false, nil
	}
	var w wireFormat
	if err := json.Unmarshal(raw, &w); err != nil {
		return FileSystem{}, false, fmt.Errorf("vfsmeta.Decode: %w", err)
	}
	// A different version is "not this decoder's payload" — let a
	// future v2 decoder claim it rather than erroring here.
	if w.Version != Version {
		return FileSystem{}, false, nil
	}
	fs := w.toFileSystem()
	if err := ValidatePath(fs.Path); err != nil {
		return FileSystem{}, false, err
	}
	return fs, true, nil
}

// Resolver is the source.Resolver for the filesystem schema. It returns
// (Path, true) for artifacts whose metadata decodes cleanly; otherwise
// ("", false).
//
// A malformed payload also returns ("", false) — the artifact ends up in
// the orphaned tree. The decode error is silently swallowed because the
// resolver runs inside the View's hot path and a single bad artifact
// must not fail backfill. Surfacing such errors is the ingester's job at
// write time.
func Resolver(m domain.Manifest) (string, bool) {
	fs, ok, err := Decode(m.Ext)
	if err != nil || !ok {
		return "", false
	}
	return fs.Path, true
}

// --- Path validation ---

// ValidatePath checks the rules for a virtual path:
//
//   - non-empty
//   - no leading slash (the path is relative)
//   - no NUL bytes
//   - no empty segments (which would arise from "//" or a trailing "/")
//   - no "." or ".." segments
//
// Returns an error wrapping errs.ErrInvalidPath when any rule is broken.
// All checks performed; the error message names the first failed rule.
//
// Reserved-root checking is a separate concern because the reserved name
// (FUSE service prefix) is configurable by the transport. Use
// ValidatePathWithReserved to combine.
func ValidatePath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: empty path", errs.ErrInvalidPath)
	}
	if strings.ContainsRune(p, 0) {
		return fmt.Errorf("%w: path contains NUL", errs.ErrInvalidPath)
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("%w: path starts with /", errs.ErrInvalidPath)
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "":
			return fmt.Errorf("%w: empty segment in %q", errs.ErrInvalidPath, p)
		case ".":
			return fmt.Errorf("%w: %q contains . segment", errs.ErrInvalidPath, p)
		case "..":
			return fmt.Errorf("%w: %q contains .. segment", errs.ErrInvalidPath, p)
		}
	}
	return nil
}

// ValidatePathWithReserved performs ValidatePath plus a check that the
// first path segment does not equal the reserved name. The reserved name
// is the FUSE service prefix (default "_scrinium"), passed by the
// transport.
//
// An empty reserved string disables the reserved-root check —
// equivalent to ValidatePath.
func ValidatePathWithReserved(p, reserved string) error {
	if err := ValidatePath(p); err != nil {
		return err
	}
	if reserved == "" {
		return nil
	}
	first := p
	if i := strings.IndexByte(p, '/'); i >= 0 {
		first = p[:i]
	}
	if first == reserved {
		return fmt.Errorf("%w: first segment %q is reserved", errs.ErrInvalidPath, reserved)
	}
	return nil
}
