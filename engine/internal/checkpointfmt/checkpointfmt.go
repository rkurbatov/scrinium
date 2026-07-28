// Package checkpointfmt is the single source of truth for where index
// checkpoints live on the CAS and how their names encode time. Both the
// checkpoint agent (producer) and the rebuild agent (consumer) depend on it,
// so the two can never drift on the prefix or the timestamp layout.
//
// A checkpoint is a full, self-contained copy of the StoreIndex published as
// engine state under Prefix, in the name-addressed space that is never
// indexed (ADR-85). Recovery lists the prefix, picks the newest, restores
// it, and replays the tail of manifests written since.
package checkpointfmt

import (
	"context"
	"fmt"
	"strings"
	"time"

	"scrinium.dev/domain"
)

// Prefix is the System() namespace every checkpoint artifact lives under.
// It is a flat, dot-separated key prefix (not a directory): a checkpoint
// is named "store.agent.checkpoint.<ts>" and the set is enumerated by a
// string-prefix Walk over the named root.
const Prefix = "store.agent.checkpoint."

// TimeLayout encodes the checkpoint instant. It is path-safe and
// lexicographically sortable (no colons, fixed-width nanoseconds, trailing
// Z) so a plain string sort of names is a chronological sort: retention
// drops the smallest names, recovery picks the largest.
const TimeLayout = "20060102T150405.000000000Z"

// ID formats the timestamp portion of a checkpoint name (no prefix). The
// instant is normalized to UTC first so names are comparable regardless of
// the producer's local zone.
func ID(t time.Time) string { return t.UTC().Format(TimeLayout) }

// Name is the full checkpoint artifact name: Prefix + ID(t).
func Name(t time.Time) string { return Prefix + ID(t) }

// ParseID extracts the checkpoint instant from a full name. It errors when
// the name is not under Prefix or its timestamp does not parse.
func ParseID(name string) (time.Time, error) {
	if !strings.HasPrefix(name, Prefix) {
		return time.Time{}, fmt.Errorf("checkpointfmt: name %q is not under %q", name, Prefix)
	}
	id := strings.TrimPrefix(name, Prefix)
	t, err := time.Parse(TimeLayout, id)
	if err != nil {
		return time.Time{}, fmt.Errorf("checkpointfmt: parse %q: %w", name, err)
	}
	return t, nil
}

// Walker is the minimal System()-store surface Latest needs; the Store's
// SystemStore satisfies it structurally.
type Walker interface {
	Walk(ctx context.Context, prefix string, cb func(name string, m domain.Manifest) error) error
}

// Latest returns the newest checkpoint under Prefix. ok is false with a nil
// error when none exists. Names that do not parse are skipped rather than
// failing the scan — a foreign object dropped under the prefix must not be
// able to block recovery.
func Latest(ctx context.Context, w Walker) (name string, createdAt time.Time, ok bool, err error) {
	var bestName string
	var bestTime time.Time
	walkErr := w.Walk(ctx, Prefix, func(n string, _ domain.Manifest) error {
		t, perr := ParseID(n)
		if perr != nil {
			return nil
		}
		if bestName == "" || n > bestName {
			bestName, bestTime = n, t
		}
		return nil
	})
	if walkErr != nil {
		return "", time.Time{}, false, fmt.Errorf("checkpointfmt: list %q: %w", Prefix, walkErr)
	}
	if bestName == "" {
		return "", time.Time{}, false, nil
	}
	return bestName, bestTime, true, nil
}
