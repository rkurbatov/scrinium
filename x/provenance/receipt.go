package provenance

import (
	"encoding/json"
	"fmt"
	"time"

	"scrinium.dev/domain"
)

// DefaultEvictRel is the relation kind of a receipt's edge to the artifact it
// explains the disappearance of. It is deliberately NOT the supersede kind:
// supersession answers "what is current now", eviction answers "where did the
// bytes go". Merging them would make a receipt for a deletion the head of the
// artifact's currency chain.
const DefaultEvictRel = "evicted"

// EvictOp is the operation name a receipt records.
const EvictOp = "evict"

// ReceiptVersion is the current version of the receipt document.
const ReceiptVersion = 1

// EvictedArtifact is what the receipt remembers about the artifact whose bytes
// are gone — enough to recognise it if it ever turns up again, and enough to
// explain a dangling reference to someone reading the graph a year later.
type EvictedArtifact struct {
	Artifact    domain.ArtifactID  `json:"artifact"`
	ContentHash domain.ContentHash `json:"content_hash,omitempty"`
	Size        int64              `json:"size,omitempty"`
	MIME        string             `json:"mime,omitempty"`
	Path        string             `json:"path,omitempty"`
	CreatedAt   time.Time          `json:"created_at,omitempty"`
}

// Receipt is the document an eviction leaves behind. It lives in the PAYLOAD of
// the receipt artifact, not in the Ext pocket: it is free-form and grows (the
// retained list can be long), while the pocket is capped and meant for what
// gets indexed. What gets indexed is the edge, and for the guard that is
// enough.
type Receipt struct {
	V int `json:"v"`

	// Evicted describes the artifact that was removed.
	Evicted EvictedArtifact `json:"evicted"`

	// Retained lists what was kept and is the reason the eviction was
	// acceptable — the derivatives that now carry the value the source used to.
	Retained []domain.ArtifactID `json:"retained,omitempty"`

	// Reason is a short machine-ish tag ("ocr-complete", "unpacked").
	Reason string `json:"reason,omitempty"`

	// Rule names the selection rule when the eviction was part of a sweep, so a
	// mass decision stays attributable to the policy that made it.
	Rule string `json:"rule,omitempty"`

	// DecidedBy and DecidedAt record who decided and when. A deletion that
	// cannot be attributed is indistinguishable from data loss.
	DecidedBy string    `json:"decided_by,omitempty"`
	DecidedAt time.Time `json:"decided_at,omitempty"`
}

// ReceiptSpec is what a caller supplies to evict an artifact. The evicted
// artifact's own description is filled in by the evictor from its manifest —
// the caller cannot get it wrong, and it cannot be forged by accident.
type ReceiptSpec struct {
	// Retained is what the caller keeps instead of the evicted artifact.
	Retained []domain.ArtifactID

	// Reason is required: an eviction without a stated reason is exactly the
	// silent hole this whole mechanism exists to prevent.
	Reason string

	// Rule is optional — the selection rule of a mass eviction.
	Rule string

	// DecidedBy is required: who decided.
	DecidedBy string

	// DecidedAt defaults to now when zero.
	DecidedAt time.Time
}

func (s ReceiptSpec) validate() error {
	if s.Reason == "" {
		return fmt.Errorf("%w: no reason stated", ErrBadReceipt)
	}
	if s.DecidedBy == "" {
		return fmt.Errorf("%w: no decider stated", ErrBadReceipt)
	}
	return nil
}

// receiptFor builds the document from the spec and the evicted artifact's own
// manifest. Path and MIME come from the filesystem schema when the artifact
// carried one; their absence is not an error — not every artifact has a path.
func receiptFor(spec ReceiptSpec, m domain.Manifest) Receipt {
	at := spec.DecidedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	ev := EvictedArtifact{
		Artifact:    m.ArtifactID,
		ContentHash: m.ContentHash,
		Size:        m.OriginalSize,
		CreatedAt:   m.CreatedAt,
	}
	ev.Path, ev.MIME = pathAndMIME(m.Ext)

	return Receipt{
		V:         ReceiptVersion,
		Evicted:   ev,
		Retained:  spec.Retained,
		Reason:    spec.Reason,
		Rule:      spec.Rule,
		DecidedBy: spec.DecidedBy,
		DecidedAt: at,
	}
}

// pathAndMIME reads the filesystem schema's path and MIME out of Ext without
// importing the schema package: the receipt is a diagnostic document, and a
// missing or foreign-shaped block simply yields empty strings.
func pathAndMIME(ext json.RawMessage) (string, string) {
	if len(ext) == 0 {
		return "", ""
	}
	var probe struct {
		VFS struct {
			Path string `json:"path"`
			MIME string `json:"mime"`
		} `json:"vfsmeta"`
	}
	if err := json.Unmarshal(ext, &probe); err != nil {
		return "", ""
	}
	return probe.VFS.Path, probe.VFS.MIME
}

// DecodeReceipt parses a receipt document read from a receipt artifact's
// payload. A document of an unknown version is refused rather than guessed at:
// a half-understood explanation is worse than an explicit "cannot read this".
func DecodeReceipt(payload []byte) (Receipt, error) {
	var r Receipt
	if err := json.Unmarshal(payload, &r); err != nil {
		return Receipt{}, fmt.Errorf("%w: undecodable document: %v", ErrBadReceipt, err)
	}
	if r.V != ReceiptVersion {
		return Receipt{}, fmt.Errorf("%w: unsupported version %d", ErrBadReceipt, r.V)
	}
	if r.Evicted.Artifact == "" {
		return Receipt{}, fmt.Errorf("%w: names no evicted artifact", ErrBadReceipt)
	}
	return r, nil
}

// encode serialises the document for the receipt artifact's payload.
func (r Receipt) encode() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadReceipt, err)
	}
	return b, nil
}
