package sqlite

// Key representation at the SQL boundary.
//
// A digest, a content hash and a blob ref are lowercase hex in the domain:
// a manifest names itself that way, and the name has to stay readable and
// self-describing (Principle 4). The index owes nobody that: it is a derived
// cache, free to store the same value in the form that costs least. Raw
// bytes cost half — 32 instead of 64 — and every index over such a column
// copies half as much, which on a large store is the difference between one
// gigabyte of pages and two.
//
// So the conversion happens here, at the one boundary where the two forms
// meet: hexKey on the way into a statement, hexOut/hexOutNull on the way
// out. Doing it in the domain types instead would push an index's storage
// choice into the manifests; doing it inline at each call site would scatter
// the same decode across every query.
//
// Which columns: manifest_digest, blob_ref, content_hash, handle_ref — the
// ones that are bare hex. NOT artifact_id (it is "<algo>-<hex>", so it
// carries a prefix), not session_id, not physical_path.

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
)

// hexKey binds a hex-encoded domain key as raw bytes.
//
// An empty string binds as NULL, which is what a manifest without a blob
// (Inline) needs. A value that is not valid hex is a programming error at
// the call site — the domain never produces one — so it surfaces as an
// error from the statement rather than being silently stored.
type hexKey string

func (k hexKey) Value() (driver.Value, error) {
	if k == "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(string(k))
	if err != nil {
		return nil, fmt.Errorf("sqlite: key %q is not hex: %w", string(k), err)
	}
	return raw, nil
}

var _ driver.Valuer = hexKey("")

// hexOut scans a raw-bytes key column back into its hex form.
//
// It accepts TEXT as well: a value written by an older build is read back
// unchanged rather than mangled. That tolerance is one-directional — nothing
// here ever WRITES text — so a store converges on the binary form as its
// rows are rewritten.
type hexOut struct{ dst *string }

func (h hexOut) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*h.dst = ""
	case []byte:
		*h.dst = hex.EncodeToString(v)
	case string:
		*h.dst = v
	default:
		return fmt.Errorf("sqlite: cannot scan %T into a key", src)
	}
	return nil
}

var _ sql.Scanner = hexOut{}

// hexOutNull is hexOut for a nullable column; it reports whether the column
// held a value. blob_ref on an Inline manifest is the case that needs it.
type hexOutNull struct {
	dst   *string
	valid *bool
}

func (h hexOutNull) Scan(src any) error {
	if src == nil {
		*h.dst = ""
		if h.valid != nil {
			*h.valid = false
		}
		return nil
	}
	if h.valid != nil {
		*h.valid = true
	}
	return hexOut{dst: h.dst}.Scan(src)
}

var _ sql.Scanner = hexOutNull{}
