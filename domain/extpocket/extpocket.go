// Package extpocket reads and writes one schema's value inside Manifest.Ext.
//
// Ext is a JSON object keyed by schema name: each extension owns its key and
// must leave the neighbours' keys alone. Every schema had grown its own copy of
// the same twelve lines — unmarshal the object, set or read one key, marshal it
// back — and the copies had begun to differ in how they treat an Ext that is not
// an object at all. One place, one behaviour.
//
// The package is deliberately tiny and knows nothing about any schema: it moves
// a raw JSON value in and out of a named slot.
package extpocket

import (
	"encoding/json"
	"fmt"
)

// Put stores value under key in ext, preserving every other key already there.
// An empty ext becomes a fresh object carrying just this key. The result is the
// whole Ext object, ready for domain.Artifact.Ext.
//
// It fails when ext is non-empty and is not a JSON object: writing into it would
// silently discard whatever was there.
func Put(ext json.RawMessage, key string, value json.RawMessage) (json.RawMessage, error) {
	obj := map[string]json.RawMessage{}
	if len(ext) > 0 {
		if err := json.Unmarshal(ext, &obj); err != nil {
			return nil, fmt.Errorf("extpocket: Ext is not a JSON object: %w", err)
		}
	}
	obj[key] = value
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("extpocket: encode Ext: %w", err)
	}
	return out, nil
}

// Get reads the value stored under key. The triple return separates three
// outcomes a schema decoder needs to tell apart:
//
//   - (nil, false, nil) — ext is empty or has no such key. The ordinary case for
//     artifacts written by other schemas; not an error.
//   - (value, true, nil) — the key is present; the value is this schema's to
//     decode.
//   - (nil, false, err) — ext is not a JSON object. Whether that is worth
//     surfacing is the caller's call: a strict schema reports it, a permissive
//     one treats it as "not us".
func Get(ext json.RawMessage, key string) (json.RawMessage, bool, error) {
	if len(ext) == 0 {
		return nil, false, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(ext, &obj); err != nil {
		return nil, false, fmt.Errorf("extpocket: Ext is not a JSON object: %w", err)
	}
	raw, ok := obj[key]
	if !ok || len(raw) == 0 {
		return nil, false, nil
	}
	return raw, true, nil
}
