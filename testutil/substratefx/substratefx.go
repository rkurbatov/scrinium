// Package substratefx supplies an in-memory customindex.Substrate for tests
// that exercise a CustomIndex in isolation, without standing up a sqlite
// backend.
//
// It exists because three packages had grown their own copy (the sqlite
// custom-index tests, the bundler, provenance), and the copies had already
// started to drift from the contract they imitate: at least one iterates a Go
// map and therefore returns keys in random order, while Substrate.Scan promises
// lexicographic key order. An index whose keys encode ordering — positions,
// zero-padded counters, path prefixes — passes such a fake by luck and fails on
// the real backend.
//
// The fake models the substrate as seen from INSIDE a backend transaction,
// which is where Setup, Index and Unindex run: Inc works. It does not model
// isolation, rollback, or the read-side handle's refusal to Inc.
package substratefx

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"testing"

	"scrinium.dev/engine/customindex"
)

// Substrate is an in-memory customindex.Substrate. Tables are separate key
// spaces, as they are in the real backend, where they are namespaced by index
// name.
type Substrate struct {
	tables map[string]map[string][]byte
}

// Memory returns an empty substrate. It takes *testing.T for symmetry with the
// other fixtures (and so a future version can register cleanup) though it
// currently needs no teardown.
func Memory(t testing.TB) *Substrate {
	t.Helper()
	return &Substrate{tables: map[string]map[string][]byte{}}
}

func (s *Substrate) Put(table, key string, value []byte) error {
	t, ok := s.tables[table]
	if !ok {
		t = map[string][]byte{}
		s.tables[table] = t
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	t[key] = cp
	return nil
}

func (s *Substrate) Get(table, key string) ([]byte, bool, error) {
	v, ok := s.tables[table][key]
	return v, ok, nil
}

func (s *Substrate) Delete(table, key string) error {
	delete(s.tables[table], key)
	return nil
}

func (s *Substrate) DeletePrefix(table, prefix string) error {
	if prefix == "" {
		return customindex.ErrEmptyPrefix
	}
	for k := range s.tables[table] {
		if strings.HasPrefix(k, prefix) {
			delete(s.tables[table], k)
		}
	}
	return nil
}

// Scan iterates matching keys in lexicographic order, as the contract requires.
// ErrStopScan from the callback ends the walk without an error; any other error
// propagates.
func (s *Substrate) Scan(table, prefix string, cb func(key string, value []byte) error) error {
	keys := make([]string, 0, len(s.tables[table]))
	for k := range s.tables[table] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := cb(k, s.tables[table][k]); err != nil {
			if errors.Is(err, customindex.ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Inc models the in-transaction accumulator: an int64 counter, big-endian.
func (s *Substrate) Inc(table, key string, delta int64) (int64, error) {
	var cur int64
	if v, ok := s.tables[table][key]; ok && len(v) == 8 {
		cur = int64(binary.BigEndian.Uint64(v))
	}
	cur += delta
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(cur))
	return cur, s.Put(table, key, b)
}

// --- assertion helpers ---

// Rows reports the number of rows in a table (0 for an unknown table).
func (s *Substrate) Rows(table string) int { return len(s.tables[table]) }

// Tables lists the tables that hold at least one row, sorted.
func (s *Substrate) Tables() []string {
	out := make([]string, 0, len(s.tables))
	for t, rows := range s.tables {
		if len(rows) > 0 {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// Each visits every row of every table, sorted by table then key. Useful for
// whole-store invariants ("no row carries an empty value").
func (s *Substrate) Each(fn func(table, key string, value []byte)) {
	tables := make([]string, 0, len(s.tables))
	for t := range s.tables {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		keys := make([]string, 0, len(s.tables[t]))
		for k := range s.tables[t] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fn(t, k, s.tables[t][k])
		}
	}
}

var _ customindex.Substrate = (*Substrate)(nil)
