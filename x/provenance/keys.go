package provenance

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"scrinium.dev/domain"
)

// Key layout of the extension's own tables. Everything about how a graph row is
// spelled lives here, so a change to the layout is one file and the schema
// version bump beside it.
//
// The separator is the NUL byte: it cannot occur in an artifact handle and is
// rejected in a relation kind, so a composite key parses unambiguously and a
// prefix scan cannot spill into a neighbouring key.
const (
	sep = "\x00"

	tableByParent = "byParent" // parent ‖ rel ‖ child → position ‖ outcome
	tableByChild  = "byChild"  // child ‖ position → parent ‖ rel
	tableByRel    = "byRel"    // rel ‖ parent ‖ child → outcome
	tableRecords  = "records"  // child → pkey ‖ outcome ‖ inputs-key ‖ repro
	tableOps      = "ops"      // work identity → result; failure tallies
	tableHeads    = "heads"    // superseded ‖ successor → marker

	opsOK   = "ok"
	opsFail = "fail"

	// mark is a non-empty value for rows that carry no payload: the substrate
	// stores values NOT NULL, so a set-membership row still needs a byte.
	mark = "1"

	// posWidth zero-pads a position so lexicographic key order is numeric
	// order: the edge array is capped at 65535, five digits cover it.
	posWidth = 5
)

// allTables is the set a whole-store assertion or a future migration walks.
func allTables() []string {
	return []string{tableByParent, tableByChild, tableByRel, tableRecords, tableOps, tableHeads}
}

func join(parts ...string) string { return strings.Join(parts, sep) }

func padPos(i int) string {
	s := strconv.Itoa(i)
	if len(s) >= posWidth {
		return s
	}
	return strings.Repeat("0", posWidth-len(s)) + s
}

// cut2 splits a two-component key on the first separator.
func cut2(s string) (string, string, bool) {
	i := strings.Index(s, sep)
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+len(sep):], true
}

// split3 splits a three-component key, reporting false on anything else.
func split3(s string) (a, b, c string, ok bool) {
	parts := strings.Split(s, sep)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// outcomeOf reads the outcome out of a byParent value ("position ‖ outcome").
func outcomeOf(value []byte) Outcome {
	_, out, ok := cut2(string(value))
	if !ok || out == "" {
		return OutcomeOK
	}
	return Outcome(out)
}

func boolStr(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

// inputsKeyOf is the companion of ParamsKey over the edge targets, in order:
// together they answer "this exact work on these exact inputs". Local to the
// index — a caller never needs to spell it, since Done takes the inputs.
func inputsKeyOf(refs []domain.HandleRef) string {
	h := sha256.New()
	for _, r := range refs {
		h.Write([]byte(r))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// splitRecord parses a records row ("pkey ‖ outcome ‖ inputs-key ‖ repro").
func splitRecord(v string) (pkey, outcome, inputsKey string, repro bool) {
	parts := strings.SplitN(v, sep, 4)
	for len(parts) < 4 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2], parts[3] == "1"
}
