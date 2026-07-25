package provenance

import (
	"encoding/json"
	"fmt"

	"scrinium.dev/present"
)

// presentedSchemas is the SchemaPresenter body (ADR-109). It is shared by the
// extension's parts: the owner of an Ext schema presents it, and which axis
// object carries the capability is a wiring detail.
func presentedSchemas() []present.Schema {
	return []present.Schema{{Key: Key, Present: presentBlock}}
}

// presentBlock lays out a production record for a surface. Sources are
// presented as refs so a surface can link them — walking up a derivation chain
// by clicking is the whole point of showing provenance at all. ok=false when
// the artifact carries no record (an origin) or a version this build does not
// know, so the surface falls back to raw JSON.
//
// Edge kinds are shown positionally against the manifest's own edge array,
// which the surface holds; the record itself only knows the kinds, since the
// targets live in the core half of the record.
func presentBlock(ext json.RawMessage) (present.Representation, bool, error) {
	b, ok, err := Decode(ext)
	if err != nil {
		return present.Representation{}, false, fmt.Errorf("provenance.Decode: %w", err)
	}
	if !ok {
		return present.Representation{}, false, nil
	}

	fields := make([]present.Field, 0, 6)
	if b.Op != "" {
		fields = append(fields, present.Field{Label: "Operation", Value: b.Op, Kind: present.Text})
	}
	for i, rel := range b.Rel {
		fields = append(fields, present.Field{
			Label: fmt.Sprintf("Input %d", i),
			Value: rel,
			Kind:  present.Ref,
		})
	}
	if len(b.Params) > 0 {
		fields = append(fields, present.Field{
			Label: "Parameters",
			Value: string(b.Params),
			Kind:  present.Text,
		})
	}
	fields = append(fields,
		present.Field{Label: "Reproducible", Value: yesNo(b.Repro), Kind: present.Text},
		present.Field{Label: "Outcome", Value: string(b.Outcome), Kind: present.Text},
	)
	if b.Seq != 0 {
		fields = append(fields, present.Field{
			Label: "Sequence",
			Value: fmt.Sprintf("%d", b.Seq),
			Kind:  present.Number,
		})
	}
	if b.PKey != "" {
		fields = append(fields, present.Field{Label: "Work key", Value: b.PKey, Kind: present.Text})
	}

	return present.Representation{Title: "Provenance", Fields: fields}, true, nil
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}
