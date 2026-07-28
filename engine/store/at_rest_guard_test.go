package store

import (
	"errors"
	"testing"

	"scrinium.dev/config"
	"scrinium.dev/engine/index"
	"scrinium.dev/errs"
)

// stubIndex is a bare index.StoreIndex placeholder: the guard only ever
// type-asserts for the reporter capability, so no method needs a body.
type stubIndex struct{ index.StoreIndex }

// reportingIndex answers the at-rest question with a fixed profile.
type reportingIndex struct {
	index.StoreIndex
	profile index.AtRest
}

func (r reportingIndex) AtRest() index.AtRest { return r.profile }

func TestGuardIndexAtRest(t *testing.T) {
	cases := []struct {
		name    string
		crypto  config.ManifestCrypto
		idx     index.StoreIndex
		wantErr bool
	}{
		{"plain mode ignores profile", config.ManifestCryptoPlain, stubIndex{}, false},
		{"sealed mode ignores profile", config.ManifestCryptoSealed, stubIndex{}, false},
		{"paranoid rejects silent index", config.ManifestCryptoParanoid, stubIndex{}, true},
		{"paranoid rejects plaintext", config.ManifestCryptoParanoid, reportingIndex{profile: index.AtRestPlaintext}, true},
		{"paranoid accepts ephemeral", config.ManifestCryptoParanoid, reportingIndex{profile: index.AtRestEphemeral}, false},
		{"paranoid accepts encrypted", config.ManifestCryptoParanoid, reportingIndex{profile: index.AtRestEncrypted}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := guardIndexAtRest(tc.crypto, tc.idx)
			if tc.wantErr {
				if !errors.Is(err, errs.ErrUnsupportedCombination) {
					t.Fatalf("got %v, want ErrUnsupportedCombination", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}
