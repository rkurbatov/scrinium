package indexsuite

import (
	"context"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/customindex"
	"scrinium.dev/engine/index"
)

// Fixture identifiers for the projection-query suite. The Index method
// below emits exactly these rows, so the QueryByExtField / ListByExtField
// cases have deterministic data to find. The extName in
// a query is the index Name (the ext_name column), independent of the
// projected field.
const (
	fixtureName     = "indexsuite.fixture"
	fixtureExtField = "color"
	fixtureExtValue = "red"
)

// fixtureIndexer is a deterministic CustomIndex used only by the
// projection-query conformance cases. It keeps no own tables: on Index it
// returns one ext projection (written into proj_ext) with a fixed
// field/value so the query cases can assert against known rows. Setup,
// Apply and Unindex are no-ops; Subscribe is empty because the Indexer
// capability is driven by IndexManifest, not by event subscriptions.
type fixtureIndexer struct{}

func (fixtureIndexer) Name() string { return fixtureName }

func (fixtureIndexer) SchemaVersion() int { return 1 }

func (fixtureIndexer) Subscribe() []customindex.EventKind { return nil }

func (fixtureIndexer) Setup(context.Context, customindex.Substrate, int) error { return nil }

func (fixtureIndexer) Apply(context.Context, customindex.Substrate, customindex.EventKind, customindex.EventArgs) error {
	return nil
}

func (fixtureIndexer) Close() error { return nil }

// Index emits one ext equality projection. It ignores the
// manifest — the fixed values are what the query cases look up. The core
// stamps the manifest digest and ext_name (= Name()) onto each row.
func (fixtureIndexer) Index(context.Context, customindex.Substrate, domain.Manifest) ([]customindex.Projection, error) {
	return []customindex.Projection{
		{Field: fixtureExtField, Value: fixtureExtValue},
	}, nil
}

func (fixtureIndexer) Unindex(context.Context, customindex.Substrate, domain.Manifest) error {
	return nil
}

// Compile-time proof the fixture satisfies both the base contract and the
// optional write-side capability — a signature drift in either interface
// breaks the build here rather than at a call site.
var (
	_ customindex.CustomIndex = fixtureIndexer{}
	_ customindex.Indexer     = fixtureIndexer{}
)

// registerFixture attaches fixtureIndexer to idx through the
// customindex.Host capability. A backend that does not expose Host (custom
// indexes unsupported) skips the calling case rather than failing — the
// projection-query contract only applies where registration exists.
func registerFixture(t *testing.T, ctx context.Context, idx index.StoreIndex) {
	t.Helper()
	host, ok := idx.(customindex.Host)
	if !ok {
		t.Skip("backend does not expose customindex.Host: custom-index registration unsupported")
	}
	if err := host.CustomIndexes().Register(ctx, fixtureIndexer{}); err != nil {
		t.Fatalf("register fixture indexer: %v", err)
	}
}
