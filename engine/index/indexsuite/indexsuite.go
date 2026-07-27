package indexsuite

import (
	"context"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/index"
)

// Factory describes one StoreIndex implementation under test.
type Factory struct {
	// Name appears in subtest output as a prefix. The suite uses
	// t.Run(Name+"/"+caseName) so multiple factories can be
	// exercised from the same test entry point if ever needed.
	Name string

	// New returns a fresh, empty StoreIndex. Each subtest gets its
	// own instance — implementations should rely on t.Cleanup for
	// teardown and never share state across subtests.
	New func(t *testing.T) index.StoreIndex
}

// Run executes the full conformance suite against f.
func Run(t *testing.T, f Factory) {
	t.Helper()
	if f.New == nil {
		t.Fatal("indexsuite.Run: Factory.New is nil")
	}
	if f.Name == "" {
		f.Name = "anon"
	}

	// Group the suite into logical sections. Each section is its
	// own t.Run so a failure in one method's tests does not hide
	// failures elsewhere.
	t.Run(f.Name+"/Resolve", func(t *testing.T) { runResolve(t, f) })
	t.Run(f.Name+"/ExistsByContent", func(t *testing.T) { runExistsByContent(t, f) })
	t.Run(f.Name+"/ExistsByHash", func(t *testing.T) { runExistsByHash(t, f) })
	t.Run(f.Name+"/GetRefCount", func(t *testing.T) { runGetRefCount(t, f) })
	t.Run(f.Name+"/IndexManifest", func(t *testing.T) { runIndexManifest(t, f) })
	t.Run(f.Name+"/DeleteManifest", func(t *testing.T) { runDeleteManifest(t, f) })
	t.Run(f.Name+"/ManifestExists", func(t *testing.T) { runManifestExists(t, f) })
	t.Run(f.Name+"/MarkVerified", func(t *testing.T) { runMarkVerified(t, f) })
	t.Run(f.Name+"/ListUnverifiedManifests", func(t *testing.T) { runListUnverifiedManifests(t, f) })
	t.Run(f.Name+"/ManifestsByBlobRef", func(t *testing.T) { runManifestsByBlobRef(t, f) })
	t.Run(f.Name+"/IterateManifests", func(t *testing.T) { runIterateManifests(t, f) })
	t.Run(f.Name+"/QueryByExtField", func(t *testing.T) { runQueryByExtField(t, f) })
	t.Run(f.Name+"/ListByExtField", func(t *testing.T) { runListByExtField(t, f) })
	t.Run(f.Name+"/GetBySession", func(t *testing.T) { runGetBySession(t, f) })
	t.Run(f.Name+"/ListOrphanBlobs", func(t *testing.T) { runListOrphanBlobs(t, f) })
	t.Run(f.Name+"/DeleteOrphanBlob", func(t *testing.T) { runDeleteOrphanBlob(t, f) })
	t.Run(f.Name+"/ListUnverifiedBlobs", func(t *testing.T) { runListUnverifiedBlobs(t, f) })

	// Optional synchronization capability (ADR-106): runSyncSource skips
	// inside when the backend does not implement index.SyncSource.
	t.Run(f.Name+"/SyncSource", func(t *testing.T) { runSyncSource(t, f) })
}

// collectAll turns a streaming IterateManifests into a slice for
// table-style assertions.
func collectAll(t *testing.T, idx index.StoreIndex) []domain.Manifest {
	t.Helper()
	var got []domain.Manifest
	err := idx.IterateManifests(context.Background(), func(m domain.Manifest) error {
		got = append(got, m)
		return nil
	})
	if err != nil {
		t.Fatalf("IterateManifests: %v", err)
	}
	return got
}
