package ejector

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/store"
	"scrinium.dev/errs"
	"scrinium.dev/event"
)

// --- Sync Agent (Reserved, D-05; scheduled for M4/S3) ---

// SyncAgent replicates artifacts between a Target and a Backup Store.
// Reserved: interface fixed for API stability, implementation deferred
// to M4/S3 pending the reconciliation-mechanism decision.
type SyncAgent interface {
	agent.Agent
	RunNow(ctx context.Context, artifactID domain.ArtifactID) error
}

// --- Ejector Agent (ADR-70) ---
//
// The Ejector materialises artifacts onto the host filesystem as plain
// POSIX files and returns a path, for consumers that need a file path
// rather than an io.Reader (a player, ImageMagick, an external binary).
// Consumers that can take a stream use Store.Get directly — the Ejector
// is path-only (ADR-70 §"Get vs Eject"). Files are named by ContentHash
// (the store's plaintext dedup key), so identical content materialises
// once regardless of which artifact id resolves to it. The materialised
// set is a private, reproducible, plaintext scratch — never the sole
// copy of anything (ADR-67), so it is reclaimable at will.
//
// The Ejector is a resident agent (ADR-68): it holds a volatile table
// of materialisations in memory; the assembler builds it once and holds
// it for the Store's lifetime, closing it via cascade.

// Ejector materialises artifacts as host files (ADR-70).
type Ejector interface {
	agent.Agent

	// Eject materialises the whole artifact and returns its path.
	// Fire-and-forget: takes no holder, lives under KeepAliveIdle.
	// Idempotent and content-addressed: identical content returns the
	// same path. A returned path is a complete, ready-to-read file.
	Eject(ctx context.Context, id domain.ArtifactID) (string, error)

	// EjectFragment materialises the byte range [start, end) and returns
	// its path (named by the fragment's own hash). Fire-and-forget.
	// start==0 always works; start>0 requires random access on the
	// ReadHandle, else ErrRandomAccessNotSupported.
	EjectFragment(ctx context.Context, id domain.ArtifactID, start, end int64) (string, error)

	// Hold materialises the whole artifact and returns a holder; while
	// held, the file is not reclaimed (except MaxLifetime / Close /
	// open-sweep). For consumers that need "do not reclaim while I work".
	Hold(ctx context.Context, id domain.ArtifactID) (EjectHandle, error)

	// Close stops the Ejector and removes all scratch, ignoring holders.
	// Idempotent.
	Close() error
}

// EjectHandle is a holder on a materialisation. Release decrements the
// holder count; idempotent.
type EjectHandle interface {
	Path() string
	Release() error
}

type entry struct {
	path        string
	verifyHash  string // sha256 hex of the materialised bytes
	size        int64
	holders     int
	extractedAt time.Time
	lastAccess  time.Time
}

type reqKey struct {
	id    domain.ArtifactID
	start int64
	end   int64
}

type ejectorAgent struct {
	agent.BaseState

	st      store.Store
	bus     event.Publisher
	storeID string
	cfg     EjectorConfig

	sem chan struct{} // bounds concurrent materialisations

	mu     sync.Mutex
	byHash map[string]*entry // contentHash -> entry (reclamation key)
	byReq  map[reqKey]string // (id,start,end) -> contentHash (skip-read memo)
	closed bool
}

var _ Ejector = (*ejectorAgent)(nil)

// NewEjector constructs an Ejector. TempDir is required; it is created
// and swept (crash leftovers removed) on open.
func NewEjector(st store.Store, bus event.Publisher, storeID string, cfg EjectorConfig, opts ...agent.AgentOption) (Ejector, error) {
	if st == nil {
		return nil, fmt.Errorf("ejector.NewEjector: nil store")
	}
	if cfg.TempDir == "" {
		return nil, fmt.Errorf("ejector.NewEjector: TempDir is required")
	}
	cfg.applyDefaults()
	if err := os.MkdirAll(cfg.TempDir, 0o700); err != nil {
		return nil, fmt.Errorf("ejector.NewEjector: temp dir: %w", err)
	}
	a := &ejectorAgent{
		BaseState: agent.NewBaseState(agent.ResolveLogger(opts...)),
		st:        st,
		bus:       bus,
		storeID:   storeID,
		cfg:       cfg,
		sem:       make(chan struct{}, cfg.MaxConcurrent),
		byHash:    make(map[string]*entry),
		byReq:     make(map[reqKey]string),
	}
	a.sweepOnOpen()
	return a, nil
}

func (a *ejectorAgent) AgentType() string { return "ejector" }

func (a *ejectorAgent) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return errs.ErrEjectorClosed
	}
	return nil
}

// Run performs one reclamation sweep (idle + lifetime). The scheduler
// (ADR-69) drives periodicity; the Ejector has no background goroutine.
func (a *ejectorAgent) Run(ctx context.Context) (*domain.AgentResult, error) {
	a.SetState(agent.StateRunning, nil)
	started := time.Now()
	if err := ctx.Err(); err != nil {
		a.SetState(agent.StateFaulted, err)
		return &domain.AgentResult{
			AgentType: "ejector", StoreID: a.storeID,
			StartedAt: started, CompletedAt: time.Now(), Partial: true,
		}, err
	}
	counts := a.reclaim(time.Now())
	a.emitCleanup(counts)
	var total int64
	for _, n := range counts {
		total += int64(n)
	}
	a.SetState(agent.StateIdle, nil)
	return &domain.AgentResult{
		AgentType:   "ejector",
		StoreID:     a.storeID,
		StartedAt:   started,
		CompletedAt: time.Now(),
		Stats:       map[string]int64{"evicted": total},
	}, nil
}

// Close stops the Ejector and removes all scratch, ignoring holders.
func (a *ejectorAgent) Close() error {
	a.mu.Lock()
	a.closed = true
	paths := make([]string, 0, len(a.byHash))
	for _, e := range a.byHash {
		paths = append(paths, e.path)
	}
	a.byHash = make(map[string]*entry)
	a.byReq = make(map[reqKey]string)
	a.mu.Unlock()
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			a.Logger().Debug("ejector: scratch remove on close failed", "path", p, "err", err)
		}
	}
	return nil
}
