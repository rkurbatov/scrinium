package assembly

// The parting checkpoint (ADR-118, INV-118-5) is best-effort by contract:
// the recovery procedure has to work with a checkpoint of any age and with
// none at all, so nothing about shutdown may hinge on it. These cases pin
// exactly that — the shape of the promise, not the checkpoint itself.
//
// The real checkpoint agent is not linked into this test binary (agents
// arrive through blank imports in the host), and pulling it in would drag a
// live store and index behind it. So the factory registered here stands in
// under the same name: the assertions are about what finalCheckpoint does
// with the agent, not about what the agent does with the index.

import (
	"context"
	"errors"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/agent"
	"scrinium.dev/engine/store"
)

// stubAgent satisfies domain.MaintenanceAgent; the store stub below never
// actually runs it.
type stubAgent struct{}

func (stubAgent) Validate(context.Context) error { return nil }
func (stubAgent) Run(context.Context) (*domain.AgentResult, error) {
	return &domain.AgentResult{}, nil
}
func (stubAgent) Status() (agent.State, error) { return agent.StateIdle, nil }
func (stubAgent) AgentType() string            { return "checkpoint" }

// stubFactory registers under "checkpoint" so agent.Build resolves in this
// binary. init runs once per test binary, and the real agent is absent here,
// so there is no duplicate registration.
type stubFactory struct{}

func (stubFactory) Name() string { return "checkpoint" }
func (stubFactory) Build(store.Store, any, agent.AgentDeps) (agent.Agent, error) {
	return stubAgent{}, nil
}

func init() { agent.Register(stubFactory{}) }

// stubStore records RunMaintenance calls and can be told to fail them.
type stubStore struct {
	store.Store
	runs   int
	runErr error
}

func (s *stubStore) RunMaintenance(_ context.Context, _ domain.MaintenanceAgent) (*domain.AgentResult, error) {
	s.runs++
	if s.runErr != nil {
		return nil, s.runErr
	}
	return &domain.AgentResult{}, nil
}

func (s *stubStore) Close() error { return nil }

func TestFinalCheckpoint_RunsOnTheWayOut(t *testing.T) {
	st := &stubStore{}
	bs := &buildState{st: st}

	bs.finalCheckpoint()

	if st.runs != 1 {
		t.Fatalf("RunMaintenance calls: got %d, want 1", st.runs)
	}
}

func TestFinalCheckpoint_FailureIsSwallowed(t *testing.T) {
	st := &stubStore{runErr: errors.New("backend unavailable")}
	bs := &buildState{st: st}

	// The whole point: a checkpoint that could not be taken must not turn a
	// clean shutdown into a failure. finalCheckpoint returns nothing, so the
	// assertion is that it was attempted and did not panic or propagate.
	bs.finalCheckpoint()

	if st.runs != 1 {
		t.Fatalf("RunMaintenance calls: got %d, want 1", st.runs)
	}
}
