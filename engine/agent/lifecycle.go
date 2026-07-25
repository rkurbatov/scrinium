package agent

import (
	"context"
	"fmt"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver"
	"scrinium.dev/engine/lease"
	"scrinium.dev/event"
)

// TerminalMode controls when the success terminal event fires (ADR-94).
type TerminalMode int

const (
	// TerminalOnSuccess emits the terminal event only on a clean verdict,
	// after the heartbeat drain — the one-shot meaning of "completed"
	// (checkpoint, rebuild → EventAgentCompleted).
	TerminalOnSuccess TerminalMode = iota
	// TerminalEveryCycle emits the terminal event unconditionally, before
	// the error / lease-lost verdict — a recurring agent's "a cycle ran"
	// (gc, scrub → EventAgentCycle). Per-item failures live in Stats; the
	// overall error is reported separately through the return value.
	TerminalEveryCycle
)

// MaintenanceSpec parameterises RunLeased for one maintenance agent.
type MaintenanceSpec struct {
	AgentType    string       // registered kind; tags every lifecycle event
	StoreID      string       // tags event payloads
	Lease        lease.Config // Path, HostID, TTL; Lease.AgentType is the lease-owner tag (may differ from AgentType)
	LeaseEnabled bool         // false → take no lease (e.g. gc SingleHost relies on location.lock)
	Terminal     string       // event.EventAgentCompleted | event.EventAgentCycle
	TerminalMode TerminalMode
	Bus          event.Publisher
	Driver       driver.Driver
}

// RunLeased runs work under the standard maintenance-agent lifecycle
// (ADR-94): the Status transitions, the EventAgent* sequence and an
// optional held lease with a heartbeat. The agent supplies only work and
// its own domain events. The returned *AgentResult is also the terminal
// event payload — value and event must match (Agents contract §0.3).
//
// Sequence: StateRunning → EventAgentStarted → [if LeaseEnabled:
// lease.Acquire (prev → EventAgentStaleLease) + heartbeat + deferred
// Release] → work → build res. Then, by TerminalMode: EveryCycle emits
// the terminal immediately (before the verdict); the verdict is the work
// error, else a lost lease (drainHeartbeat); on error StateFaulted +
// EventAgentCancelled (context) or EventAgentFailed, else StateIdle and,
// for OnSuccess, the terminal. A failed Acquire is an early path:
// StateFaulted + the failure event, no work and no terminal.
func RunLeased(
	ctx context.Context,
	base *BaseState,
	spec MaintenanceSpec,
	work func(ctx context.Context) (stats map[string]int64, err error),
) (*domain.AgentResult, error) {
	base.SetState(StateRunning, nil)
	// One reading of the clock: the event and the result must agree, and a
	// result without StartedAt makes CompletedAt.Sub(StartedAt) a number in the
	// millions of hours.
	started := time.Now()
	spec.Bus.Publish(event.Event{Type: event.EventAgentStarted, Payload: event.AgentStartedPayload{
		AgentType: spec.AgentType, StoreID: spec.StoreID, StartedAt: started,
	}})

	runCtx := ctx
	var hbErr <-chan error
	if spec.LeaseEnabled {
		l, prev, err := lease.Acquire(ctx, spec.Driver, spec.Lease)
		if err != nil {
			return failTerminal(base, spec, &domain.AgentResult{
				AgentType: spec.AgentType, StoreID: spec.StoreID,
				StartedAt: started, CompletedAt: time.Now(),
			}, fmt.Errorf("agent %q: acquire lease: %w", spec.AgentType, err))
		}
		if prev != nil {
			spec.Bus.Publish(event.Event{Type: event.EventAgentStaleLease, Payload: event.LeaseTakeoverPayload{
				LeaseKey: spec.Lease.Name, PreviousHolder: prev.HostID,
				ExpiredAt: prev.ExpiresAt, TakenBy: spec.Lease.HostID,
			}})
		}
		var cancel context.CancelFunc
		runCtx, cancel = context.WithCancel(ctx)
		ch := make(chan error, 1)
		go func() { ch <- l.Heartbeat(runCtx) }()
		hbErr = ch
		defer func() {
			cancel()
			if err := l.Release(context.WithoutCancel(ctx)); err != nil {
				base.Logger().Warn("lease release failed; lease will expire via TTL", "err", err)
			}
		}()
	}

	stats, werr := work(runCtx)
	res := &domain.AgentResult{
		AgentType:   spec.AgentType,
		StoreID:     spec.StoreID,
		StartedAt:   started,
		CompletedAt: time.Now(),
		Stats:       stats,
	}

	if spec.TerminalMode == TerminalEveryCycle {
		spec.Bus.Publish(event.Event{Type: spec.Terminal, Payload: *res})
	}

	err := werr
	if err == nil {
		err = drainHeartbeat(hbErr) // surface a lost lease mid-run
	}
	if err != nil {
		return failTerminal(base, spec, res, err)
	}

	base.SetState(StateIdle, nil)
	if spec.TerminalMode == TerminalOnSuccess {
		spec.Bus.Publish(event.Event{Type: spec.Terminal, Payload: *res})
	}
	return res, nil
}

// failTerminal records the faulted state and emits the matching terminal
// failure event: cancellation (context error → partial result) or
// failure. It returns the same (res, err) the agent's Run propagates.
func failTerminal(base *BaseState, spec MaintenanceSpec, res *domain.AgentResult, err error) (*domain.AgentResult, error) {
	base.SetState(StateFaulted, err)
	if IsCtxErr(err) {
		res.Partial = true
		spec.Bus.Publish(event.Event{Type: event.EventAgentCancelled})
		return res, err
	}
	spec.Bus.Publish(event.Event{Type: event.EventAgentFailed, Payload: event.AgentFailedPayload{
		AgentType: spec.AgentType, StoreID: spec.StoreID, Err: err,
	}})
	return res, err
}

// drainHeartbeat returns a non-context heartbeat error already pending on
// ch, else nil. ch is nil when no lease was taken.
func drainHeartbeat(ch <-chan error) error {
	if ch == nil {
		return nil
	}
	select {
	case herr := <-ch:
		if herr != nil && !IsCtxErr(herr) {
			return herr
		}
	default:
	}
	return nil
}
