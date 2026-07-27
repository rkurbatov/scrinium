// RunLeased's result contract: both timestamps are filled, on success and on
// failure alike, and they agree with the started event. A result carrying only
// CompletedAt turns a host's CompletedAt.Sub(StartedAt) into 2562047h — which is
// how this was found.

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"scrinium.dev/domain"
	"scrinium.dev/event"
)

// collect returns a bus that records every event it is given.
func collect(recs *[]event.Event) event.EventBus {
	bus := event.NewEventBus()
	bus.Subscribe(func(e event.Event) { *recs = append(*recs, e) })
	return bus
}

func spec(bus event.EventBus) MaintenanceSpec {
	return MaintenanceSpec{
		AgentType:    "testagent",
		StoreID:      "store-1",
		Terminal:     event.EventAgentCompleted,
		TerminalMode: TerminalOnSuccess,
		Bus:          bus,
	}
}

func TestRunLeased_ResultCarriesBothTimestamps(t *testing.T) {
	cases := []struct {
		name string
		work func(ctx context.Context) (map[string]int64, error)
		fail bool
	}{
		{
			name: "success",
			work: func(context.Context) (map[string]int64, error) {
				time.Sleep(2 * time.Millisecond)
				return map[string]int64{"done": 1}, nil
			},
		},
		{
			name: "failure",
			work: func(context.Context) (map[string]int64, error) {
				time.Sleep(2 * time.Millisecond)
				return nil, errors.New("work failed")
			},
			fail: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var recs []event.Event
			base := NewBaseState(nil)

			before := time.Now()
			res, err := RunLeased(context.Background(), &base, spec(collect(&recs)), tc.work)
			after := time.Now()

			if tc.fail != (err != nil) {
				t.Fatalf("err = %v, want failure=%v", err, tc.fail)
			}
			if res == nil {
				t.Fatal("a result is owed even when the work failed")
			}
			if res.StartedAt.IsZero() {
				t.Error("StartedAt is zero: the field is declared and must be filled")
			}
			if res.CompletedAt.IsZero() {
				t.Error("CompletedAt is zero")
			}
			if res.CompletedAt.Before(res.StartedAt) {
				t.Errorf("CompletedAt %v is before StartedAt %v", res.CompletedAt, res.StartedAt)
			}
			if res.StartedAt.Before(before) || res.CompletedAt.After(after) {
				t.Errorf("timestamps outside the call: %v..%v not within %v..%v",
					res.StartedAt, res.CompletedAt, before, after)
			}
			if d := res.CompletedAt.Sub(res.StartedAt); d <= 0 || d > time.Minute {
				t.Errorf("duration = %v, want something a host can print", d)
			}
		})
	}
}

// The started event and the result read the same clock, so a consumer joining
// the two does not see the agent finish before it began.
func TestRunLeased_StartedEventMatchesResult(t *testing.T) {
	var recs []event.Event
	base := NewBaseState(nil)

	res, err := RunLeased(context.Background(), &base, spec(collect(&recs)),
		func(context.Context) (map[string]int64, error) { return nil, nil })
	if err != nil {
		t.Fatalf("RunLeased: %v", err)
	}

	var startedAt time.Time
	for _, e := range recs {
		if p, ok := e.Payload.(event.AgentStartedPayload); ok && e.Type == event.EventAgentStarted {
			startedAt = p.StartedAt
		}
	}
	if startedAt.IsZero() {
		t.Fatal("no agent.started event carrying a timestamp")
	}
	if !startedAt.Equal(res.StartedAt) {
		t.Errorf("event StartedAt %v != result StartedAt %v", startedAt, res.StartedAt)
	}
}

// A cancelled run is partial, and a partial result is still a result: the
// timestamps have to be there for a host to report how long it ran before it
// gave up.
func TestRunLeased_CancelledStillTimestamped(t *testing.T) {
	var recs []event.Event
	base := NewBaseState(nil)
	ctx, cancel := context.WithCancel(context.Background())

	res, err := RunLeased(ctx, &base, spec(collect(&recs)),
		func(context.Context) (map[string]int64, error) {
			cancel()
			return nil, context.Canceled
		})
	if err == nil {
		t.Fatal("expected the cancellation to surface")
	}
	if res == nil || res.StartedAt.IsZero() || res.CompletedAt.IsZero() {
		t.Fatalf("cancelled run must still carry both timestamps: %+v", res)
	}
	var _ *domain.AgentResult = res
}
