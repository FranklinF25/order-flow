package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeGate is the in-memory FastGate driving the wrapper tests (the go-redis
// adapter needs no server for these: the port contract is what matters).
type fakeGate struct {
	mu        sync.Mutex
	store     map[string]bool
	checkErr  error
	markErr   error
	seenCalls []string
	markCalls []string
}

func newFakeGate() *fakeGate { return &fakeGate{store: map[string]bool{}} }

func (f *fakeGate) Seen(_ context.Context, eventID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seenCalls = append(f.seenCalls, eventID)
	if f.checkErr != nil {
		return false, f.checkErr
	}
	return f.store[eventID], nil
}

func (f *fakeGate) Mark(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markCalls = append(f.markCalls, eventID)
	if f.markErr != nil {
		return f.markErr
	}
	f.store[eventID] = true
	return nil
}

// spyHandler records dispatches: on a gate hit it must NEVER run.
type spyHandler struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *spyHandler) handle(ctx context.Context, rec *kgo.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func recordWithEventID(id string) *kgo.Record {
	return &kgo.Record{
		Topic:   "t",
		Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte(id)}},
	}
}

// Gate hit absorbs: the handler is not invoked and the record returns nil, so
// the consumer commits the offset exactly like the DB-guard duplicate path.
func TestFastGateHitAbsorbsWithoutHandler(t *testing.T) {
	gate := newFakeGate()
	gate.store["evt-1"] = true
	spy := &spyHandler{}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err != nil {
		t.Fatalf("absorbed replay must return nil (offset commits), got %v", err)
	}
	if spy.calls != 0 {
		t.Errorf("handler dispatched %d times on gate hit, want 0", spy.calls)
	}
	if len(gate.markCalls) != 0 {
		t.Errorf("absorb must not re-mark, got %v", gate.markCalls)
	}
}

// Gate miss dispatches normally and marks exactly once, after the handler
// returned nil (tx committed).
func TestFastGateMissInvokesHandlerThenMarks(t *testing.T) {
	gate := newFakeGate()
	spy := &spyHandler{}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err != nil {
		t.Fatalf("gate miss must run the normal handler path, got %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
	if len(gate.markCalls) != 1 || gate.markCalls[0] != "evt-1" {
		t.Errorf("mark calls = %v, want exactly [evt-1] after handler success", gate.markCalls)
	}
}

// NEVER mark before the tx commits: a handler error propagates (record stays
// uncommitted) and the gate must not have marked the event — a crash between
// a premature mark and the commit would permanently lose the event.
func TestFastGateMarkOnlyAfterHandlerSuccess(t *testing.T) {
	gate := newFakeGate()
	spy := &spyHandler{err: errors.New("tx failed")}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err == nil {
		t.Fatal("handler error must propagate so the offset stays uncommitted")
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
	if len(gate.markCalls) != 0 {
		t.Errorf("mark called on handler error: %v — premature mark would lose the event on crash", gate.markCalls)
	}
}

// Redis unavailability degrades to the normal path (availability beats
// fastness): the check error is absorbed, the handler runs, and the mark is
// still attempted after success.
func TestFastGateCheckErrorProceedsToHandler(t *testing.T) {
	gate := newFakeGate()
	gate.checkErr = errors.New("redis unavailable")
	spy := &spyHandler{}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err != nil {
		t.Fatalf("check error must proceed to the handler (DB guard is the real gate), got %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
	if len(gate.markCalls) != 1 {
		t.Errorf("mark must still be attempted after success, got %v", gate.markCalls)
	}
}

// A mark failure AFTER the tx committed only logs: the record must not fail
// (the DB processed_events guard already covers the replay).
func TestFastGateMarkFailureAfterCommitIsAbsorbed(t *testing.T) {
	gate := newFakeGate()
	gate.markErr = errors.New("redis write timeout")
	spy := &spyHandler{}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err != nil {
		t.Fatalf("mark failure after commit must not fail the record, got %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
}

// Disabled gate (REDIS_URL unset): the wrapper must behave exactly like the
// bare handler.
func TestFastGateDisabledPassthrough(t *testing.T) {
	spy := &spyHandler{}
	h := WithFastGate(nil, spy.handle)

	if err := h(context.Background(), recordWithEventID("evt-1")); err != nil {
		t.Fatalf("disabled gate must pass through, got %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
}

// A record without an event_id header cannot be gated; it must bypass the
// gate untouched (the DB guard owns its safety).
func TestFastGateMissingEventIDBypassesGate(t *testing.T) {
	gate := newFakeGate()
	spy := &spyHandler{}
	h := WithFastGate(gate, spy.handle)

	if err := h(context.Background(), &kgo.Record{Topic: "t"}); err != nil {
		t.Fatalf("ungateable record must pass through, got %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("handler calls = %d, want 1", spy.calls)
	}
	if len(gate.seenCalls)+len(gate.markCalls) != 0 {
		t.Errorf("gate touched for a record without event_id: seen=%v mark=%v", gate.seenCalls, gate.markCalls)
	}
}

// The go-redis adapter satisfies the port, scopes keys per consumer group
// (order and payment both consume inventory.events — a shared keyspace would
// let one service's mark absorb the other's processing) and uses the 72h TTL.
func TestRedisGateAdapter(t *testing.T) {
	var _ FastGate = (*RedisGate)(nil)
	g := &RedisGate{scope: "order.inventory-events"}
	if got := g.key("evt-1"); got != "processed:order.inventory-events:evt-1" {
		t.Errorf("key = %q, want processed:order.inventory-events:evt-1", got)
	}
	if gateTTL != 259200*time.Second {
		t.Errorf("gateTTL = %v, want 259200s (72h)", gateTTL)
	}
}
