package kafka

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
)

// fakeDLQ is the in-memory DLQProducer driving the retries/DLQ wrapper tests
// (the franz-go adapter needs no broker: the port contract is what matters).
type fakeDLQ struct {
	mu         sync.Mutex
	produced   []*kgo.Record
	produceErr error
}

func (f *fakeDLQ) Produce(_ context.Context, rec *kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.produceErr != nil {
		return f.produceErr
	}
	f.produced = append(f.produced, rec)
	return nil
}

// Close satisfies the port; tests never close the fake.
func (f *fakeDLQ) Close() {}

func (f *fakeDLQ) records() []*kgo.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*kgo.Record{}, f.produced...)
}

// flakyHandler fails its first failFirst dispatches with err, then succeeds;
// a poison handler is one whose failFirst budget outlives every attempt.
type flakyHandler struct {
	mu        sync.Mutex
	calls     int
	failFirst int
	err       error
}

func (h *flakyHandler) handle(_ context.Context, _ *kgo.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.calls <= h.failFirst {
		return h.err
	}
	return nil
}

func (h *flakyHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// poisonRecord is a realistic consumed record: source topic, partition,
// offset, aggregate key, raw avro wire bytes and the producer's headers.
func poisonRecord() *kgo.Record {
	return &kgo.Record{
		Topic:     "inventory.events",
		Partition: 2,
		Offset:    7,
		Key:       []byte("order-1"),
		Value:     []byte(`{"raw":"wire-bytes"}`),
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte("orderflow.inventory.StockReserved")},
			{Key: "event_id", Value: []byte("evt-42")},
		},
	}
}

// Poison handler: exactly 3 attempts (1 initial + 2 retries), then the RAW
// record is parked on <source-topic>.<group>.dlq with the full dlq.* header
// set, and the wrapper returns nil so the offset COMMITS (topology §7: the
// stream continues past the parked record).
func TestWithRetriesPoisonParksRawRecordOnDLQAndCommits(t *testing.T) {
	dlq := &fakeDLQ{}
	boom := errors.New("db unreachable")
	h := &flakyHandler{failFirst: 3, err: boom}
	wrapped := WithRetries(dlq, metrics.New(), 0, "order.inventory-events", h.handle) // zero baseDelay: no real sleeping

	rec := poisonRecord()
	start := time.Now()
	err := wrapped(context.Background(), rec)
	if d := time.Since(start); d > time.Second {
		t.Errorf("zero baseDelay must not really sleep, took %v", d)
	}
	if err != nil {
		t.Fatalf("after the DLQ park the wrapper must return nil (offset commits), got %v", err)
	}
	if got := h.count(); got != 3 {
		t.Errorf("handler attempts = %d, want exactly 3 (1 initial + 2 retries)", got)
	}
	produced := dlq.records()
	if len(produced) != 1 {
		t.Fatalf("dlq produces = %d, want exactly 1", len(produced))
	}
	parked := produced[0]
	if parked.Topic != "inventory.events.order.inventory-events.dlq" {
		t.Errorf("dlq topic = %q, want inventory.events.order.inventory-events.dlq", parked.Topic)
	}
	if !bytes.Equal(parked.Key, rec.Key) {
		t.Errorf("dlq key = %q, want the original key %q preserved", parked.Key, rec.Key)
	}
	if !bytes.Equal(parked.Value, rec.Value) {
		t.Errorf("dlq value = %q, want the original raw bytes %q unchanged", parked.Value, rec.Value)
	}
	want := map[string]string{
		"dlq.original-topic": "inventory.events",
		"dlq.partition":      "2",
		"dlq.offset":         "7",
		"dlq.event-type":     "orderflow.inventory.StockReserved",
		"dlq.error":          boom.Error(),
		"dlq.attempts":       "3",
	}
	for k, v := range want {
		if got := headerValue(parked, k); got != v {
			t.Errorf("dlq header %s = %q, want %q", k, got, v)
		}
	}
	if headerValue(parked, "event_id") != "evt-42" {
		t.Error("original headers must be preserved on the parked record")
	}
}

// Transient failure: the 3rd attempt succeeds, so NO DLQ produce happens and
// the wrapper returns nil — the offset commits on the healthy path.
func TestWithRetriesTransientFailureRecoversWithoutDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 2, err: errors.New("transient deadlock")}
	wrapped := WithRetries(dlq, metrics.New(), 0, "order.inventory-events", h.handle)

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("recovery on the 3rd attempt must return nil, got %v", err)
	}
	if got := h.count(); got != 3 {
		t.Errorf("handler attempts = %d, want 3", got)
	}
	if produced := dlq.records(); len(produced) != 0 {
		t.Errorf("dlq produces = %d, want 0 (recovery must not park the record)", len(produced))
	}
}

// DLQ produce failure: the wrapper returns the error so the offset stays
// uncommitted — the record is not lost; redelivery re-runs the whole path.
func TestWithRetriesDLQProduceFailureReturnsError(t *testing.T) {
	dlq := &fakeDLQ{produceErr: errors.New("broker down")}
	h := &flakyHandler{failFirst: 3, err: errors.New("poison")}
	wrapped := WithRetries(dlq, metrics.New(), 0, "order.payment-events", h.handle)

	rec := poisonRecord()
	rec.Topic = "payment.events"
	if err := wrapped(context.Background(), rec); err == nil {
		t.Fatal("a DLQ produce failure must return the error (no commit path)")
	}
	if got := h.count(); got != 3 {
		t.Errorf("handler attempts = %d, want 3 before the DLQ park", got)
	}
	if produced := dlq.records(); len(produced) != 0 {
		t.Errorf("failed dlq produces must not be recorded as parked, got %d", len(produced))
	}
}

// The exponential backoff is real: base 20ms means two backoffs (20ms + 40ms)
// before the 3rd attempt, so the wrapper must take at least 60ms.
func TestWithRetriesHonorsExponentialBackoff(t *testing.T) {
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 2, err: errors.New("flaky")}
	wrapped := WithRetries(dlq, metrics.New(), 20*time.Millisecond, "order.inventory-events", h.handle)

	start := time.Now()
	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("transient recovery must return nil, got %v", err)
	}
	if d := time.Since(start); d < 60*time.Millisecond {
		t.Errorf("exponential backoff 20ms+40ms must take >= 60ms, took %v", d)
	}
}

// Gate-absorb composition (gate OUTSIDE the retry wrapper): a gate hit must
// absorb before any attempt — zero handler dispatches, zero retries, zero DLQ
// produces, nil (offset commits exactly like the duplicate path).
func TestFastGateAbsorbPerformsZeroRetriesAndZeroDLQ(t *testing.T) {
	gate := newFakeGate()
	gate.store["evt-42"] = true
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 99, err: errors.New("must never run")}
	wrapped := WithFastGate(gate, WithRetries(dlq, metrics.New(), 0, "order.inventory-events", h.handle))

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("absorbed replay must return nil, got %v", err)
	}
	if got := h.count(); got != 0 {
		t.Errorf("handler dispatched %d times on gate hit, want 0 (no retries burned)", got)
	}
	if produced := dlq.records(); len(produced) != 0 {
		t.Errorf("dlq produces = %d, want 0 on the absorb path", len(produced))
	}
}
