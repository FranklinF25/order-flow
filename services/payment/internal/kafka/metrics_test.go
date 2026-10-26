package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/payment/internal/events"
	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

// --- fakes (payment.Tx/Store stand-ins + relay ports; same-package reuse of
// fakeDLQ / flakyHandler / poisonRecord from dlq_test.go) ---

// memStore is an in-memory payment.Store/Tx: the guard and the order_id
// UNIQUE stand-in behave exactly like the postgres adapter.
type memStore struct {
	payments  map[string]*payment.Payment
	processed map[string]bool
}

func newMemStore() *memStore {
	return &memStore{payments: map[string]*payment.Payment{}, processed: map[string]bool{}}
}

func (s *memStore) SavePayment(_ context.Context, p *payment.Payment) (bool, error) {
	if _, ok := s.payments[p.OrderID]; ok {
		return false, nil
	}
	cp := *p
	s.payments[p.OrderID] = &cp
	return true, nil
}

func (s *memStore) AppendProcessedEvent(_ context.Context, id string) (bool, error) {
	if s.processed[id] {
		return false, nil
	}
	s.processed[id] = true
	return true, nil
}

func (s *memStore) AppendOutbox(_ context.Context, _ payment.OutboxEvent) error { return nil }

type memTx struct{ s *memStore }

func (t *memTx) WithinTx(ctx context.Context, fn func(context.Context, payment.Store) error) error {
	return fn(ctx, t.s)
}

// stockReservedWire builds a Confluent wire value (magic + schema id + avro
// body) for the given event, the same shape the decoder reads.
func stockReservedWire(t *testing.T, evt events.StockReserved) []byte {
	t.Helper()
	c, err := avro.Parse(`{
		"type": "record",
		"name": "StockReserved",
		"namespace": "orderflow.inventory",
		"fields": [
			{"name": "event_id", "type": "string"},
			{"name": "order_id", "type": "string"},
			{"name": "occurred_at", "type": "string"},
			{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "StockItem",
				"fields": [{"name": "sku", "type": "string"}, {"name": "qty", "type": "int"}]
			}}},
			{"name": "total_cents", "type": "long"},
			{"name": "customer_id", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	body, err := avro.Marshal(c, evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	value := make([]byte, 5+len(body))
	value[0] = 0x00
	value[1] = 0
	value[2] = 0
	value[3] = 0
	value[4] = 42
	copy(value[5:], body)
	return value
}

// stockReservedRecord is a realistic StockReserved record on inventory.events.
func stockReservedRecord(t *testing.T, eventID string) *kgo.Record {
	t.Helper()
	return &kgo.Record{
		Topic: "inventory.events",
		Value: stockReservedWire(t, events.StockReserved{
			EventID:    eventID,
			OrderID:    "ord-" + eventID,
			OccurredAt: "2026-10-25T12:00:00Z",
			Items:      []events.StockItem{{SKU: "SKU-A", Qty: 1}},
			TotalCents: 4998,
			CustomerID: "cus-ok-1",
		}),
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(events.StockReservedType)},
			{Key: "event_id", Value: []byte(eventID)},
		},
	}
}

func newTestMetrics() (*metrics.Metrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return metrics.NewWithRegisterer(reg), reg
}

// labeled reads one labelled child of a vec (panics with a useful message if
// the test got the pinned labels wrong).
func labeled(t *testing.T, vec *prometheus.CounterVec, labelValues ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(labelValues...))
}

// --- events_total: the outcome label paths ---

// A fresh StockReserved is processed; its replay is absorbed by the DB guard;
// a non-matching event_type pass-through and a header-less record are both
// absorbed. A decode failure (garbage value) counts NOTHING: the offset never
// commits, redelivery re-runs the path.
func TestMetricsEventsProcessedVsAbsorbed(t *testing.T) {
	m, _ := newTestMetrics()
	store := newMemStore()
	c := &Consumer{
		svc: payment.NewService(&memTx{store}, payment.NewSimulatedGateway(), events.NewEncoder()),
		dec: events.NewDecoder(),
		m:   m,
	}
	ctx := context.Background()

	// Fresh delivery → processed.
	if err := c.handle(ctx, stockReservedRecord(t, "evt-1")); err != nil {
		t.Fatalf("fresh handle: %v", err)
	}
	// Redelivery of the SAME event id → the DB guard absorbs it → absorbed.
	if err := c.handle(ctx, stockReservedRecord(t, "evt-1")); err != nil {
		t.Fatalf("replayed handle: %v", err)
	}
	// Non-matching event_type → pass-through → absorbed, labeled with what
	// the header actually said.
	other := stockReservedRecord(t, "evt-2")
	other.Headers[0].Value = []byte("orderflow.order.OrderPlaced")
	if err := c.handle(ctx, other); err != nil {
		t.Fatalf("pass-through handle: %v", err)
	}
	// Header-less record → pass-through → absorbed with the empty event_type.
	bare := stockReservedRecord(t, "evt-3")
	bare.Headers = nil
	if err := c.handle(ctx, bare); err != nil {
		t.Fatalf("bare handle: %v", err)
	}
	// Decode failure → uncounted (no commit decision).
	bad := stockReservedRecord(t, "evt-4")
	bad.Value = []byte("not-avro")
	if err := c.handle(ctx, bad); err == nil {
		t.Fatal("garbage value must fail to decode")
	}

	group, topic, et := consumerGroup, "inventory.events", events.StockReservedType
	if got := labeled(t, m.EventsCounter(), metrics.ServiceName, group, topic, et, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("events_total processed = %v, want 1", got)
	}
	if got := labeled(t, m.EventsCounter(), metrics.ServiceName, group, topic, et, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("events_total absorbed (db duplicate) = %v, want 1", got)
	}
	if got := labeled(t, m.EventsCounter(), metrics.ServiceName, group, topic, "orderflow.order.OrderPlaced", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("events_total absorbed (pass-through) = %v, want 1", got)
	}
	if got := labeled(t, m.EventsCounter(), metrics.ServiceName, group, topic, "", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("events_total absorbed (header-less) = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.EventsCounter(), "orderflow_events_total"); got != 4 {
		t.Errorf("events_total children = %d, want 4 (decode failure must not count)", got)
	}
}

// --- retries_total + dlq_parked_total through the REAL NewConsumer
// composition (gate → retries/DLQ → handle): a poison record (StockReserved
// header, undecodable value) burns exactly 3 blocked attempts and parks once,
// labeled with the SOURCE topic. A DLQ produce failure parks nothing.
func TestMetricsCompositionCountsBlockedAttemptsAndParks(t *testing.T) {
	m, _ := newTestMetrics()
	dlq := &fakeDLQ{}
	c, err := NewConsumer([]string{"localhost:9092"}, // lazy client: never dialed in tests
		payment.NewService(&memTx{newMemStore()}, payment.NewSimulatedGateway(), events.NewEncoder()),
		events.NewDecoder(), nil, dlq, m)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	poison := stockReservedRecord(t, "evt-poison")
	poison.Value = []byte("garbage") // decode fails on every attempt
	if err := c.handler(context.Background(), poison); err != nil {
		t.Fatalf("after the park the wrapper must return nil, got %v", err)
	}
	if got := labeled(t, m.RetriesCounter(), metrics.ServiceName, consumerGroup); got != 3 {
		t.Errorf("retries_total = %v, want 3 (1 initial + 2 blocked retries)", got)
	}
	if got := labeled(t, m.DLQParkedCounter(), metrics.ServiceName, consumerGroup, "inventory.events"); got != 1 {
		t.Errorf("dlq_parked_total = %v, want 1 (source topic label, not the dlq topic)", got)
	}
	if len(dlq.records()) != 1 {
		t.Fatalf("dlq produces = %d, want 1", len(dlq.records()))
	}
	// A SECOND poison record adds 3 blocked attempts on the same group label.
	if err := c.handler(context.Background(), poison); err != nil {
		t.Fatalf("second poison: %v", err)
	}
	if got := labeled(t, m.RetriesCounter(), metrics.ServiceName, consumerGroup); got != 6 {
		t.Errorf("retries_total = %v, want 6 after two poison records", got)
	}
}

// Transient recovery: two blocked attempts then success — retries counts 2,
// nothing parks.
func TestMetricsRetriesCountOnlyBlockedAttempts(t *testing.T) {
	m, _ := newTestMetrics()
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 2, err: errors.New("transient")}
	wrapped := WithRetries(newMetricsDLQ(dlq, m, consumerGroup), 0, consumerGroup,
		blockedAttemptHandler{next: h.handle, m: m, group: consumerGroup}.handle)

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("transient recovery must return nil, got %v", err)
	}
	if got := labeled(t, m.RetriesCounter(), metrics.ServiceName, consumerGroup); got != 2 {
		t.Errorf("retries_total = %v, want 2 blocked attempts", got)
	}
	if got := labeled(t, m.DLQParkedCounter(), metrics.ServiceName, consumerGroup, "inventory.events"); got != 0 {
		t.Errorf("dlq_parked_total = %v, want 0 (recovered, never parked)", got)
	}
}

// A DLQ produce failure leaves the record unparked (offset uncommitted): the
// park counter must not move.
func TestMetricsDLQProduceFailureDoesNotCountPark(t *testing.T) {
	m, _ := newTestMetrics()
	dlq := &fakeDLQ{produceErr: errors.New("broker down")}
	h := &flakyHandler{failFirst: 3, err: errors.New("poison")}
	wrapped := WithRetries(newMetricsDLQ(dlq, m, consumerGroup), 0, consumerGroup,
		blockedAttemptHandler{next: h.handle, m: m, group: consumerGroup}.handle)

	if err := wrapped(context.Background(), poisonRecord()); err == nil {
		t.Fatal("dlq produce failure must return the error")
	}
	if got := labeled(t, m.RetriesCounter(), metrics.ServiceName, consumerGroup); got != 3 {
		t.Errorf("retries_total = %v, want 3 (attempts were still blocked)", got)
	}
	if got := labeled(t, m.DLQParkedCounter(), metrics.ServiceName, consumerGroup, "inventory.events"); got != 0 {
		t.Errorf("dlq_parked_total = %v, want 0 (park failed)", got)
	}
}

// --- outbox relay: pending gauge per tick, published counter per ack ---

type fakeOutboxStore struct {
	mu      sync.Mutex
	pending int64
	events  []payment.OutboxEvent
	marked  [][]string
	markErr error
}

func (s *fakeOutboxStore) CountUnpublished(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending, nil
}

func (s *fakeOutboxStore) FetchUnpublished(_ context.Context, _ int) ([]payment.OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]payment.OutboxEvent{}, s.events...), nil
}

func (s *fakeOutboxStore) MarkPublished(_ context.Context, ids []string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil {
		return s.markErr
	}
	s.marked = append(s.marked, ids)
	return nil
}

func (s *fakeOutboxStore) markedBatches() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marked
}

type fakeEncoder struct{}

func (fakeEncoder) SchemaID(string) (int, error) { return 42, nil }

func (fakeEncoder) Encode(id int, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0x00
	out[1] = byte(id >> 24)
	out[2] = byte(id >> 16)
	out[3] = byte(id >> 8)
	out[4] = byte(id)
	copy(out[5:], payload)
	return out
}

type fakePublisher struct {
	mu    sync.Mutex
	recs  []*kgo.Record
	calls int
	failN int // fail the first failN publishes
}

func (p *fakePublisher) PublishSync(_ context.Context, rec *kgo.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failN {
		return errors.New("broker down")
	}
	p.recs = append(p.recs, rec)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.recs)
}

func outboxEvent(id string) payment.OutboxEvent {
	return payment.OutboxEvent{EventID: id, RecordType: "orderflow.payment.PaymentCaptured", AggregateID: "ord-1", Payload: []byte("x")}
}

// The pending gauge refreshes on EVERY flush tick — including down to 0 —
// and the published counter moves once per acked event with the topic label.
func TestRelayRefreshesPendingGaugeAndCountsPublished(t *testing.T) {
	m, _ := newTestMetrics()
	store := &fakeOutboxStore{pending: 5, events: []payment.OutboxEvent{outboxEvent("e1"), outboxEvent("e2")}}
	pub := &fakePublisher{}
	r := &Relay{pub: pub, store: store, serde: fakeEncoder{}, topic: "payment.events", interval: time.Millisecond, m: m}
	ctx := context.Background()

	if err := r.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := testutil.ToFloat64(m.OutboxPendingGauge()); got != 5 {
		t.Errorf("outbox_pending = %v, want 5", got)
	}
	if got := labeled(t, m.OutboxPublishedCounter(), metrics.ServiceName, "payment.events"); got != 2 {
		t.Errorf("outbox_published_total = %v, want 2", got)
	}
	if pub.count() != 2 {
		t.Errorf("publishes = %d, want 2", pub.count())
	}
	if batches := store.markedBatches(); len(batches) != 1 || len(batches[0]) != 2 {
		t.Errorf("mark batches = %v, want one batch of 2", batches)
	}

	// Second tick with an empty queue: the gauge must FALL back to 0.
	store.pending = 0
	store.events = nil
	if err := r.flush(ctx); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if got := testutil.ToFloat64(m.OutboxPendingGauge()); got != 0 {
		t.Errorf("outbox_pending = %v, want 0 after the queue drains", got)
	}
	if got := labeled(t, m.OutboxPublishedCounter(), metrics.ServiceName, "payment.events"); got != 2 {
		t.Errorf("outbox_published_total = %v, want still 2 (nothing new published)", got)
	}
}

// A publish that never acks is not a publish: the counter counts only the
// acked prefix and the batch is not marked (it will re-publish — the
// at-least-once contract).
func TestRelayPublishFailureCountsOnlyAckedEvents(t *testing.T) {
	m, _ := newTestMetrics()
	store := &fakeOutboxStore{pending: 2, events: []payment.OutboxEvent{outboxEvent("e1"), outboxEvent("e2")}}
	pub := &fakePublisher{failN: 1} // first publish fails
	r := &Relay{pub: pub, store: store, serde: fakeEncoder{}, topic: "payment.events", interval: time.Millisecond, m: m}

	if err := r.flush(context.Background()); err == nil {
		t.Fatal("a failed publish must fail the flush")
	}
	if got := labeled(t, m.OutboxPublishedCounter(), metrics.ServiceName, "payment.events"); got != 0 {
		t.Errorf("outbox_published_total = %v, want 0 (nothing acked)", got)
	}
	if batches := store.markedBatches(); len(batches) != 0 {
		t.Errorf("mark batches = %v, want 0 (unacked batch must not be marked)", batches)
	}
}

// --- the pinned contract, verbatim: exact names, label sets and label order
// as gathered from the registry after one of each path ran. This is the test
// that fails if anyone drifts from odd/tasks/order-flow-m3.md.

func TestPinnedMetricContractExposition(t *testing.T) {
	m, reg := newTestMetrics()

	// One of each path: processed + absorbed events, one poison through the
	// composition (3 blocked attempts, 1 park), a pending gauge of 5 and 2
	// acked publishes.
	store := newMemStore()
	c := &Consumer{
		svc: payment.NewService(&memTx{store}, payment.NewSimulatedGateway(), events.NewEncoder()),
		dec: events.NewDecoder(),
		m:   m,
	}
	ctx := context.Background()
	if err := c.handle(ctx, stockReservedRecord(t, "evt-1")); err != nil {
		t.Fatalf("fresh handle: %v", err)
	}
	if err := c.handle(ctx, stockReservedRecord(t, "evt-1")); err != nil {
		t.Fatalf("replayed handle: %v", err)
	}
	dlq := &fakeDLQ{}
	cc, err := NewConsumer([]string{"localhost:9092"},
		payment.NewService(&memTx{newMemStore()}, payment.NewSimulatedGateway(), events.NewEncoder()),
		events.NewDecoder(), nil, dlq, m)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	poison := stockReservedRecord(t, "evt-poison")
	poison.Value = []byte("garbage")
	if err := cc.handler(ctx, poison); err != nil {
		t.Fatalf("poison through composition: %v", err)
	}
	ostore := &fakeOutboxStore{pending: 5, events: []payment.OutboxEvent{outboxEvent("e1"), outboxEvent("e2")}}
	r := &Relay{pub: &fakePublisher{}, store: ostore, serde: fakeEncoder{}, topic: "payment.events", interval: time.Millisecond, m: m}
	if err := r.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	expected := `
# HELP orderflow_dlq_parked_total Records parked on <topic>.<group>.dlq after exhausting the retry budget (parent of alert S4).
# TYPE orderflow_dlq_parked_total counter
orderflow_dlq_parked_total{group="payment.inventory-events",service="payment",topic="inventory.events"} 1
# HELP orderflow_events_total Handled consumer records by outcome: processed (fresh) or absorbed (duplicate-marker replay / non-matching pass-through).
# TYPE orderflow_events_total counter
orderflow_events_total{event_type="orderflow.inventory.StockReserved",group="payment.inventory-events",outcome="absorbed",service="payment",topic="inventory.events"} 1
orderflow_events_total{event_type="orderflow.inventory.StockReserved",group="payment.inventory-events",outcome="processed",service="payment",topic="inventory.events"} 1
# HELP orderflow_outbox_pending Unpublished outbox rows (published_at IS NULL), refreshed on each relay tick (parent of the L6 head-of-line alert).
# TYPE orderflow_outbox_pending gauge
orderflow_outbox_pending{service="payment"} 5
# HELP orderflow_outbox_published_total Outbox events successfully published (produced and acked) by the relay.
# TYPE orderflow_outbox_published_total counter
orderflow_outbox_published_total{service="payment",topic="payment.events"} 2
# HELP orderflow_retries_total Blocked handler attempts inside the blocking-retry wrapper (1 initial + 2 retries).
# TYPE orderflow_retries_total counter
orderflow_retries_total{group="payment.inventory-events",service="payment"} 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"orderflow_events_total", "orderflow_retries_total", "orderflow_dlq_parked_total",
		"orderflow_outbox_pending", "orderflow_outbox_published_total"); err != nil {
		t.Fatalf("pinned contract drifted:\n%v", err)
	}
}
