package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/order/internal/events"
	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// ---- order-port fakes: a REAL order.Service drives the full consumer chain
// below (typed handlers + domain absorb paths), no infrastructure involved.

type metricsOrderStore struct {
	mu        sync.Mutex
	orders    map[string]*order.Order
	processed map[string]bool
	outbox    []order.OutboxEvent
}

func newMetricsOrderStore() *metricsOrderStore {
	return &metricsOrderStore{orders: map[string]*order.Order{}, processed: map[string]bool{}}
}

func (f *metricsOrderStore) SaveOrder(_ context.Context, o *order.Order) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *o
	f.orders[o.ID] = &cp
	return nil
}

func (f *metricsOrderStore) GetOrder(_ context.Context, id string) (*order.Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.orders[id]; ok {
		cp := *o
		return &cp, nil
	}
	return nil, order.ErrOrderNotFound
}

func (f *metricsOrderStore) UpdateOrder(_ context.Context, o *order.Order) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *o
	f.orders[o.ID] = &cp
	return nil
}

func (f *metricsOrderStore) AppendOutbox(_ context.Context, e order.OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outbox = append(f.outbox, e)
	return nil
}

func (f *metricsOrderStore) InsertProcessedEvent(_ context.Context, eventID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.processed[eventID] {
		return false, nil
	}
	f.processed[eventID] = true
	return true, nil
}

type metricsOrderTx struct{ store *metricsOrderStore }

func (f *metricsOrderTx) WithinTx(ctx context.Context, fn func(context.Context, order.Store) error) error {
	return fn(ctx, f.store)
}

type metricsOrderEncoder struct{}

func (metricsOrderEncoder) OrderPlaced(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-p-" + o.ID, RecordType: events.OrderPlacedType, AggregateID: o.ID}, nil
}

func (metricsOrderEncoder) OrderConfirmed(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-c-" + o.ID, RecordType: events.OrderConfirmedType, AggregateID: o.ID}, nil
}

func (metricsOrderEncoder) OrderCancelled(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-x-" + o.ID, RecordType: events.OrderCancelledType, AggregateID: o.ID}, nil
}

// ---- wire helpers: real Avro records in the Confluent envelope, decoded by
// the embedded schemas exactly like production traffic.

func wirePayload(t *testing.T, schemaName string, v any) []byte {
	t.Helper()
	text, err := events.SchemaText(schemaName)
	if err != nil {
		t.Fatalf("schema %s: %v", schemaName, err)
	}
	schema, err := avro.Parse(text)
	if err != nil {
		t.Fatalf("parse %s: %v", schemaName, err)
	}
	body, err := avro.Marshal(schema, v)
	if err != nil {
		t.Fatalf("marshal %s: %v", schemaName, err)
	}
	return (&Serde{}).Encode(1, body) // Encode is a pure function: no registry
}

func stockReservedRecord(t *testing.T, eventID, orderID string) *kgo.Record {
	t.Helper()
	return &kgo.Record{
		Topic: "inventory.events",
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(events.StockReservedType)},
			{Key: "event_id", Value: []byte(eventID)},
		},
		Value: wirePayload(t, "stock-reserved", events.StockReserved{
			EventID:    eventID,
			OrderID:    orderID,
			OccurredAt: "2026-10-25T00:00:00Z",
			Items:      []events.StockItem{{SKU: "SKU-A", Qty: 1}},
			TotalCents: 100,
		}),
	}
}

func stockUnavailableRecord(t *testing.T, eventID, orderID string) *kgo.Record {
	t.Helper()
	return &kgo.Record{
		Topic: "inventory.events",
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(events.StockUnavailableType)},
			{Key: "event_id", Value: []byte(eventID)},
		},
		Value: wirePayload(t, "stock-unavailable", events.StockUnavailable{
			EventID:    eventID,
			OrderID:    orderID,
			OccurredAt: "2026-10-25T00:00:00Z",
			Shortages:  []events.Shortage{{SKU: "SKU-A", Requested: 1, Available: 0}},
		}),
	}
}

// fullChain builds the production composition (WithMetrics → fast-gate →
// retries/DLQ → chained typed handlers) over a REAL order service.
func fullChain(m *metrics.Metrics, gate FastGate, dlq DLQProducer, svc *order.Service) Handler {
	return WithMetrics(m, "order.inventory-events",
		WithFastGate(gate,
			WithRetries(dlq, m, 0, "order.inventory-events",
				Chain(StockReservedHandler(svc), StockUnavailableHandler(svc)))))
}

func eventsCounter(m *metrics.Metrics, topic, eventType, outcome string) float64 {
	return testutil.ToFloat64(m.EventsTotal.WithLabelValues(
		metrics.ServiceName, "order.inventory-events", topic, eventType, outcome))
}

// Happy path end to end: a real StockReserved record transitions the order to
// AWAITING_PAYMENT and counts exactly one processed event.
func TestWithMetricsProcessedEndToEnd(t *testing.T) {
	m := metrics.New()
	store := newMetricsOrderStore()
	o, err := order.NewOrder("ord-1", "cus-1", []order.Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOrder(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	svc := order.NewService(&metricsOrderTx{store}, store, metricsOrderEncoder{})

	h := fullChain(m, nil, &fakeDLQ{}, svc)
	if err := h(context.Background(), stockReservedRecord(t, "evt-sr-1", "ord-1")); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("orderflow_events_total{processed} = %v, want 1", got)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeAbsorbed); got != 0 {
		t.Errorf("orderflow_events_total{absorbed} = %v, want 0", got)
	}
	if got := store.orders["ord-1"].Status; got != order.StatusAwaitingPayment {
		t.Errorf("status = %q, want AWAITING_PAYMENT (the metric must sit on a real transition)", got)
	}
}

// A DB-guard duplicate (same event id twice) processes once and absorbs once.
func TestWithMetricsDuplicateEventCountsProcessedThenAbsorbed(t *testing.T) {
	m := metrics.New()
	store := newMetricsOrderStore()
	o, _ := order.NewOrder("ord-1", "cus-1", []order.Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err := store.SaveOrder(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	svc := order.NewService(&metricsOrderTx{store}, store, metricsOrderEncoder{})
	h := fullChain(m, nil, &fakeDLQ{}, svc)

	if err := h(context.Background(), stockReservedRecord(t, "evt-sr-dup", "ord-1")); err != nil {
		t.Fatal(err)
	}
	if err := h(context.Background(), stockReservedRecord(t, "evt-sr-dup", "ord-1")); err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("processed = %v, want 1", got)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed = %v, want 1 (the domain duplicate)", got)
	}
}

// Compensation absorb (M2 semantics): StockUnavailable racing to
// AWAITING_PAYMENT is a safe no-op — and counts as absorbed, not processed.
func TestWithMetricsCompensationAbsorbCountsAbsorbed(t *testing.T) {
	m := metrics.New()
	store := newMetricsOrderStore()
	o, _ := order.NewOrder("ord-1", "cus-1", []order.Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err := o.OnStockReserved(); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOrder(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	svc := order.NewService(&metricsOrderTx{store}, store, metricsOrderEncoder{})

	h := fullChain(m, nil, &fakeDLQ{}, svc)
	if err := h(context.Background(), stockUnavailableRecord(t, "evt-su-1", "ord-1")); err != nil {
		t.Fatalf("wrong-state compensation must absorb without error, got %v", err)
	}
	if got := eventsCounter(m, "inventory.events", events.StockUnavailableType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed = %v, want 1", got)
	}
	if got := eventsCounter(m, "inventory.events", events.StockUnavailableType, metrics.OutcomeProcessed); got != 0 {
		t.Errorf("processed = %v, want 0", got)
	}
	if got := store.orders["ord-1"].Status; got != order.StatusAwaitingPayment {
		t.Errorf("status = %q, want unchanged AWAITING_PAYMENT", got)
	}
}

// Fast-gate absorb: the replay window absorbs BEFORE any handler dispatch;
// counted absorbed, and the domain never sees the record.
func TestWithMetricsGateAbsorbCountsAbsorbed(t *testing.T) {
	m := metrics.New()
	store := newMetricsOrderStore()
	o, _ := order.NewOrder("ord-1", "cus-1", []order.Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err := store.SaveOrder(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	svc := order.NewService(&metricsOrderTx{store}, store, metricsOrderEncoder{})
	gate := newFakeGate()
	gate.store["evt-sr-1"] = true

	h := fullChain(m, gate, &fakeDLQ{}, svc)
	if err := h(context.Background(), stockReservedRecord(t, "evt-sr-1", "ord-1")); err != nil {
		t.Fatal(err)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed = %v, want 1 (gate hit)", got)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeProcessed); got != 0 {
		t.Errorf("processed = %v, want 0", got)
	}
	if got := store.orders["ord-1"].Status; got != order.StatusAwaitingStock {
		t.Errorf("status = %q, want untouched AWAITING_STOCK", got)
	}
}

// Non-matching event_type pass-throughs classify as absorbed (contract rule):
// no subscribed handler claims the record, yet the offset still commits.
func TestWithMetricsPassThroughCountsAbsorbed(t *testing.T) {
	m := metrics.New()
	store := newMetricsOrderStore()
	svc := order.NewService(&metricsOrderTx{store}, store, metricsOrderEncoder{})

	h := fullChain(m, nil, &fakeDLQ{}, svc)
	rec := stockReservedRecord(t, "evt-other", "ord-1")
	rec.Headers = []kgo.RecordHeader{{Key: "event_type", Value: []byte(events.OrderPlacedType)}} // not subscribed here
	if err := h(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if got := eventsCounter(m, "inventory.events", events.OrderPlacedType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed = %v, want 1 (pass-through)", got)
	}
	if got := eventsCounter(m, "inventory.events", events.OrderPlacedType, metrics.OutcomeProcessed); got != 0 {
		t.Errorf("processed = %v, want 0", got)
	}
}

// A failed record is NOT counted: it stays uncommitted and redelivers —
// counting it would double-count the same offset.
func TestWithMetricsFailedRecordCountsNothing(t *testing.T) {
	m := metrics.New()
	h := WithMetrics(m, "order.inventory-events", func(context.Context, *kgo.Record) error {
		return errors.New("db unreachable")
	})
	if err := h(context.Background(), stockReservedRecord(t, "evt-fail", "ord-1")); err == nil {
		t.Fatal("handler error must propagate (no commit path)")
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeProcessed); got != 0 {
		t.Errorf("processed = %v, want 0 (failed records count nothing)", got)
	}
	if got := eventsCounter(m, "inventory.events", events.StockReservedType, metrics.OutcomeAbsorbed); got != 0 {
		t.Errorf("absorbed = %v, want 0 (failed records count nothing)", got)
	}
}

// Retries: every BLOCKED attempt (a failure that enters the §7 backoff)
// counts orderflow_retries_total; the final parking attempt is not a retry.
func TestWithRetriesCountsBlockedAttempts(t *testing.T) {
	m := metrics.New()
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 3, err: errors.New("poison")}
	wrapped := WithRetries(dlq, m, 0, "order.inventory-events", h.handle)

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("parked record must return nil, got %v", err)
	}
	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(metrics.ServiceName, "order.inventory-events")); got != 2 {
		t.Errorf("orderflow_retries_total = %v, want 2 (attempts 1 and 2 blocked; attempt 3 parks)", got)
	}
	if got := testutil.ToFloat64(m.DLQParkedTotal.WithLabelValues(metrics.ServiceName, "order.inventory-events", "inventory.events")); got != 1 {
		t.Errorf("orderflow_dlq_parked_total = %v, want 1", got)
	}
}

// Success on the first attempt burns no retry budget and parks nothing.
func TestWithRetriesCountsNothingOnHealthyPath(t *testing.T) {
	m := metrics.New()
	dlq := &fakeDLQ{}
	h := &flakyHandler{}
	wrapped := WithRetries(dlq, m, 0, "order.payment-events", h.handle)

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("healthy path: %v", err)
	}
	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(metrics.ServiceName, "order.payment-events")); got != 0 {
		t.Errorf("orderflow_retries_total = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.DLQParkedTotal.WithLabelValues(metrics.ServiceName, "order.payment-events", "inventory.events")); got != 0 {
		t.Errorf("orderflow_dlq_parked_total = %v, want 0", got)
	}
}

// A transient failure that recovers on attempt 3 still logged its two
// blocked attempts but never parks.
func TestWithRetriesTransientCountsRetriesWithoutDLQ(t *testing.T) {
	m := metrics.New()
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 2, err: errors.New("transient deadlock")}
	wrapped := WithRetries(dlq, m, 0, "order.inventory-events", h.handle)

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(metrics.ServiceName, "order.inventory-events")); got != 2 {
		t.Errorf("orderflow_retries_total = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.DLQParkedTotal.WithLabelValues(metrics.ServiceName, "order.inventory-events", "inventory.events")); got != 0 {
		t.Errorf("orderflow_dlq_parked_total = %v, want 0 (recovered)", got)
	}
}

// Contract deviation (pinned here): a DLQ-parked record counts
// orderflow_events_total under outcome="absorbed" — it never advanced the
// saga; the alert-bearing series is orderflow_dlq_parked_total.
func TestWithMetricsParkedRecordCountsAbsorbed(t *testing.T) {
	m := metrics.New()
	dlq := &fakeDLQ{}
	handler := func(ctx context.Context, rec *kgo.Record) error {
		metrics.MarkMatched(ctx) // a subscribed handler claimed and failed the record
		return errors.New("db unreachable")
	}
	wrapped := WithMetrics(m, "order.inventory-events",
		WithRetries(dlq, m, 0, "order.inventory-events", handler))

	rec := poisonRecord() // event_type header: orderflow.inventory.StockReserved
	if err := wrapped(context.Background(), rec); err != nil {
		t.Fatalf("parked record must return nil, got %v", err)
	}
	if got := eventsCounter(m, "inventory.events", "orderflow.inventory.StockReserved", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed = %v, want 1 (parked deviation)", got)
	}
	if got := eventsCounter(m, "inventory.events", "orderflow.inventory.StockReserved", metrics.OutcomeProcessed); got != 0 {
		t.Errorf("processed = %v, want 0", got)
	}
}

// ---- relay: orderflow_outbox_pending refresh + orderflow_outbox_published.

type fakeOutboxStore struct {
	mu          sync.Mutex
	pending     []order.OutboxEvent
	published   map[string]bool
	countCalled int
}

func newFakeOutboxStore(events ...order.OutboxEvent) *fakeOutboxStore {
	return &fakeOutboxStore{pending: events, published: map[string]bool{}}
}

func (f *fakeOutboxStore) FetchUnpublished(_ context.Context, limit int) ([]order.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []order.OutboxEvent
	for _, e := range f.pending {
		if !f.published[e.EventID] {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeOutboxStore) MarkPublished(_ context.Context, eventIDs []string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range eventIDs {
		f.published[id] = true
	}
	return nil
}

func (f *fakeOutboxStore) CountUnpublished(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.countCalled++
	var n int64
	for _, e := range f.pending {
		if !f.published[e.EventID] {
			n++
		}
	}
	return n, nil
}

type fakeSchemaIDer struct{}

func (fakeSchemaIDer) SchemaID(string) (int, error) { return 1, nil }
func (fakeSchemaIDer) Encode(_ int, payload []byte) []byte { return payload }

type fakeProducer struct {
	mu        sync.Mutex
	failFirst int
	calls     int
	produced  int
}

func (f *fakeProducer) Close() {}

func (f *fakeProducer) ProduceSync(_ context.Context, recs ...*kgo.Record) kgo.ProduceResults {
	f.mu.Lock()
	f.calls++
	fail := f.calls <= f.failFirst
	f.produced += len(recs)
	f.mu.Unlock()
	results := make(kgo.ProduceResults, 0, len(recs))
	for _, rec := range recs {
		r := kgo.ProduceResult{Record: rec}
		if fail {
			r.Err = errors.New("broker unreachable")
		}
		results = append(results, r)
	}
	return results
}

func outboxEvent(id string) order.OutboxEvent {
	return order.OutboxEvent{EventID: id, RecordType: events.OrderConfirmedType, AggregateID: "ord-1", Payload: []byte("avro")}
}

// waitFor polls cond until it holds or the timeout elapses (ticker-driven
// relay paths need a bounded wait; the timeout is the RED/GREEN failure).
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestRelayRefreshesPendingGaugeAndCountsPublished(t *testing.T) {
	m := metrics.New()
	store := newFakeOutboxStore(outboxEvent("evt-1"), outboxEvent("evt-2"))
	fp := &fakeProducer{}
	r := &Relay{
		client: fp, store: store, serde: fakeSchemaIDer{},
		topic: "order.events", interval: 2 * time.Millisecond, metrics: m,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.OutboxPublished.WithLabelValues(metrics.ServiceName, "order.events")) == 2
	}, "relay never published both events")
	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.OutboxPending.WithLabelValues(metrics.ServiceName)) == 0
	}, "outbox_pending gauge never drained to 0 after publish")
	if fp.produced != 2 {
		t.Errorf("produced records = %d, want 2", fp.produced)
	}
	cancel()
	<-done
}

// Pending refreshes on EVERY tick — even before anything publishes — and a
// produce failure counts zero publishes while the gauge keeps reporting.
func TestRelayPendingRefreshesWhileProduceFailing(t *testing.T) {
	m := metrics.New()
	store := newFakeOutboxStore(outboxEvent("evt-1"))
	fp := &fakeProducer{failFirst: 1000} // every produce fails
	r := &Relay{
		client: fp, store: store, serde: fakeSchemaIDer{},
		topic: "order.events", interval: 2 * time.Millisecond, metrics: m,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.OutboxPending.WithLabelValues(metrics.ServiceName)) == 1
	}, "outbox_pending gauge never refreshed while produces failed")
	if got := testutil.ToFloat64(m.OutboxPublished.WithLabelValues(metrics.ServiceName, "order.events")); got != 0 {
		t.Errorf("orderflow_outbox_published_total = %v, want 0 (acks failed)", got)
	}
	if got := testutil.ToFloat64(m.OutboxPublished.WithLabelValues(metrics.ServiceName, "inventory.events")); got != 0 {
		t.Errorf("wrong-topic series = %v, want 0", got)
	}
	cancel()
	<-done
}

// The relay's store port must expose the pending count (gauge source).
func TestOutboxStorePortHasCountUnpublished(t *testing.T) {
	var _ OutboxStore = (*fakeOutboxStore)(nil)
	store := newFakeOutboxStore(outboxEvent("evt-1"), outboxEvent("evt-2"))
	store.published["evt-1"] = true
	n, err := store.CountUnpublished(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("CountUnpublished = %d, want 1", n)
	}
	if store.countCalled != 1 {
		t.Errorf("count calls = %d, want 1", store.countCalled)
	}
}

// The composition used by main: WithMetrics outermost, gate, retries, chain.
func TestMainCompositionCompiles(t *testing.T) {
	m := metrics.New()
	svc := order.NewService(&metricsOrderTx{newMetricsOrderStore()}, newMetricsOrderStore(), metricsOrderEncoder{})
	h := WithMetrics(m, "order.payment-events",
		WithFastGate(nil,
			WithRetries(&fakeDLQ{}, m, 0, "order.payment-events",
				Chain(PaymentCapturedHandler(svc), PaymentFailedHandler(svc)))))
	if h == nil {
		t.Fatal("nil handler")
	}
}
