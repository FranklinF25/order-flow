package kafka

// M3 metric contract tests: every pinned label path of the inventory service,
// proven through the real wrappers and the real application service — the
// consumers classify processed/absorbed, WithRetries counts blocked attempts
// and DLQ parks, the relay refreshes the pending gauge and counts publishes.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/FranklinF25/order-flow/services/inventory/internal/events"
	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
	"github.com/FranklinF25/order-flow/services/inventory/internal/metrics"
)

// --- domain fakes (in-memory Store/Tx/Encoder driving the real Service) ----

type metricsStore struct {
	processed    map[string]bool
	stock        map[string]int
	reservations []inventory.Reservation
}

func newMetricsStore(stock map[string]int) *metricsStore {
	return &metricsStore{processed: map[string]bool{}, stock: stock}
}

func (f *metricsStore) MarkProcessed(_ context.Context, eventID string) error {
	if f.processed[eventID] {
		return inventory.ErrDuplicateEvent
	}
	f.processed[eventID] = true
	return nil
}

func (f *metricsStore) GetStock(_ context.Context, sku string) (*inventory.Stock, error) {
	if n, ok := f.stock[sku]; ok {
		return &inventory.Stock{SKU: sku, Available: n}, nil
	}
	return nil, inventory.ErrStockNotFound
}

func (f *metricsStore) ListStock(_ context.Context) ([]inventory.Stock, error) { return nil, nil }

func (f *metricsStore) DecrementStock(_ context.Context, sku string, qty int) error {
	f.stock[sku] -= qty
	return nil
}

func (f *metricsStore) SaveReservation(_ context.Context, r inventory.Reservation) error {
	f.reservations = append(f.reservations, r)
	return nil
}

func (f *metricsStore) GetReservation(_ context.Context, orderID string) (*inventory.Reservation, error) {
	for _, r := range f.reservations {
		if r.OrderID == orderID {
			copy := r // mirrors SELECT: caller mutations must not leak into the store
			return &copy, nil
		}
	}
	return nil, inventory.ErrReservationNotFound
}

func (f *metricsStore) ReleaseReservation(_ context.Context, orderID string) (bool, error) {
	for i := range f.reservations {
		if f.reservations[i].OrderID == orderID {
			if f.reservations[i].Status != inventory.StatusReserved {
				return false, nil
			}
			f.reservations[i].Status = inventory.StatusReleased
			return true, nil
		}
	}
	return false, nil
}

func (f *metricsStore) IncrementStock(_ context.Context, sku string, qty int) error {
	f.stock[sku] += qty
	return nil
}

func (f *metricsStore) AppendOutbox(_ context.Context, _ inventory.OutboxEvent) error { return nil }

type metricsTx struct{ store *metricsStore }

func (t *metricsTx) WithinTx(ctx context.Context, fn func(context.Context, inventory.Store) error) error {
	return fn(ctx, t.store)
}

type metricsEncoder struct{}

func (metricsEncoder) StockReserved(inventory.Reservation) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

func (metricsEncoder) StockUnavailable(string, []inventory.Shortage) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

func (metricsEncoder) StockReleased(string, []inventory.Item) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

// --- wire helpers: realistic consumed records (Confluent envelope + Avro) ---

// metricRecord wraps an Avro payload in the Confluent wire envelope
// (magic byte 0x00 + schema id — the id is irrelevant: decode is embedded)
// and stamps the producer's headers.
func metricRecord(topic, eventType, eventID string, schemaText string, payload any) (*kgo.Record, error) {
	schema, err := avro.Parse(schemaText)
	if err != nil {
		return nil, err
	}
	body, err := avro.Marshal(schema, payload)
	if err != nil {
		return nil, err
	}
	value := append([]byte{0x00, 0, 0, 0, 1}, body...)
	return &kgo.Record{
		Topic: topic,
		Key:   []byte("ord-1"),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "event_id", Value: []byte(eventID)},
		},
	}, nil
}

func orderPlacedRecord(eventID, orderID string) (*kgo.Record, error) {
	schema, err := events.SchemaText("order-placed")
	if err != nil {
		return nil, err
	}
	return metricRecord("order.events", events.OrderPlacedType, eventID, schema, events.OrderPlaced{
		EventID: eventID, OrderID: orderID, OccurredAt: "2026-10-25T00:00:00Z",
		CustomerID: "cus-1",
		Items:      []events.OrderItem{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}},
		TotalCents: 100,
	})
}

func paymentFailedRecord(eventID, orderID string) (*kgo.Record, error) {
	schema, err := events.SchemaText("payment-failed")
	if err != nil {
		return nil, err
	}
	return metricRecord("payment.events", events.PaymentFailedType, eventID, schema, events.PaymentFailed{
		EventID: eventID, OrderID: orderID, OccurredAt: "2026-10-25T00:00:00Z",
		AmountCents: 100, FailureReason: "CARD_DECLINED",
	})
}

func eventsCounter(group, topic, eventType, outcome string) float64 {
	return testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, group, topic, eventType, outcome))
}

// resetMetrics isolates each test from the package-level collectors: the
// pinned metrics are globals, so earlier tests' increments accumulate on the
// same children unless every vec is reset between tests (assertions here
// read absolute values).
func resetMetrics() {
	metrics.EventsTotal.Reset()
	metrics.RetriesTotal.Reset()
	metrics.DlqParkedTotal.Reset()
	metrics.OutboxPublishedTotal.Reset()
	metrics.SetOutboxPending(0)
}

// --- order.events consumer: processed + absorbed (duplicate, pass-through) --

func TestOrderConsumerHandleClassifiesProcessedAndAbsorbed(t *testing.T) {
	resetMetrics()
	store := newMetricsStore(map[string]int{"SKU-A": 10})
	c := &Consumer{svc: inventory.NewService(&metricsTx{store}, store, metricsEncoder{}), group: "inventory.order-events"}

	rec, err := orderPlacedRecord("evt-op-1", "ord-1")
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	if err := c.handle(context.Background(), rec); err != nil {
		t.Fatalf("first OrderPlaced: %v", err)
	}
	if got := eventsCounter("inventory.order-events", "order.events", events.OrderPlacedType, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("processed = %v, want 1 after an applied reservation", got)
	}

	// Replayed event_id: the processed_events guard absorbs it inside the tx.
	if err := c.handle(context.Background(), rec); err != nil {
		t.Fatalf("duplicate OrderPlaced: %v", err)
	}
	if got := eventsCounter("inventory.order-events", "order.events", events.OrderPlacedType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed (duplicate) = %v, want 1", got)
	}

	// Pass-through: a non-matching event_type never reaches the domain and
	// counts as absorbed under its own event_type label.
	passThrough := &kgo.Record{
		Topic:   "order.events",
		Headers: []kgo.RecordHeader{{Key: "event_type", Value: []byte("orderflow.order.OrderConfirmed")}},
	}
	if err := c.handle(context.Background(), passThrough); err != nil {
		t.Fatalf("pass-through record: %v", err)
	}
	if got := eventsCounter("inventory.order-events", "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed (pass-through) = %v, want 1", got)
	}
}

// --- payment.events consumer: processed + all three absorbed variants -------

func TestPaymentConsumerHandleClassifiesProcessedAndAbsorbed(t *testing.T) {
	resetMetrics()
	store := newMetricsStore(map[string]int{"SKU-A": 10})
	r, err := inventory.NewReservation("ord-1", "cus-1", 100, []inventory.Item{{SKU: "SKU-A", Qty: 1}})
	if err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	store.reservations = append(store.reservations, r)
	c := &PaymentConsumer{svc: inventory.NewService(&metricsTx{store}, store, metricsEncoder{}), group: "inventory.payment-events"}
	const et = events.PaymentFailedType

	// Applied compensation: the reservation flips RESERVED→RELEASED.
	rec, err := paymentFailedRecord("evt-pf-1", "ord-1")
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	if err := c.handle(context.Background(), rec); err != nil {
		t.Fatalf("first PaymentFailed: %v", err)
	}
	if got := eventsCounter("inventory.payment-events", "payment.events", et, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("processed = %v, want 1", got)
	}

	// Absorbed: duplicate event (processed_events guard).
	if err := c.handle(context.Background(), rec); err != nil {
		t.Fatalf("duplicate PaymentFailed: %v", err)
	}
	// Absorbed: already-RELEASED (new event id, same order — nothing to release).
	release, err := paymentFailedRecord("evt-pf-2", "ord-1")
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	if err := c.handle(context.Background(), release); err != nil {
		t.Fatalf("already-released PaymentFailed: %v", err)
	}
	// Absorbed: unknown order (never reserved — shortage path).
	unknown, err := paymentFailedRecord("evt-pf-3", "ord-404")
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	if err := c.handle(context.Background(), unknown); err != nil {
		t.Fatalf("unknown-order PaymentFailed: %v", err)
	}
	if got := eventsCounter("inventory.payment-events", "payment.events", et, metrics.OutcomeAbsorbed); got != 3 {
		t.Errorf("absorbed = %v, want 3 (duplicate + already-RELEASED + unknown order)", got)
	}

	// Pass-through: PaymentCaptured never reaches the domain.
	passThrough := &kgo.Record{
		Topic:   "payment.events",
		Headers: []kgo.RecordHeader{{Key: "event_type", Value: []byte("orderflow.payment.PaymentCaptured")}},
	}
	if err := c.handle(context.Background(), passThrough); err != nil {
		t.Fatalf("pass-through record: %v", err)
	}
	if got := eventsCounter("inventory.payment-events", "payment.events", "orderflow.payment.PaymentCaptured", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("absorbed (pass-through) = %v, want 1", got)
	}
}

// --- retries/DLQ wrapper: blocked attempts + parks --------------------------

func TestWithRetriesCountsBlockedAttemptsAndParks(t *testing.T) {
	resetMetrics()
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 3, err: errors.New("db unreachable")}
	wrapped := WithRetries(dlq, 0, "inventory.order-events", h.handle) // poison: exhausts the budget

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("parked record must return nil (offset commits), got %v", err)
	}
	if got := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, "inventory.order-events")); got != 2 {
		t.Errorf("retries_total = %v, want 2 (attempts 2 and 3 blocked after the initial failure)", got)
	}
	if got := testutil.ToFloat64(metrics.DlqParkedTotal.WithLabelValues(metrics.ServiceName, "inventory.order-events", "order.events")); got != 1 {
		t.Errorf("dlq_parked_total = %v, want 1 after the budget is exhausted", got)
	}
}

func TestWithRetriesCountsSingleBlockedRetryWithoutPark(t *testing.T) {
	resetMetrics()
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 1, err: errors.New("transient")}
	wrapped := WithRetries(dlq, 0, "inventory.payment-events", h.handle) // recovers on attempt 2

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("recovered record must return nil, got %v", err)
	}
	if got := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, "inventory.payment-events")); got != 1 {
		t.Errorf("retries_total = %v, want 1 (only the second attempt was blocked)", got)
	}
	if got := testutil.ToFloat64(metrics.DlqParkedTotal.WithLabelValues(metrics.ServiceName, "inventory.payment-events", "order.events")); got != 0 {
		t.Errorf("dlq_parked_total = %v, want 0 (no park on recovery)", got)
	}
}

// --- outbox relay: pending gauge + published counter ------------------------

type relayStoreFake struct {
	mu          sync.Mutex
	count       int64
	unpublished []inventory.OutboxEvent
	marked      [][]string
}

func (f *relayStoreFake) CountUnpublished(context.Context) (int64, error) { return f.count, nil }

func (f *relayStoreFake) FetchUnpublished(context.Context, int) ([]inventory.OutboxEvent, error) {
	return f.unpublished, nil
}

func (f *relayStoreFake) MarkPublished(_ context.Context, ids []string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, ids)
	return nil
}

type relayProducerFake struct {
	failAt int // 1-based produce call that fails; 0 = never fail
	calls  int
}

func (f *relayProducerFake) Produce(context.Context, *kgo.Record) error {
	f.calls++
	if f.failAt == f.calls {
		return errors.New("broker unavailable")
	}
	return nil
}

func (f *relayProducerFake) Close() {}

func relayWithBacklog(count int64, n int) *Relay {
	unpublished := make([]inventory.OutboxEvent, n)
	for i := range unpublished {
		unpublished[i] = inventory.OutboxEvent{
			EventID:     "evt-" + string(rune('a'+i)),
			RecordType:  events.StockReservedType,
			AggregateID: "ord-1",
			Payload:     []byte("avro-bytes"),
		}
	}
	return &Relay{
		producer: &relayProducerFake{},
		store:    &relayStoreFake{count: count, unpublished: unpublished},
		serde:    &Serde{ids: map[string]int{events.StockReservedType: 1}},
		topic:    "inventory.events",
	}
}

func TestRelayFlushRefreshesPendingAndCountsPublished(t *testing.T) {
	resetMetrics()
	r := relayWithBacklog(2, 2)

	if err := r.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := testutil.ToFloat64(metrics.OutboxPending); got != 2 {
		t.Errorf("outbox_pending = %v, want 2 after the tick's COUNT(*)", got)
	}
	if got := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "inventory.events")); got != 2 {
		t.Errorf("outbox_published_total = %v, want 2 (one per broker-acked event)", got)
	}

	// Next tick with a drained backlog: the gauge must fall back to 0 —
	// the refresh happens even when there is nothing to publish.
	r.store = &relayStoreFake{count: 0}
	if err := r.flush(context.Background()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if got := testutil.ToFloat64(metrics.OutboxPending); got != 0 {
		t.Errorf("outbox_pending after drain = %v, want 0 (refreshed every tick)", got)
	}
	if got := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "inventory.events")); got != 2 {
		t.Errorf("outbox_published_total after drain = %v, want 2 (nothing new published)", got)
	}
}

// The head-of-line stall is exactly when the pending gauge must be visible:
// a failed produce keeps the backlog, and the gauge must still reflect it.
func TestRelayFlushProducerFailureKeepsPendingVisible(t *testing.T) {
	resetMetrics()
	r := relayWithBacklog(2, 2)
	r.producer = &relayProducerFake{failAt: 1}

	if err := r.flush(context.Background()); err == nil {
		t.Fatal("a failed produce must surface the error (relay retries next tick)")
	}
	if got := testutil.ToFloat64(metrics.OutboxPending); got != 2 {
		t.Errorf("outbox_pending = %v, want 2 (refreshed before the produce attempts)", got)
	}
	if got := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "inventory.events")); got != 0 {
		t.Errorf("outbox_published_total = %v, want 0 (nothing was acked)", got)
	}
}
