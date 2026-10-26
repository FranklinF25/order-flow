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

	"github.com/FranklinF25/order-flow/services/notification/internal/events"
	"github.com/FranklinF25/order-flow/services/notification/internal/metrics"
	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

// ---- fakes for the notification application service (same-package tests) ----

type memStore struct {
	mu            sync.Mutex
	processed     map[string]bool
	notifications []notification.Notification
	outbox        []notification.OutboxEvent
}

func newMemStore() *memStore { return &memStore{processed: map[string]bool{}} }

func (s *memStore) SaveNotification(_ context.Context, n *notification.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notifications = append(s.notifications, *n)
	return nil
}

func (s *memStore) AppendOutbox(_ context.Context, e notification.OutboxEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox = append(s.outbox, e)
	return nil
}

func (s *memStore) InsertProcessedEvent(_ context.Context, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processed[eventID] {
		return false, nil
	}
	s.processed[eventID] = true
	return true, nil
}

type memTx struct{ store *memStore }

func (t *memTx) WithinTx(ctx context.Context, fn func(context.Context, notification.Store) error) error {
	return fn(ctx, t.store)
}

type memPublisher struct{}

func (memPublisher) Publish(_ context.Context, _ notification.Notification) error { return nil }

type memEncoder struct{}

func (memEncoder) NotificationSent(n notification.Notification) (notification.OutboxEvent, error) {
	return notification.OutboxEvent{
		EventID:     "evt-ns-" + n.ID,
		RecordType:  events.NotificationSentType,
		AggregateID: n.OrderID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

// ---- wire-format helpers (confluent header + embedded-schema avro body) ----

func wireRecord(t *testing.T, schemaName, eventType string, value any) *kgo.Record {
	t.Helper()
	text, err := events.SchemaText(schemaName)
	if err != nil {
		t.Fatalf("schema text %s: %v", schemaName, err)
	}
	schema, err := avro.Parse(text)
	if err != nil {
		t.Fatalf("parse %s: %v", schemaName, err)
	}
	payload, err := avro.Marshal(schema, value)
	if err != nil {
		t.Fatalf("marshal %s: %v", schemaName, err)
	}
	return &kgo.Record{
		Topic: orderEventsTopic,
		Value: append([]byte{0x00, 0x00, 0x00, 0x00, 0x01}, payload...), // magic + int32 schema id
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "event_id", Value: []byte(eventIDOf(t, value))},
		},
	}
}

func eventIDOf(t *testing.T, value any) string {
	t.Helper()
	switch v := value.(type) {
	case events.OrderConfirmed:
		return v.EventID
	case events.OrderCancelled:
		return v.EventID
	}
	t.Fatalf("no event id on %T", value)
	return ""
}

// eventsDelta reads one exact events_total series through the pinned labels.
func eventsDelta(t *testing.T, before float64, topic, eventType, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, topic, eventType, outcome)) - before
}

// handle processes OrderConfirmed AND OrderCancelled fresh events: each
// increments orderflow_events_total{service=notification, group, topic,
// event_type, outcome=processed} exactly once (offset-commit decision).
func TestHandleFreshEventsCountProcessed(t *testing.T) {
	svc := notification.NewService(&memTx{newMemStore()}, memPublisher{}, memEncoder{})
	c := &Consumer{svc: svc, dec: events.NewDecoder()}
	ctx := context.Background()

	confirmedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeProcessed))
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeAbsorbed))

	rec := wireRecord(t, "order-confirmed", events.OrderConfirmedType,
		events.OrderConfirmed{EventID: "evt-conf-1", OrderID: "ord-1", OccurredAt: "2026-01-01T00:00:00Z"})
	if err := c.handle(ctx, rec); err != nil {
		t.Fatalf("handle OrderConfirmed: %v", err)
	}

	if got := eventsDelta(t, confirmedBefore, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("events_total{outcome=processed} delta = %v, want 1", got)
	}
	if got := eventsDelta(t, absorbedBefore, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeAbsorbed); got != 0 {
		t.Errorf("events_total{outcome=absorbed} delta = %v, want 0 (fresh event is not absorbed)", got)
	}

	// Cancelled kind: same contract, its own event_type label value.
	cancelledBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderCancelledType, metrics.OutcomeProcessed))
	rec = wireRecord(t, "order-cancelled", events.OrderCancelledType,
		events.OrderCancelled{EventID: "evt-canc-1", OrderID: "ord-2", OccurredAt: "2026-01-01T00:00:00Z", Reason: "PAYMENT_FAILED"})
	if err := c.handle(ctx, rec); err != nil {
		t.Fatalf("handle OrderCancelled: %v", err)
	}
	if got := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderCancelledType, metrics.OutcomeProcessed)) - cancelledBefore; got != 1 {
		t.Errorf("events_total{cancelled,processed} delta = %v, want 1", got)
	}
}

// A duplicate replay (processed_events guard absorbs inside Notify) commits
// its offset with outcome=absorbed — never processed.
func TestHandleDuplicateEventCountsAbsorbed(t *testing.T) {
	store := newMemStore()
	svc := notification.NewService(&memTx{store}, memPublisher{}, memEncoder{})
	c := &Consumer{svc: svc, dec: events.NewDecoder()}
	ctx := context.Background()

	processedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeProcessed))
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeAbsorbed))

	rec := wireRecord(t, "order-confirmed", events.OrderConfirmedType,
		events.OrderConfirmed{EventID: "evt-dup-1", OrderID: "ord-1", OccurredAt: "2026-01-01T00:00:00Z"})
	if err := c.handle(ctx, rec); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := c.handle(ctx, rec); err != nil {
		t.Fatalf("replayed handle: %v", err)
	}

	if got := eventsDelta(t, processedBefore, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeProcessed); got != 1 {
		t.Errorf("events_total{outcome=processed} delta = %v, want 1 (only the fresh event)", got)
	}
	if got := eventsDelta(t, absorbedBefore, orderEventsTopic, events.OrderConfirmedType, metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("events_total{outcome=absorbed} delta = %v, want 1 (the duplicate)", got)
	}
}

// Non-matching event_type records are committed pass-throughs: handled, so
// counted once with outcome=absorbed and their own event_type label.
func TestHandlePassThroughEventTypeCountsAbsorbed(t *testing.T) {
	svc := notification.NewService(&memTx{newMemStore()}, memPublisher{}, memEncoder{})
	c := &Consumer{svc: svc, dec: events.NewDecoder()}
	ctx := context.Background()

	otherType := "orderflow.inventory.StockAdjusted"
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, otherType, metrics.OutcomeAbsorbed))

	rec := wireRecord(t, "order-confirmed", otherType,
		events.OrderConfirmed{EventID: "evt-other-1", OrderID: "ord-9", OccurredAt: "2026-01-01T00:00:00Z"})
	if err := c.handle(ctx, rec); err != nil {
		t.Fatalf("handle pass-through: %v", err)
	}

	if got := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, orderEventsTopic, otherType, metrics.OutcomeAbsorbed)) - absorbedBefore; got != 1 {
		t.Errorf("events_total{pass-through,absorbed} delta = %v, want 1", got)
	}
}

// Poison record: exactly 2 blocked retries (orderflow_retries_total), 1 DLQ
// park (orderflow_dlq_parked_total) and the parked record itself counts as
// ONE handled record with outcome=absorbed (its offset commits via the park).
func TestWithRetriesPoisonCountsRetriesDlqParkAndAbsorbedEvent(t *testing.T) {
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 3, err: errors.New("poison")}
	wrapped := WithRetries(dlq, 0, consumerGroup, h.handle)

	retriesBefore := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup))
	dlqBefore := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events"))
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed))

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("park path must return nil, got %v", err)
	}

	if got := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup)) - retriesBefore; got != 2 {
		t.Errorf("retries_total delta = %v, want 2 (1 initial + 2 blocked retries before the park)", got)
	}
	if got := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events")) - dlqBefore; got != 1 {
		t.Errorf("dlq_parked_total delta = %v, want 1", got)
	}
	if got := eventsDelta(t, absorbedBefore, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed); got != 1 {
		t.Errorf("events_total{parked,absorbed} delta = %v, want 1", got)
	}
}

// Transient recovery: blocked retries count, but no park and no absorbed
// event — the handler succeeded and the consumer's own path owns the
// events_total outcome.
func TestWithRetriesTransientRecoveryCountsRetriesOnly(t *testing.T) {
	dlq := &fakeDLQ{}
	h := &flakyHandler{failFirst: 2, err: errors.New("transient")}
	wrapped := WithRetries(dlq, 0, consumerGroup, h.handle)

	retriesBefore := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup))
	dlqBefore := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events"))
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed))

	if err := wrapped(context.Background(), poisonRecord()); err != nil {
		t.Fatalf("recovery must return nil, got %v", err)
	}

	if got := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup)) - retriesBefore; got != 2 {
		t.Errorf("retries_total delta = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events")) - dlqBefore; got != 0 {
		t.Errorf("dlq_parked_total delta = %v, want 0 (record recovered)", got)
	}
	if got := eventsDelta(t, absorbedBefore, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed); got != 0 {
		t.Errorf("events_total delta = %v, want 0 (no offset-commit decision in the wrapper on success)", got)
	}
}

// DLQ produce failure: the offset stays uncommitted (redelivery re-runs the
// path), so NOTHING commits on this pass — no park, no absorbed event.
func TestWithRetriesDLQProduceFailureCountsNothingCommitted(t *testing.T) {
	dlq := &fakeDLQ{produceErr: errors.New("broker down")}
	h := &flakyHandler{failFirst: 3, err: errors.New("poison")}
	wrapped := WithRetries(dlq, 0, consumerGroup, h.handle)

	retriesBefore := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup))
	dlqBefore := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events"))
	absorbedBefore := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues(
		metrics.ServiceName, consumerGroup, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed))

	if err := wrapped(context.Background(), poisonRecord()); err == nil {
		t.Fatal("DLQ produce failure must return the error (uncommitted)")
	}

	if got := testutil.ToFloat64(metrics.RetriesTotal.WithLabelValues(metrics.ServiceName, consumerGroup)) - retriesBefore; got != 2 {
		t.Errorf("retries_total delta = %v, want 2 (the retries really happened)", got)
	}
	if got := testutil.ToFloat64(metrics.DLQParkedTotal.WithLabelValues(metrics.ServiceName, consumerGroup, "order.events")) - dlqBefore; got != 0 {
		t.Errorf("dlq_parked_total delta = %v, want 0 (park failed)", got)
	}
	if got := eventsDelta(t, absorbedBefore, "order.events", "orderflow.order.OrderConfirmed", metrics.OutcomeAbsorbed); got != 0 {
		t.Errorf("events_total delta = %v, want 0 (offset NOT committed)", got)
	}
}

// ---- outbox relay metrics through the fake OutboxStore ----

type fakeOutbox struct {
	mu      sync.Mutex
	pending []notification.OutboxEvent
	fetches int
}

func (f *fakeOutbox) CountUnpublished(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending), nil
}

func (f *fakeOutbox) FetchUnpublished(_ context.Context, _ int) ([]notification.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	return append([]notification.OutboxEvent{}, f.pending...), nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, eventIDs []string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	remaining := f.pending[:0]
	for _, e := range f.pending {
		drop := false
		for _, id := range eventIDs {
			if e.EventID == id {
				drop = true
				break
			}
		}
		if !drop {
			remaining = append(remaining, e)
		}
	}
	f.pending = remaining
	return nil
}

// One tick: refreshes orderflow_outbox_pending from CountUnpublished, then
// publishes and counts each successfully relayed event on
// orderflow_outbox_published_total{service,topic}. The next tick observes
// the drained outbox (gauge back to 0).
func TestRelayTickRefreshesPendingAndCountsPublished(t *testing.T) {
	store := &fakeOutbox{pending: []notification.OutboxEvent{
		{EventID: "evt-ob-1", RecordType: events.NotificationSentType, AggregateID: "ord-1", Payload: []byte("a")},
		{EventID: "evt-ob-2", RecordType: events.NotificationSentType, AggregateID: "ord-1", Payload: []byte("b")},
	}}
	publishCalls := 0
	r := &Relay{
		store: store,
		topic: "notification.events",
		publish: func(context.Context, notification.OutboxEvent) error {
			publishCalls++
			return nil
		},
	}
	ctx := context.Background()

	pendingBefore := testutil.ToFloat64(metrics.OutboxPending)
	publishedBefore := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "notification.events"))

	r.tick(ctx)

	if got := testutil.ToFloat64(metrics.OutboxPending) - pendingBefore; got != 2 {
		t.Errorf("outbox_pending delta after tick = %v, want 2 (CountUnpublished refreshed the gauge)", got)
	}
	if publishCalls != 2 {
		t.Errorf("publish calls = %d, want 2", publishCalls)
	}
	if got := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "notification.events")) - publishedBefore; got != 2 {
		t.Errorf("outbox_published_total delta = %v, want 2", got)
	}

	// After MarkPublished the pending count drains; the NEXT tick refreshes
	// the gauge to the drained value (head-of-line alert parent).
	r.tick(ctx)
	if got := testutil.ToFloat64(metrics.OutboxPending); got != 0 {
		t.Errorf("outbox_pending after drained tick = %v, want 0", got)
	}
}

// A publish failure must not count anything as published: MarkPublished is
// the success decision point, and the counter moves only after it.
func TestRelayPublishFailureCountsNothing(t *testing.T) {
	store := &fakeOutbox{pending: []notification.OutboxEvent{
		{EventID: "evt-ob-1", RecordType: events.NotificationSentType, AggregateID: "ord-1", Payload: []byte("a")},
	}}
	r := &Relay{
		store: store,
		topic: "notification.events",
		publish: func(context.Context, notification.OutboxEvent) error {
			return errors.New("broker down")
		},
	}

	publishedBefore := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "notification.events"))
	pendingBefore := testutil.ToFloat64(metrics.OutboxPending)

	r.tick(context.Background())

	if got := testutil.ToFloat64(metrics.OutboxPublishedTotal.WithLabelValues(metrics.ServiceName, "notification.events")) - publishedBefore; got != 0 {
		t.Errorf("outbox_published_total delta = %v, want 0 (publish failed)", got)
	}
	store.mu.Lock()
	left := len(store.pending)
	store.mu.Unlock()
	if left != 1 {
		t.Errorf("pending rows = %d, want 1 (nothing marked published)", left)
	}
	// The pending gauge still refreshed (the tick always reports reality).
	if got := testutil.ToFloat64(metrics.OutboxPending) - pendingBefore; got != 1 {
		t.Errorf("outbox_pending delta = %v, want 1 (gauge refreshes even on flush failure)", got)
	}
}
