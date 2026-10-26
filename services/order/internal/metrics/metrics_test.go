package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Movement tests for the pinned M3 metric contract (S5): every assertion
// drives a helper and reads the real collector via prometheus/testutil.

func TestIncEventsRecordsPinnedLabels(t *testing.T) {
	m := New()
	m.IncEvents("order.inventory-events", "inventory.events", "orderflow.inventory.StockReserved", OutcomeProcessed)
	m.IncEvents("order.inventory-events", "inventory.events", "orderflow.inventory.StockReserved", OutcomeProcessed)
	m.IncEvents("order.inventory-events", "inventory.events", "orderflow.inventory.StockUnavailable", OutcomeAbsorbed)

	if got := testutil.ToFloat64(m.EventsTotal.WithLabelValues(ServiceName, "order.inventory-events", "inventory.events", "orderflow.inventory.StockReserved", OutcomeProcessed)); got != 2 {
		t.Errorf("orderflow_events_total{processed} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.EventsTotal.WithLabelValues(ServiceName, "order.inventory-events", "inventory.events", "orderflow.inventory.StockUnavailable", OutcomeAbsorbed)); got != 1 {
		t.Errorf("orderflow_events_total{absorbed} = %v, want 1", got)
	}
	// A distinct outcome label must not bleed into the counted series.
	if got := testutil.ToFloat64(m.EventsTotal.WithLabelValues(ServiceName, "order.inventory-events", "inventory.events", "orderflow.inventory.StockReserved", OutcomeAbsorbed)); got != 0 {
		t.Errorf("orderflow_events_total{absorbed,StockReserved} = %v, want 0", got)
	}
}

func TestIncRetriesAndDLQParkedPerGroupAndTopic(t *testing.T) {
	m := New()
	m.IncRetries("order.inventory-events")
	m.IncRetries("order.inventory-events")
	m.IncRetries("order.payment-events")
	m.IncDLQParked("order.inventory-events", "inventory.events")

	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(ServiceName, "order.inventory-events")); got != 2 {
		t.Errorf("orderflow_retries_total{order.inventory-events} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(ServiceName, "order.payment-events")); got != 1 {
		t.Errorf("orderflow_retries_total{order.payment-events} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.DLQParkedTotal.WithLabelValues(ServiceName, "order.inventory-events", "inventory.events")); got != 1 {
		t.Errorf("orderflow_dlq_parked_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.DLQParkedTotal.WithLabelValues(ServiceName, "order.payment-events", "payment.events")); got != 0 {
		t.Errorf("orderflow_dlq_parked_total{payment} = %v, want 0", got)
	}
}

func TestOutboxPendingAndPublished(t *testing.T) {
	m := New()
	m.SetOutboxPending(7)
	if got := testutil.ToFloat64(m.OutboxPending.WithLabelValues(ServiceName)); got != 7 {
		t.Errorf("orderflow_outbox_pending = %v, want 7", got)
	}
	m.SetOutboxPending(0) // gauge semantics: a later tick must overwrite, not accumulate
	if got := testutil.ToFloat64(m.OutboxPending.WithLabelValues(ServiceName)); got != 0 {
		t.Errorf("orderflow_outbox_pending after refresh = %v, want 0", got)
	}

	m.IncOutboxPublished("order.events")
	m.IncOutboxPublished("order.events")
	m.IncOutboxPublished("order.events")
	if got := testutil.ToFloat64(m.OutboxPublished.WithLabelValues(ServiceName, "order.events")); got != 3 {
		t.Errorf("orderflow_outbox_published_total = %v, want 3", got)
	}
}

func TestRefreshSagaOrdersZeroesThenAppliesCounts(t *testing.T) {
	m := New()
	statuses := []string{"AWAITING_STOCK", "AWAITING_PAYMENT", "CONFIRMED", "CANCELLED"}
	counts := map[string]int64{"AWAITING_STOCK": 2, "CONFIRMED": 5}
	if err := m.RefreshSagaOrders(context.Background(), statuses, func(context.Context) (map[string]int64, error) {
		return counts, nil
	}); err != nil {
		t.Fatalf("RefreshSagaOrders: %v", err)
	}
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("AWAITING_STOCK")); got != 2 {
		t.Errorf("orderflow_saga_orders{AWAITING_STOCK} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("CONFIRMED")); got != 5 {
		t.Errorf("orderflow_saga_orders{CONFIRMED} = %v, want 5", got)
	}
	// A status absent from the COUNT result must read 0, not a stale value.
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("CANCELLED")); got != 0 {
		t.Errorf("orderflow_saga_orders{CANCELLED} = %v, want 0 (zeroed)", got)
	}

	// Next poll: the order confirmed → the old count must not linger.
	counts = map[string]int64{"CONFIRMED": 6}
	if err := m.RefreshSagaOrders(context.Background(), statuses, func(context.Context) (map[string]int64, error) {
		return counts, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("AWAITING_STOCK")); got != 0 {
		t.Errorf("orderflow_saga_orders{AWAITING_STOCK} after refresh = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("CONFIRMED")); got != 6 {
		t.Errorf("orderflow_saga_orders{CONFIRMED} after refresh = %v, want 6", got)
	}
}

func TestRefreshSagaOrdersPropagatesFetchError(t *testing.T) {
	m := New()
	boom := errors.New("db down")
	err := m.RefreshSagaOrders(context.Background(), []string{"CONFIRMED"}, func(context.Context) (map[string]int64, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the fetch error propagated", err)
	}
}

func TestRunSagaOrdersPollerRefreshesUntilCancelled(t *testing.T) {
	m := New()
	statuses := []string{"CONFIRMED"}
	calls := make(chan struct{}, 16)
	fetch := func(context.Context) (map[string]int64, error) {
		calls <- struct{}{}
		return map[string]int64{"CONFIRMED": 1}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunSagaOrdersPoller(ctx, statuses, 5*time.Millisecond, fetch)
		close(done)
	}()

	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("poller never refreshed the saga gauge (immediate first refresh expected)")
	}
	// A second tick must arrive (the loop keeps polling, interval ≤ 15s bound).
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("poller stopped after the first refresh")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop on cancelled ctx")
	}
	if got := testutil.ToFloat64(m.SagaOrders.WithLabelValues("CONFIRMED")); got != 1 {
		t.Errorf("orderflow_saga_orders{CONFIRMED} = %v, want 1 after polled refresh", got)
	}
}

// The handler serves the pinned families on the existing mux (GET /metrics);
// no new port and only orderflow_* series on the private registry.
func TestHandlerServesPinnedFamilies(t *testing.T) {
	m := New()
	m.IncEvents("g", "topic", "event_type", OutcomeProcessed)
	m.SetOutboxPending(3)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, family := range []string{
		"orderflow_events_total",
		"orderflow_retries_total",
		"orderflow_dlq_parked_total",
		"orderflow_outbox_pending",
		"orderflow_outbox_published_total",
		"orderflow_saga_orders",
	} {
		if !strings.Contains(body, family) {
			t.Errorf("GET /metrics body missing family %q", family)
		}
	}
	if strings.Contains(body, "go_goroutines") {
		t.Error("private registry must not leak go-runtime collectors")
	}
}

func TestOutcomeCarrierNilSafe(t *testing.T) {
	// Layers run with or without the instrumented consumer; marking outside
	// the carrier (plain ctx) must be a no-op, never a panic.
	MarkAbsorbed(context.Background())
	MarkMatched(context.Background())

	o := NewEventOutcome()
	if o.Absorbed() || o.Matched() {
		t.Fatal("fresh carrier must be unmarked")
	}
	MarkAbsorbed(ContextWithOutcome(context.Background(), o))
	if !o.Absorbed() {
		t.Error("MarkAbsorbed did not mark the carrier")
	}
}
