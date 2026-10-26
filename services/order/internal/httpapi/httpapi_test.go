package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// Minimal order ports so the mux test drives a REAL service (the /metrics
// route must not disturb the existing API surface).

type apiStore struct {
	orders map[string]*order.Order
}

func newAPIStore() *apiStore { return &apiStore{orders: map[string]*order.Order{}} }

func (f *apiStore) SaveOrder(_ context.Context, o *order.Order) error {
	f.orders[o.ID] = o
	return nil
}

func (f *apiStore) GetOrder(_ context.Context, id string) (*order.Order, error) {
	if o, ok := f.orders[id]; ok {
		return o, nil
	}
	return nil, order.ErrOrderNotFound
}

func (f *apiStore) UpdateOrder(context.Context, *order.Order) error { return nil }

func (f *apiStore) AppendOutbox(context.Context, order.OutboxEvent) error { return nil }

func (f *apiStore) InsertProcessedEvent(context.Context, string) (bool, error) { return true, nil }

type apiTx struct{ store *apiStore }

func (f *apiTx) WithinTx(ctx context.Context, fn func(context.Context, order.Store) error) error {
	return fn(ctx, f.store)
}

type apiEncoder struct{}

func (apiEncoder) OrderPlaced(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-" + o.ID, RecordType: "orderflow.order.OrderPlaced", AggregateID: o.ID, Payload: []byte("{}")}, nil
}

func (apiEncoder) OrderConfirmed(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-c-" + o.ID, RecordType: "orderflow.order.OrderConfirmed", AggregateID: o.ID, Payload: []byte("{}")}, nil
}

func (apiEncoder) OrderCancelled(o order.Order) (order.OutboxEvent, error) {
	return order.OutboxEvent{EventID: "evt-x-" + o.ID, RecordType: "orderflow.order.OrderCancelled", AggregateID: o.ID, Payload: []byte("{}")}, nil
}

// GET /metrics serves the pinned orderflow families on the EXISTING mux:
// no new port, and the previous routes keep working.
func TestMetricsRouteOnExistingMux(t *testing.T) {
	store := newAPIStore()
	svc := order.NewService(&apiTx{store}, store, apiEncoder{})
	m := metrics.New()
	handler := New(svc, m.Handler())

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want Prometheus text/plain exposition", ct)
	}
	for _, family := range []string{"orderflow_events_total", "orderflow_saga_orders", "orderflow_outbox_pending"} {
		if !strings.Contains(rec.Body.String(), family) {
			t.Errorf("GET /metrics body missing family %q", family)
		}
	}

	// Existing surface intact: health + a real POST /orders through the mux.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("GET /healthz = %d, want 200", rec.Code)
	}

	body := strings.NewReader(`{"customer_id":"cus-1","items":[{"sku":"SKU-A","qty":1,"unit_price_cents":100}]}`)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/orders", body))
	if rec.Code != 201 {
		t.Fatalf("POST /orders = %d, want 201", rec.Code)
	}
	var created orderView
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created order: %v", err)
	}
	if created.Status != "AWAITING_STOCK" {
		t.Errorf("created status = %q, want AWAITING_STOCK", created.Status)
	}

	// A nil handler must skip the route (metrics are optional to the API).
	rec = httptest.NewRecorder()
	New(svc, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 404 {
		t.Errorf("GET /metrics with nil handler = %d, want 404 (route absent)", rec.Code)
	}
}
