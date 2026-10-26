package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
	"github.com/FranklinF25/order-flow/services/inventory/internal/metrics"
)

// Minimal service stubs: the /metrics route never reaches the domain, but
// New requires a *inventory.Service.
type stubStore struct{}

func (stubStore) MarkProcessed(context.Context, string) error                       { return nil }
func (stubStore) GetStock(context.Context, string) (*inventory.Stock, error)        { return nil, nil }
func (stubStore) ListStock(context.Context) ([]inventory.Stock, error)              { return nil, nil }
func (stubStore) DecrementStock(context.Context, string, int) error                 { return nil }
func (stubStore) SaveReservation(context.Context, inventory.Reservation) error      { return nil }
func (stubStore) AppendOutbox(context.Context, inventory.OutboxEvent) error         { return nil }
func (stubStore) GetReservation(context.Context, string) (*inventory.Reservation, error) { return nil, nil }
func (stubStore) ReleaseReservation(context.Context, string) (bool, error)          { return false, nil }
func (stubStore) IncrementStock(context.Context, string, int) error                 { return nil }

type stubTx struct{}

func (stubTx) WithinTx(ctx context.Context, fn func(context.Context, inventory.Store) error) error {
	return fn(ctx, stubStore{})
}

type stubEncoder struct{}

func (stubEncoder) StockReserved(inventory.Reservation) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

func (stubEncoder) StockUnavailable(string, []inventory.Shortage) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

func (stubEncoder) StockReleased(string, []inventory.Item) (inventory.OutboxEvent, error) {
	return inventory.OutboxEvent{}, nil
}

// GET /metrics serves the pinned orderflow contract on the EXISTING mux (M3:
// no new port, no new env): after one handled event the exact exposition
// series — all five labels, constant service=inventory — must appear.
func TestMetricsEndpointServesPinnedSeries(t *testing.T) {
	metrics.IncEvent("inventory.order-events", "order.events", "orderflow.order.OrderPlaced", metrics.OutcomeProcessed)

	svc := inventory.NewService(stubTx{}, stubStore{}, stubEncoder{})
	h := New(svc)

	rec := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, rec)

	if rw.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rw.Code)
	}
	if ct := rw.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text exposition (text/plain...)", ct)
	}
	want := `orderflow_events_total{event_type="orderflow.order.OrderPlaced",group="inventory.order-events",outcome="processed",service="inventory",topic="order.events"}`
	if !strings.Contains(rw.Body.String(), want) {
		t.Errorf("GET /metrics body does not contain the pinned series %q\nbody:\n%s", want, rw.Body.String())
	}
}
