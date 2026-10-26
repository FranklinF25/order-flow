package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FranklinF25/order-flow/services/payment/internal/metrics"
)

// GET /metrics serves the default registry on the EXISTING mux (no new
// port, no new env): after one movement of every pinned collector, the
// exposition carries all five orderflow_* families.
func TestMetricsEndpointServesPinnedContract(t *testing.T) {
	// Single metrics.New() in this package's test binary: it registers on
	// the default registry, which is exactly what promhttp serves.
	m := metrics.New()
	m.IncEvents("payment.inventory-events", "inventory.events", "orderflow.inventory.StockReserved", metrics.OutcomeProcessed)
	m.IncRetries("payment.inventory-events")
	m.IncDLQParked("payment.inventory-events", "inventory.events")
	m.SetOutboxPending(3)
	m.IncOutboxPublished("payment.events")

	srv := httptest.NewServer(New())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	buf := make([]byte, 0)
	tmp := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	body := string(buf)
	for _, want := range []string{
		"orderflow_events_total",
		"orderflow_retries_total",
		"orderflow_dlq_parked_total",
		"orderflow_outbox_pending",
		"orderflow_outbox_published_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("GET /metrics body missing %q", want)
		}
	}
}

// Liveness is untouched by the metrics surface.
func TestHealthzStillServes(t *testing.T) {
	srv := httptest.NewServer(New())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
