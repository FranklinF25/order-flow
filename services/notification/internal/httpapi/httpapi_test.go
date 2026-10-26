package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /metrics is served by the EXISTING mux (no new port, no new env) and
// exposes the pinned M3 orderflow metric contract.
func TestMetricsEndpointServesPinnedContract(t *testing.T) {
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, name := range []string{
		"orderflow_events_total",
		"orderflow_retries_total",
		"orderflow_dlq_parked_total",
		"orderflow_outbox_pending",
		"orderflow_outbox_published_total",
		"orderflow_notifications_published_total",
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("/metrics body missing pinned metric %s", name)
		}
	}
}

// /healthz keeps its healthcheck contract for the compose probe.
func TestHealthzStillServed(t *testing.T) {
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
