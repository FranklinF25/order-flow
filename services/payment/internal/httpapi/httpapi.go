// Package httpapi is the driving REST adapter for the payment service,
// reached through the nginx gateway (/api/payments/...). Payments are purely
// reactive in M1: no business endpoints exist — only liveness and the M3
// metrics surface. The compose healthcheck probes /healthz directly; the
// gateway serves /api/payments/healthz as /payments/healthz here.
package httpapi

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New builds the HTTP handler. GET /metrics serves the default Prometheus
// registry — the metrics package registers the pinned orderflow_* contract
// there (no new port, no new env; the M3 scrape config reads it here).
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /payments/healthz", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
