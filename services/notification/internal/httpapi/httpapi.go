// Package httpapi is the driving REST adapter for the notification service.
// Notifications are purely reactive in M2: no business endpoints exist — only
// liveness (probed by the compose healthcheck) and the pinned M3 metric
// contract on GET /metrics (spec odd/tasks/order-flow-m3.md; no new port).
package httpapi

import (
	"net/http"

	"github.com/FranklinF25/order-flow/services/notification/internal/metrics"
)

// New builds the HTTP handler.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", metrics.Handler())
	return mux
}
