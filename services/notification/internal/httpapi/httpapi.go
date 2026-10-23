// Package httpapi is the driving REST adapter for the notification service.
// Notifications are purely reactive in M2: no business endpoints exist — only
// liveness, which the compose healthcheck probes directly.
package httpapi

import "net/http"

// New builds the HTTP handler.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
