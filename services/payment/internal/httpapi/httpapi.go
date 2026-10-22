// Package httpapi is the driving REST adapter for the payment service,
// reached through the nginx gateway (/api/payments/...). Payments are purely
// reactive in M1: no business endpoints exist — only liveness. The compose
// healthcheck probes /healthz directly; the gateway serves
// /api/payments/healthz as /payments/healthz here.
package httpapi

import "net/http"

// New builds the HTTP handler.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /payments/healthz", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
