// Package httpapi is the driving REST adapter for the inventory service,
// reached through the nginx gateway (/api/inventory/... → /inventory/...).
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server routes requests to the application service.
type Server struct {
	svc *inventory.Service
}

// New builds the HTTP handler. /inventory/* are the gateway-mapped paths;
// /healthz is the compose container healthcheck.
func New(svc *inventory.Service) http.Handler {
	s := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /inventory/healthz", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.HandleFunc("GET /inventory/stock", s.handleListStock)
	// M3 metric contract: Prometheus exposition on the EXISTING mux (no new
	// port, no new env) — the default registry carries the pinned orderflow_*
	// collectors from internal/metrics.
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

type stockView struct {
	SKU       string `json:"sku"`
	Available int    `json:"available"`
}

func (s *Server) handleListStock(w http.ResponseWriter, r *http.Request) {
	stocks, err := s.svc.ListStock(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	views := make([]stockView, len(stocks))
	for i, st := range stocks {
		views[i] = stockView{SKU: st.SKU, Available: st.Available}
	}
	writeJSON(w, http.StatusOK, views)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
