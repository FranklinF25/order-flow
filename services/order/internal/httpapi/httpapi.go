// Package httpapi is the driving REST adapter for the order service,
// reached through the nginx gateway (/api/orders/...).
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// Server routes requests to the application service.
type Server struct {
	svc *order.Service
}

// New builds the HTTP handler.
func New(svc *order.Service) http.Handler {
	s := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.handlePlaceOrder)
	mux.HandleFunc("GET /orders/{id}", s.handleGetOrder)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

type placeOrderRequest struct {
	CustomerID string    `json:"customer_id"`
	Items      []reqItem `json:"items"`
}

type reqItem struct {
	SKU            string `json:"sku"`
	Qty            int    `json:"qty"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type orderView struct {
	ID           string       `json:"id"`
	CustomerID   string       `json:"customer_id"`
	Items        []order.Item `json:"items"`
	TotalCents   int64        `json:"total_cents"`
	Status       string       `json:"status"`
	CancelReason string       `json:"cancel_reason,omitempty"`
}

func view(o *order.Order) orderView {
	return orderView{
		ID:         o.ID,
		CustomerID: o.CustomerID,
		Items:      o.Items,
		TotalCents: o.TotalCents,
		Status:     string(o.Status),
		CancelReason: string(o.CancelReason),
	}
}

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	in := order.PlaceOrderInput{CustomerID: req.CustomerID}
	for _, it := range req.Items {
		in.Items = append(in.Items, order.Item{SKU: it.SKU, Qty: it.Qty, UnitPriceCents: it.UnitPriceCents})
	}
	o, err := s.svc.PlaceOrder(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, view(o))
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	o, err := s.svc.GetOrder(r.Context(), r.PathValue("id"))
	if errors.Is(err, order.ErrOrderNotFound) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, view(o))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
