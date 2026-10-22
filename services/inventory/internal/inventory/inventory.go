// Package inventory holds the stock domain: pure, no infrastructure.
// The reservation aggregate is inventory-internal (topology §6); M1 has no
// release path yet — StockReleased lands with M2 compensations.
package inventory

import (
	"errors"
	"fmt"
)

// Stock is one SKU's on-hand quantity.
type Stock struct {
	SKU       string
	Available int
}

// ErrInsufficientStock guards against overselling.
var ErrInsufficientStock = errors.New("insufficient stock")

// Reserve decrements the on-hand quantity, refusing to oversell.
func (s *Stock) Reserve(qty int) error {
	if qty <= 0 {
		return fmt.Errorf("stock %q: reserve qty must be > 0", s.SKU)
	}
	if s.Available < qty {
		return fmt.Errorf("%w: %q has %d available, requested %d", ErrInsufficientStock, s.SKU, s.Available, qty)
	}
	s.Available -= qty
	return nil
}

// Item is one order line to reserve.
type Item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// Shortage reports one unfulfillable line (StockUnavailable.shortages).
type Shortage struct {
	SKU       string
	Requested int
	Available int
}

// Reservation is the inventory-internal fact of stock held for an order.
// TotalCents echoes OrderPlaced.total_cents (schema v2) so payment can charge
// from StockReserved without another subscription.
type Reservation struct {
	OrderID    string
	Items      []Item
	TotalCents int64
}

// NewReservation validates identity and lines (order.NewOrder enforces the
// same invariants upstream; revalidated at this service boundary).
func NewReservation(orderID string, totalCents int64, items []Item) (Reservation, error) {
	if orderID == "" {
		return Reservation{}, errors.New("order id is required")
	}
	if totalCents < 0 {
		return Reservation{}, errors.New("total must be >= 0")
	}
	if len(items) == 0 {
		return Reservation{}, errors.New("at least one item is required")
	}
	seen := make(map[string]bool, len(items))
	for _, it := range items {
		if it.SKU == "" {
			return Reservation{}, errors.New("item sku is required")
		}
		if seen[it.SKU] {
			return Reservation{}, fmt.Errorf("duplicate sku %q", it.SKU)
		}
		seen[it.SKU] = true
		if it.Qty <= 0 {
			return Reservation{}, fmt.Errorf("item %q: qty must be > 0", it.SKU)
		}
	}
	return Reservation{
		OrderID:    orderID,
		Items:      append([]Item(nil), items...),
		TotalCents: totalCents,
	}, nil
}
