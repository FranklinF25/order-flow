// Package order holds the Order aggregate: pure domain, no infrastructure.
// Status machine per docs/event-topology.md §6:
//
//	AWAITING_STOCK → AWAITING_PAYMENT → CONFIRMED
//	AWAITING_STOCK → CANCELLED(STOCK_UNAVAILABLE)
//	AWAITING_PAYMENT → CANCELLED(PAYMENT_FAILED)
package order

import (
	"errors"
	"fmt"
)

// Status is the Order aggregate lifecycle state.
type Status string

// Order statuses (topology v1).
const (
	StatusAwaitingStock   Status = "AWAITING_STOCK"
	StatusAwaitingPayment Status = "AWAITING_PAYMENT"
	StatusConfirmed       Status = "CONFIRMED"
	StatusCancelled       Status = "CANCELLED"
)

// CancelReason explains a terminal CANCELLED state.
type CancelReason string

// Cancel reasons (topology v1).
const (
	CancelStockUnavailable CancelReason = "STOCK_UNAVAILABLE"
	CancelPaymentFailed    CancelReason = "PAYMENT_FAILED"
)

// Item is one order line.
type Item struct {
	SKU            string `json:"sku"`
	Qty            int    `json:"qty"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Order is the aggregate root.
type Order struct {
	ID           string
	CustomerID   string
	Items        []Item
	TotalCents   int64
	Status       Status
	CancelReason CancelReason
}

// InvalidTransitionError reports a saga event applied to the wrong state.
type InvalidTransitionError struct {
	From Status
	On   string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid transition: %s on status %q", e.On, e.From)
}

// NewOrder creates an order in AWAITING_STOCK, validating identity, items and
// computing the total. Duplicate SKUs are rejected: reservations are per SKU.
func NewOrder(id, customerID string, items []Item) (Order, error) {
	if id == "" {
		return Order{}, errors.New("order id is required")
	}
	if customerID == "" {
		return Order{}, errors.New("customer id is required")
	}
	if len(items) == 0 {
		return Order{}, errors.New("at least one item is required")
	}
	seen := make(map[string]bool, len(items))
	var total int64
	for _, it := range items {
		if it.SKU == "" {
			return Order{}, errors.New("item sku is required")
		}
		if seen[it.SKU] {
			return Order{}, fmt.Errorf("duplicate sku %q", it.SKU)
		}
		seen[it.SKU] = true
		if it.Qty <= 0 {
			return Order{}, fmt.Errorf("item %q: qty must be > 0", it.SKU)
		}
		if it.UnitPriceCents < 0 {
			return Order{}, fmt.Errorf("item %q: unit price must be >= 0", it.SKU)
		}
		total += int64(it.Qty) * it.UnitPriceCents
	}
	return Order{
		ID:         id,
		CustomerID: customerID,
		Items:      append([]Item(nil), items...),
		TotalCents: total,
		Status:     StatusAwaitingStock,
	}, nil
}

func (o *Order) require(status Status, on string) error {
	if o.Status != status {
		return &InvalidTransitionError{From: o.Status, On: on}
	}
	return nil
}

// OnStockReserved: AWAITING_STOCK → AWAITING_PAYMENT.
func (o *Order) OnStockReserved() error {
	if err := o.require(StatusAwaitingStock, "StockReserved"); err != nil {
		return err
	}
	o.Status = StatusAwaitingPayment
	return nil
}

// OnStockUnavailable: AWAITING_STOCK → CANCELLED(STOCK_UNAVAILABLE).
func (o *Order) OnStockUnavailable() error {
	if err := o.require(StatusAwaitingStock, "StockUnavailable"); err != nil {
		return err
	}
	o.Status = StatusCancelled
	o.CancelReason = CancelStockUnavailable
	return nil
}

// OnPaymentCaptured: AWAITING_PAYMENT → CONFIRMED.
func (o *Order) OnPaymentCaptured() error {
	if err := o.require(StatusAwaitingPayment, "PaymentCaptured"); err != nil {
		return err
	}
	o.Status = StatusConfirmed
	return nil
}

// OnPaymentFailed: AWAITING_PAYMENT → CANCELLED(PAYMENT_FAILED).
func (o *Order) OnPaymentFailed() error {
	if err := o.require(StatusAwaitingPayment, "PaymentFailed"); err != nil {
		return err
	}
	o.Status = StatusCancelled
	o.CancelReason = CancelPaymentFailed
	return nil
}

// Terminal reports whether the aggregate is in a terminal state
// (CONFIRMED or CANCELLED reject every further transition).
func (o *Order) Terminal() bool {
	return o.Status == StatusConfirmed || o.Status == StatusCancelled
}
