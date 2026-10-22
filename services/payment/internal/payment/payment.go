// Package payment holds the Payment aggregate: pure domain, no infrastructure.
// Topology §6: payments are terminal on creation (CAPTURED) and idempotent per
// order_id — M1 captures only; the failure switch arrives with M2.
package payment

import "errors"

// Status is the Payment lifecycle state.
type Status string

// Payment statuses (topology v1).
const (
	StatusCaptured Status = "CAPTURED"
)

// Payment is the aggregate root.
type Payment struct {
	ID          string
	OrderID     string
	AmountCents int64
	Status      Status
}

// NewPayment creates a CAPTURED payment for orderID. Terminal on creation:
// every further lifecycle step (refund, failure) is deferred per topology §9.
func NewPayment(id, orderID string, amountCents int64) (Payment, error) {
	if id == "" {
		return Payment{}, errors.New("payment id is required")
	}
	if orderID == "" {
		return Payment{}, errors.New("order id is required")
	}
	if amountCents < 0 {
		return Payment{}, errors.New("amount must be >= 0")
	}
	return Payment{
		ID:          id,
		OrderID:     orderID,
		AmountCents: amountCents,
		Status:      StatusCaptured,
	}, nil
}
