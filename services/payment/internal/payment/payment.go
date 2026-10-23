// Package payment holds the Payment aggregate: pure domain, no infrastructure.
// Topology §6: payments are terminal on creation — CAPTURED by the simulated
// gateway, or FAILED when the M2 failure simulation switch rejects the charge.
package payment

import "errors"

// Status is the Payment lifecycle state.
type Status string

// Payment statuses (topology v1).
const (
	StatusCaptured Status = "CAPTURED"
	StatusFailed   Status = "FAILED"
)

// Payment is the aggregate root.
type Payment struct {
	ID          string
	OrderID     string
	AmountCents int64
	Status      Status
}

// NewPayment creates a CAPTURED payment for orderID. Terminal on creation:
// every further lifecycle step (e.g. refund) is deferred per topology §9.
func NewPayment(id, orderID string, amountCents int64) (Payment, error) {
	if err := validate(id, orderID, amountCents); err != nil {
		return Payment{}, err
	}
	return Payment{
		ID:          id,
		OrderID:     orderID,
		AmountCents: amountCents,
		Status:      StatusCaptured,
	}, nil
}

// NewFailedPayment creates a FAILED payment for a charge the simulated gateway
// rejected (compensation B entry point; topology §6). Like captures, failures
// are idempotent per order_id via the payments UNIQUE constraint.
func NewFailedPayment(id, orderID string, amountCents int64) (Payment, error) {
	if err := validate(id, orderID, amountCents); err != nil {
		return Payment{}, err
	}
	return Payment{
		ID:          id,
		OrderID:     orderID,
		AmountCents: amountCents,
		Status:      StatusFailed,
	}, nil
}

func validate(id, orderID string, amountCents int64) error {
	if id == "" {
		return errors.New("payment id is required")
	}
	if orderID == "" {
		return errors.New("order id is required")
	}
	if amountCents < 0 {
		return errors.New("amount must be >= 0")
	}
	return nil
}
