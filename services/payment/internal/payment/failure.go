package payment

import "fmt"

// FailureReason is the machine-readable rejection cause carried by
// PaymentFailed.failure_reason (topology §5 enum).
type FailureReason string

// Failure reasons (schema: schemas/payment/payment-failed.avsc).
const (
	ReasonCardDeclined      FailureReason = "CARD_DECLINED"
	ReasonInsufficientFunds FailureReason = "INSUFFICIENT_FUNDS"
	ReasonGatewayError      FailureReason = "GATEWAY_ERROR"
)

// DeclineError is the simulated gateway's structured rejection: the charge was
// deterministically declined (customer_id marker, M2 session decision S3) and
// Reason feeds PaymentFailed.failure_reason. Distinct from an unplanned gateway
// outage, which surfaces as an ordinary error and persists nothing.
type DeclineError struct {
	Reason FailureReason
}

func (e *DeclineError) Error() string {
	return fmt.Sprintf("gateway declined: %s", e.Reason)
}
