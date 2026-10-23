package payment

import (
	"context"
	"strings"
)

// SimulatedGateway is the payment gateway adapter. Charges are a deterministic
// function of the customer_id marker (M2 session decision S3): the lowercased
// customer_id is prefix-matched — decline* → CARD_DECLINED, insufficient* →
// INSUFFICIENT_FUNDS, error* → GATEWAY_ERROR — and any other value captures
// with a minted UUID payment id. The Capture port itself stays injectable;
// only this adapter owns the marker rule.
type SimulatedGateway struct{}

// NewSimulatedGateway builds the marker-driven simulated gateway.
func NewSimulatedGateway() *SimulatedGateway { return &SimulatedGateway{} }

// Capture evaluates the failure marker for customerID and either rejects the
// charge with a DeclineError carrying the topology failure reason or succeeds.
func (SimulatedGateway) Capture(_ context.Context, _ string, _ int64, customerID string) (string, error) {
	switch marker := strings.ToLower(customerID); {
	case strings.HasPrefix(marker, "decline"):
		return "", &DeclineError{Reason: ReasonCardDeclined}
	case strings.HasPrefix(marker, "insufficient"):
		return "", &DeclineError{Reason: ReasonInsufficientFunds}
	case strings.HasPrefix(marker, "error"):
		return "", &DeclineError{Reason: ReasonGatewayError}
	}
	return NewUUID()
}
