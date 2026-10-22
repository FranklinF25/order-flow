package payment

import "context"

// SimulatedGateway is the M1 payment gateway: it always captures and mints a
// UUID payment id. Failure simulation (declines, gateway errors) is M2 scope
// per the M1 slice decisions.
type SimulatedGateway struct{}

// NewSimulatedGateway builds the always-capture gateway.
func NewSimulatedGateway() *SimulatedGateway { return &SimulatedGateway{} }

// Capture succeeds unconditionally (M1 contract).
func (SimulatedGateway) Capture(_ context.Context, _ string, _ int64) (string, error) {
	return NewUUID()
}
