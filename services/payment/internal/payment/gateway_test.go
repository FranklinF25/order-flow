package payment

import (
	"context"
	"errors"
	"testing"
)

// TestSimulatedGatewayMarkerMatrix pins the M2 failure simulation switch (S3):
// the lowercased customer_id is prefix-matched — decline* → CARD_DECLINED,
// insufficient* → INSUFFICIENT_FUNDS, error* → GATEWAY_ERROR — and any other
// value captures normally. The prefix must be exact: "errol@" does not start
// with "error".
func TestSimulatedGatewayMarkerMatrix(t *testing.T) {
	cases := []struct {
		name       string
		customerID string
		wantReason FailureReason // "" means the charge must capture
	}{
		{"decline prefix declines", "decline@x.com", ReasonCardDeclined},
		{"insufficient prefix is case-insensitive", "INSUFFICIENT@x.com", ReasonInsufficientFunds},
		{"error prefix maps to gateway error", "error-x", ReasonGatewayError},
		{"error prefix is exact so errol captures", "errol@x.com", ""},
		{"any other value captures", "ok@x.com", ""},
		{"empty customer id captures", "", ""},
	}
	gw := NewSimulatedGateway()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := gw.Capture(context.Background(), "ord-1", 4998, tc.customerID)
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("Capture(%q): %v, want a normal capture", tc.customerID, err)
				}
				if id == "" {
					t.Error("capture must return a payment id")
				}
				return
			}
			var decl *DeclineError
			if !errors.As(err, &decl) {
				t.Fatalf("Capture(%q): %v, want a DeclineError", tc.customerID, err)
			}
			if decl.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", decl.Reason, tc.wantReason)
			}
		})
	}
}
