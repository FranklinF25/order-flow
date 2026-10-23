package payment

import "testing"

func TestNewPaymentCapturesTerminalStatus(t *testing.T) {
	p, err := NewPayment("pay-1", "ord-1", 4998)
	if err != nil {
		t.Fatalf("NewPayment: %v", err)
	}
	if p.Status != StatusCaptured {
		t.Errorf("status = %q, want CAPTURED (terminal on creation)", p.Status)
	}
	if p.ID != "pay-1" || p.OrderID != "ord-1" || p.AmountCents != 4998 {
		t.Errorf("payment = %+v, want id/order/amount copied", p)
	}
}

func TestNewFailedPaymentMarksTerminalFailure(t *testing.T) {
	p, err := NewFailedPayment("pay-f-1", "ord-1", 4998)
	if err != nil {
		t.Fatalf("NewFailedPayment: %v", err)
	}
	if p.Status != StatusFailed {
		t.Errorf("status = %q, want FAILED", p.Status)
	}
	if p.ID != "pay-f-1" || p.OrderID != "ord-1" || p.AmountCents != 4998 {
		t.Errorf("payment = %+v, want id/order/amount copied", p)
	}
}

func TestNewPaymentValidation(t *testing.T) {
	cases := []struct {
		name        string
		id, orderID string
		amountCents int64
	}{
		{"empty payment id", "", "ord-1", 100},
		{"empty order id", "pay-1", "", 100},
		{"negative amount", "pay-1", "ord-1", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPayment(tc.id, tc.orderID, tc.amountCents); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}
