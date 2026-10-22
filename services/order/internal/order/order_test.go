package order

import (
	"errors"
	"testing"
)

func validItems() []Item {
	return []Item{
		{SKU: "SKU-A", Qty: 2, UnitPriceCents: 1500},
		{SKU: "SKU-B", Qty: 1, UnitPriceCents: 9900},
	}
}

func TestNewOrderValid(t *testing.T) {
	o, err := NewOrder("ord-1", "cus-1", validItems())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.Status != StatusAwaitingStock {
		t.Errorf("status = %q, want AWAITING_STOCK", o.Status)
	}
	if want := int64(2*1500 + 9900); o.TotalCents != want {
		t.Errorf("total = %d, want %d", o.TotalCents, want)
	}
	if o.CancelReason != "" {
		t.Errorf("cancel reason = %q, want empty", o.CancelReason)
	}
}

func TestNewOrderValidation(t *testing.T) {
	cases := []struct {
		name string
		id   string
		cus  string
		items []Item
	}{
		{"empty id", "", "cus-1", validItems()},
		{"empty customer", "ord-1", "", validItems()},
		{"no items", "ord-1", "cus-1", nil},
		{"empty sku", "ord-1", "cus-1", []Item{{SKU: "", Qty: 1, UnitPriceCents: 100}}},
		{"zero qty", "ord-1", "cus-1", []Item{{SKU: "SKU-A", Qty: 0, UnitPriceCents: 100}}},
		{"negative qty", "ord-1", "cus-1", []Item{{SKU: "SKU-A", Qty: -1, UnitPriceCents: 100}}},
		{"negative price", "ord-1", "cus-1", []Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: -1}}},
		{"duplicate sku", "ord-1", "cus-1", []Item{
			{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100},
			{SKU: "SKU-A", Qty: 2, UnitPriceCents: 200},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewOrder(tc.id, tc.cus, tc.items); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestHappyPathTransitions(t *testing.T) {
	o, err := NewOrder("ord-1", "cus-1", validItems())
	if err != nil {
		t.Fatal(err)
	}
	if err := o.OnStockReserved(); err != nil {
		t.Fatalf("OnStockReserved: %v", err)
	}
	if o.Status != StatusAwaitingPayment {
		t.Errorf("after reserve status = %q, want AWAITING_PAYMENT", o.Status)
	}
	if err := o.OnPaymentCaptured(); err != nil {
		t.Fatalf("OnPaymentCaptured: %v", err)
	}
	if o.Status != StatusConfirmed {
		t.Errorf("after capture status = %q, want CONFIRMED", o.Status)
	}
	if !o.Terminal() {
		t.Error("CONFIRMED must be terminal")
	}
}

func TestCancellationTransitions(t *testing.T) {
	t.Run("stock unavailable from awaiting stock", func(t *testing.T) {
		o, _ := NewOrder("ord-1", "cus-1", validItems())
		if err := o.OnStockUnavailable(); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if o.Status != StatusCancelled || o.CancelReason != CancelStockUnavailable {
			t.Errorf("got %q/%q, want CANCELLED/STOCK_UNAVAILABLE", o.Status, o.CancelReason)
		}
		if !o.Terminal() {
			t.Error("CANCELLED must be terminal")
		}
	})
	t.Run("payment failed from awaiting payment", func(t *testing.T) {
		o, _ := NewOrder("ord-1", "cus-1", validItems())
		_ = o.OnStockReserved()
		if err := o.OnPaymentFailed(); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if o.Status != StatusCancelled || o.CancelReason != CancelPaymentFailed {
			t.Errorf("got %q/%q, want CANCELLED/PAYMENT_FAILED", o.Status, o.CancelReason)
		}
	})
}

func TestInvalidTransitions(t *testing.T) {
	type step struct {
		name string
		apply func(*Order) error
	}
	// A confirmed order rejects everything (terminal).
	confirmed := func(t *testing.T) *Order {
		t.Helper()
		o, _ := NewOrder("ord-1", "cus-1", validItems())
		_ = o.OnStockReserved()
		_ = o.OnPaymentCaptured()
		return &o
	}
	cancelled := func(t *testing.T) *Order {
		t.Helper()
		o, _ := NewOrder("ord-1", "cus-1", validItems())
		_ = o.OnStockUnavailable()
		return &o
	}
	for _, tc := range []struct {
		name string
		o    *Order
		step step
	}{
		{"capture before reserve", &Order{Status: StatusAwaitingStock}, step{"OnPaymentCaptured", (*Order).OnPaymentCaptured}},
		{"confirmed then reserve", nil, step{"OnStockReserved", (*Order).OnStockReserved}},
		{"confirmed then unavailable", nil, step{"OnStockUnavailable", (*Order).OnStockUnavailable}},
		{"confirmed then failed", nil, step{"OnPaymentFailed", (*Order).OnPaymentFailed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.o
			if o == nil {
				o = confirmed(t)
			}
			var target Order = *o
			if err := tc.step.apply(&target); err == nil {
				t.Fatal("expected InvalidTransitionError, got nil")
			}
			var ite *InvalidTransitionError
			if !errors.As(errOf(tc.step.apply(&target)), &ite) {
				t.Fatalf("error %v is not *InvalidTransitionError", errOf(tc.step.apply(&target)))
			}
		})
	}
	t.Run("unavailable from awaiting payment", func(t *testing.T) {
		o, _ := NewOrder("ord-1", "cus-1", validItems())
		_ = o.OnStockReserved()
		if err := o.OnStockUnavailable(); err == nil {
			t.Fatal("expected error: stock unavailable only valid from AWAITING_STOCK")
		}
	})
	t.Run("cancelled rejects reserve", func(t *testing.T) {
		o := cancelled(t)
		var target Order = *o
		if err := target.OnStockReserved(); err == nil {
			t.Fatal("cancelled order must reject OnStockReserved")
		}
	})
}

func errOf(err error) error { return err }
