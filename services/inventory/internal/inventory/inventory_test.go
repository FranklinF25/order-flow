package inventory

import (
	"errors"
	"testing"
)

func TestNewReservationValid(t *testing.T) {
	items := []Item{{SKU: "SKU-A", Qty: 2}, {SKU: "SKU-B", Qty: 1}}
	r, err := NewReservation("ord-1", 3000, items)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.OrderID != "ord-1" || r.TotalCents != 3000 {
		t.Errorf("reservation = %q/%d, want ord-1/3000", r.OrderID, r.TotalCents)
	}
	if len(r.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(r.Items))
	}
	// The reservation must own a copy: later caller mutations must not leak in.
	items[0].Qty = 99
	if r.Items[0].Qty != 2 {
		t.Errorf("reservation items mutated by caller: %+v", r.Items)
	}
}

func TestNewReservationValidation(t *testing.T) {
	cases := []struct {
		name    string
		orderID string
		total   int64
		items   []Item
	}{
		{"empty order id", "", 100, []Item{{SKU: "SKU-A", Qty: 1}}},
		{"negative total", "ord-1", -1, []Item{{SKU: "SKU-A", Qty: 1}}},
		{"no items", "ord-1", 100, nil},
		{"empty sku", "ord-1", 100, []Item{{SKU: "", Qty: 1}}},
		{"zero qty", "ord-1", 100, []Item{{SKU: "SKU-A", Qty: 0}}},
		{"negative qty", "ord-1", 100, []Item{{SKU: "SKU-A", Qty: -1}}},
		{"duplicate sku", "ord-1", 100, []Item{
			{SKU: "SKU-A", Qty: 1},
			{SKU: "SKU-A", Qty: 2},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewReservation(tc.orderID, tc.total, tc.items); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestStockReserve(t *testing.T) {
	t.Run("success decrements", func(t *testing.T) {
		s := Stock{SKU: "SKU-A", Available: 10}
		if err := s.Reserve(4); err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		if s.Available != 6 {
			t.Errorf("available = %d, want 6", s.Available)
		}
	})
	t.Run("insufficient is rejected unchanged", func(t *testing.T) {
		s := Stock{SKU: "SKU-A", Available: 5}
		err := s.Reserve(6)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, ErrInsufficientStock) {
			t.Fatalf("error %v is not ErrInsufficientStock", err)
		}
		if s.Available != 5 {
			t.Errorf("available = %d, want unchanged 5", s.Available)
		}
	})
	t.Run("non positive qty rejected", func(t *testing.T) {
		s := Stock{SKU: "SKU-A", Available: 5}
		if err := s.Reserve(0); err == nil {
			t.Fatal("expected error for zero qty")
		}
		if err := s.Reserve(-2); err == nil {
			t.Fatal("expected error for negative qty")
		}
		if s.Available != 5 {
			t.Errorf("available = %d, want unchanged 5", s.Available)
		}
	})
}
