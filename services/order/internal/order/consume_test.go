package order

import (
	"context"
	"errors"
	"testing"
)

func awaitingPaymentOrder(t *testing.T, store *fakeStore) *Order {
	t.Helper()
	o, err := NewOrder("ord-live", "cus-1", []Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.OnStockReserved(); err != nil {
		t.Fatal(err)
	}
	store.orders[o.ID] = &o
	store.saved = append(store.saved, o)
	return &o
}

func freshPlacedOrder(t *testing.T, store *fakeStore) *Order {
	t.Helper()
	o, err := NewOrder("ord-live", "cus-1", []Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err != nil {
		t.Fatal(err)
	}
	store.orders[o.ID] = &o
	store.saved = append(store.saved, o)
	return &o
}

func TestOnStockReservedTransitionsToAwaitingPayment(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnStockReserved(context.Background(), "evt-sr-1", o.ID); err != nil {
		t.Fatalf("OnStockReserved: %v", err)
	}
	if got := store.orders[o.ID].Status; got != StatusAwaitingPayment {
		t.Errorf("status = %q, want AWAITING_PAYMENT", got)
	}
	if store.orders[o.ID].Terminal() {
		t.Error("order must not be terminal after reservation")
	}
	if len(store.outbox) != 0 {
		t.Errorf("StockReserved must not emit outbox events, got %d", len(store.outbox))
	}
}

func TestOnStockReservedDuplicateEventIsNoOp(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnStockReserved(context.Background(), "evt-sr-1", o.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.OnStockReserved(context.Background(), "evt-sr-1", o.ID); err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if got := store.orders[o.ID].Status; got != StatusAwaitingPayment {
		t.Errorf("status after replay = %q, want AWAITING_PAYMENT", got)
	}
}

func TestOnStockReservedRejectsWrongState(t *testing.T) {
	store := newFakeStore()
	o := awaitingPaymentOrder(t, store) // already AWAITING_PAYMENT
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	err := svc.OnStockReserved(context.Background(), "evt-sr-other", o.ID)
	if err == nil {
		t.Fatal("expected InvalidTransitionError for non-AWAITING_STOCK order")
	}
	var ite *InvalidTransitionError
	if !errors.As(err, &ite) {
		t.Fatalf("error %v is not *InvalidTransitionError", err)
	}
}

func TestOnStockReservedRollsBackOnUpdateFailure(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	store.failOn = "UpdateOrder"
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnStockReserved(context.Background(), "evt-sr-1", o.ID); err == nil {
		t.Fatal("expected UpdateOrder failure to propagate")
	}
	if got := store.orders[o.ID].Status; got != StatusAwaitingStock {
		t.Errorf("status after rollback = %q, want AWAITING_STOCK", got)
	}
	if store.processed["evt-sr-1"] {
		t.Error("processed_events guard must roll back with the tx")
	}
}

func TestOnPaymentCapturedConfirmsAndEmitsOrderConfirmed(t *testing.T) {
	store := newFakeStore()
	o := awaitingPaymentOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnPaymentCaptured(context.Background(), "evt-pc-1", o.ID); err != nil {
		t.Fatalf("OnPaymentCaptured: %v", err)
	}
	if got := store.orders[o.ID].Status; got != StatusConfirmed {
		t.Errorf("status = %q, want CONFIRMED", got)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 (OrderConfirmed)", len(store.outbox))
	}
	if store.outbox[0].RecordType != "orderflow.order.OrderConfirmed" {
		t.Errorf("record type = %q, want orderflow.order.OrderConfirmed", store.outbox[0].RecordType)
	}
	if store.outbox[0].AggregateID != o.ID {
		t.Errorf("aggregate = %q, want %q", store.outbox[0].AggregateID, o.ID)
	}
}

func TestOnPaymentCapturedDuplicateEventIsNoOp(t *testing.T) {
	store := newFakeStore()
	o := awaitingPaymentOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnPaymentCaptured(context.Background(), "evt-pc-1", o.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.OnPaymentCaptured(context.Background(), "evt-pc-1", o.ID); err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if got := store.orders[o.ID].Status; got != StatusConfirmed {
		t.Errorf("status after replay = %q, want CONFIRMED", got)
	}
	if len(store.outbox) != 1 {
		t.Errorf("duplicate must not emit a second OrderConfirmed, got %d", len(store.outbox))
	}
}

func TestOnPaymentCapturedUnknownOrderPropagates(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	err := svc.OnPaymentCaptured(context.Background(), "evt-pc-x", "no-such-order")
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("err = %v, want ErrOrderNotFound", err)
	}
}

func cancelledOrder(t *testing.T, store *fakeStore, reason CancelReason) *Order {
	t.Helper()
	o, err := NewOrder("ord-live", "cus-1", []Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}})
	if err != nil {
		t.Fatal(err)
	}
	o.Status = StatusCancelled
	o.CancelReason = reason
	store.orders[o.ID] = &o
	store.saved = append(store.saved, o)
	return &o
}

// Rule T2.1: StockUnavailable on AWAITING_STOCK → CANCELLED(STOCK_UNAVAILABLE)
// plus an OrderCancelled outbox event in the same transaction.
func TestOnStockUnavailableCancelsAndEmitsOrderCancelled(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnStockUnavailable(context.Background(), "evt-su-1", o.ID); err != nil {
		t.Fatalf("OnStockUnavailable: %v", err)
	}
	got := store.orders[o.ID]
	if got.Status != StatusCancelled || got.CancelReason != CancelStockUnavailable {
		t.Errorf("order = %q/%q, want CANCELLED/STOCK_UNAVAILABLE", got.Status, got.CancelReason)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 (OrderCancelled)", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.order.OrderCancelled" {
		t.Errorf("record type = %q, want orderflow.order.OrderCancelled", ev.RecordType)
	}
	if ev.AggregateID != o.ID {
		t.Errorf("aggregate = %q, want %q", ev.AggregateID, o.ID)
	}
	if ev.EventID != "evt-x-STOCK_UNAVAILABLE" {
		t.Errorf("event id = %q, want the STOCK_UNAVAILABLE reason to reach the encoder", ev.EventID)
	}
}

// Rule T2.2: PaymentFailed on AWAITING_PAYMENT → CANCELLED(PAYMENT_FAILED)
// plus an OrderCancelled outbox event in the same transaction.
func TestOnPaymentFailedCancelsAndEmitsOrderCancelled(t *testing.T) {
	store := newFakeStore()
	o := awaitingPaymentOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	if err := svc.OnPaymentFailed(context.Background(), "evt-pf-1", o.ID); err != nil {
		t.Fatalf("OnPaymentFailed: %v", err)
	}
	got := store.orders[o.ID]
	if got.Status != StatusCancelled || got.CancelReason != CancelPaymentFailed {
		t.Errorf("order = %q/%q, want CANCELLED/PAYMENT_FAILED", got.Status, got.CancelReason)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 (OrderCancelled)", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.order.OrderCancelled" {
		t.Errorf("record type = %q, want orderflow.order.OrderCancelled", ev.RecordType)
	}
	if ev.EventID != "evt-x-PAYMENT_FAILED" {
		t.Errorf("event id = %q, want the PAYMENT_FAILED reason to reach the encoder", ev.EventID)
	}
}

// Duplicate event ids on the compensation branches are silent no-ops:
// no second transition, no second outbox row.
func TestCompensationDuplicateEventIsNoOp(t *testing.T) {
	t.Run("stock unavailable replay", func(t *testing.T) {
		store := newFakeStore()
		o := freshPlacedOrder(t, store)
		svc := NewService(&fakeTx{store}, store, fakeEncoder{})
		if err := svc.OnStockUnavailable(context.Background(), "evt-su-1", o.ID); err != nil {
			t.Fatal(err)
		}
		if err := svc.OnStockUnavailable(context.Background(), "evt-su-1", o.ID); err != nil {
			t.Fatalf("duplicate replay must be a no-op, got %v", err)
		}
		if len(store.outbox) != 1 {
			t.Errorf("outbox events = %d, want 1", len(store.outbox))
		}
	})
	t.Run("payment failed replay", func(t *testing.T) {
		store := newFakeStore()
		o := awaitingPaymentOrder(t, store)
		svc := NewService(&fakeTx{store}, store, fakeEncoder{})
		if err := svc.OnPaymentFailed(context.Background(), "evt-pf-1", o.ID); err != nil {
			t.Fatal(err)
		}
		if err := svc.OnPaymentFailed(context.Background(), "evt-pf-1", o.ID); err != nil {
			t.Fatalf("duplicate replay must be a no-op, got %v", err)
		}
		if len(store.outbox) != 1 {
			t.Errorf("outbox events = %d, want 1", len(store.outbox))
		}
	})
}

// Rule T2.4: compensation events arriving in a wrong or terminal state are
// absorbed as safe no-ops — no error (offset commits), no state change,
// no outbox row. The event still lands in processed_events (tx committed).
func TestCompensationWrongStateIsAbsorbedAsNoOp(t *testing.T) {
	newConfirmed := func(t *testing.T, store *fakeStore) *Order {
		t.Helper()
		o := awaitingPaymentOrder(t, store)
		if err := o.OnPaymentCaptured(); err != nil {
			t.Fatal(err)
		}
		store.orders[o.ID] = o
		return o
	}
	cases := []struct {
		name  string
		sort  func(t *testing.T, store *fakeStore) *Order
		apply func(svc *Service, id string) error
	}{
		{"stock unavailable on awaiting payment", func(t *testing.T, store *fakeStore) *Order {
			return awaitingPaymentOrder(t, store)
		}, func(svc *Service, id string) error { return svc.OnStockUnavailable(context.Background(), "evt-su-w", id) }},
		{"stock unavailable on confirmed", newConfirmed, func(svc *Service, id string) error { return svc.OnStockUnavailable(context.Background(), "evt-su-w", id) }},
		{"stock unavailable on cancelled", func(t *testing.T, store *fakeStore) *Order {
			return cancelledOrder(t, store, CancelPaymentFailed)
		}, func(svc *Service, id string) error { return svc.OnStockUnavailable(context.Background(), "evt-su-w", id) }},
		{"payment failed on awaiting stock", func(t *testing.T, store *fakeStore) *Order {
			return freshPlacedOrder(t, store)
		}, func(svc *Service, id string) error { return svc.OnPaymentFailed(context.Background(), "evt-pf-w", id) }},
		{"payment failed on confirmed", newConfirmed, func(svc *Service, id string) error { return svc.OnPaymentFailed(context.Background(), "evt-pf-w", id) }},
		{"payment failed on already cancelled", func(t *testing.T, store *fakeStore) *Order {
			return cancelledOrder(t, store, CancelStockUnavailable)
		}, func(svc *Service, id string) error { return svc.OnPaymentFailed(context.Background(), "evt-pf-w", id) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			o := tc.sort(t, store)
			before := *store.orders[o.ID]
			svc := NewService(&fakeTx{store}, store, fakeEncoder{})

			if err := tc.apply(svc, o.ID); err != nil {
				t.Fatalf("wrong/terminal state must be absorbed without error, got %v", err)
			}
			got := store.orders[o.ID]
			if got.Status != before.Status || got.CancelReason != before.CancelReason {
				t.Errorf("state changed %q/%q → %q/%q, absorb must be a no-op",
					before.Status, before.CancelReason, got.Status, got.CancelReason)
			}
			if len(store.outbox) != 0 {
				t.Errorf("outbox events = %d, want 0 (absorbed)", len(store.outbox))
			}
			if len(store.processed) != 1 {
				t.Errorf("processed events = %d, want 1 (absorb still commits)", len(store.processed))
			}
		})
	}
}
