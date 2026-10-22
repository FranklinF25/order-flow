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
