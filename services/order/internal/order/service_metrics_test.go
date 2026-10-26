package order

import (
	"context"
	"testing"

	"github.com/FranklinF25/order-flow/services/order/internal/metrics"
)

// The domain knows when an event was ABSORBED (DB-guard duplicate,
// compensation no-op). It reports that through the context carrier the
// instrumented consumer places in ctx (M3 events_total outcome=absorbed);
// these tests pin each domain absorb path at the carrier level.

func carrierCtx() (context.Context, *metrics.EventOutcome) {
	o := metrics.NewEventOutcome()
	return metrics.ContextWithOutcome(context.Background(), o), o
}

func TestDuplicateEventMarksOutcomeAbsorbed(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	ctx, outcome := carrierCtx()
	if err := svc.OnStockReserved(ctx, "evt-sr-1", o.ID); err != nil {
		t.Fatal(err)
	}
	if outcome.Absorbed() {
		t.Error("first delivery must count processed, not absorbed")
	}

	ctx2, outcome2 := carrierCtx()
	if err := svc.OnStockReserved(ctx2, "evt-sr-1", o.ID); err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if !outcome2.Absorbed() {
		t.Error("duplicate replay must mark the outcome absorbed")
	}

	// The happy-path branch for PaymentCaptured carries the same contract.
	store2 := newFakeStore()
	o2 := awaitingPaymentOrder(t, store2)
	svc2 := NewService(&fakeTx{store2}, store2, fakeEncoder{})
	ctx3, outcome3 := carrierCtx()
	if err := svc2.OnPaymentCaptured(ctx3, "evt-pc-1", o2.ID); err != nil {
		t.Fatal(err)
	}
	if outcome3.Absorbed() {
		t.Error("fresh PaymentCaptured must count processed")
	}
	ctx4, outcome4 := carrierCtx()
	if err := svc2.OnPaymentCaptured(ctx4, "evt-pc-1", o2.ID); err != nil {
		t.Fatalf("duplicate replay must be a no-op, got %v", err)
	}
	if !outcome4.Absorbed() {
		t.Error("duplicate PaymentCaptured replay must mark absorbed")
	}
}

func TestCompensationAbsorbMarksOutcomeAbsorbed(t *testing.T) {
	store := newFakeStore()
	o := awaitingPaymentOrder(t, store) // StockUnavailable here is a wrong-state absorb
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	ctx, outcome := carrierCtx()
	if err := svc.OnStockUnavailable(ctx, "evt-su-1", o.ID); err != nil {
		t.Fatalf("wrong-state compensation must absorb without error, got %v", err)
	}
	if !outcome.Absorbed() {
		t.Error("compensation absorb must mark the outcome absorbed")
	}
	if len(store.outbox) != 0 {
		t.Errorf("outbox events = %d, want 0 (absorb emits nothing)", len(store.outbox))
	}
}

func TestProcessedTransitionDoesNotMarkAbsorbed(t *testing.T) {
	store := newFakeStore()
	o := freshPlacedOrder(t, store)
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	ctx, outcome := carrierCtx()
	if err := svc.OnStockReserved(ctx, "evt-sr-1", o.ID); err != nil {
		t.Fatal(err)
	}
	if outcome.Absorbed() {
		t.Error("a real transition must never mark absorbed")
	}

	// Same for the cancellation branch that REALLY cancels.
	store2 := newFakeStore()
	o2 := freshPlacedOrder(t, store2)
	svc2 := NewService(&fakeTx{store2}, store2, fakeEncoder{})
	ctx2, outcome2 := carrierCtx()
	if err := svc2.OnStockUnavailable(ctx2, "evt-su-2", o2.ID); err != nil {
		t.Fatal(err)
	}
	if outcome2.Absorbed() {
		t.Error("a real cancellation must never mark absorbed")
	}
	if got := store2.orders[o2.ID].Status; got != StatusCancelled {
		t.Errorf("status = %q, want CANCELLED (metric sits on a real transition)", got)
	}
}

// AllStatuses is the pinned label set of orderflow_saga_orders: exactly the
// statuses the domain defines, in lifecycle order.
func TestStatusesMatchDomain(t *testing.T) {
	want := []Status{StatusAwaitingStock, StatusAwaitingPayment, StatusConfirmed, StatusCancelled}
	if len(Statuses) != len(want) {
		t.Fatalf("Statuses = %v, want %v", Statuses, want)
	}
	for i := range want {
		if Statuses[i] != want[i] {
			t.Errorf("Statuses[%d] = %q, want %q", i, Statuses[i], want[i])
		}
	}
}
