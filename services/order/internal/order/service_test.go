package order

import (
	"context"
	"errors"
	"testing"
)

// fakeStore is an in-memory Store for deterministic app-service tests.
type fakeStore struct {
	saved     []Order
	outbox    []OutboxEvent
	orders    map[string]*Order
	processed map[string]bool
	failOn    string // method name to force an error
}

func newFakeStore() *fakeStore {
	return &fakeStore{orders: map[string]*Order{}, processed: map[string]bool{}}
}

func (f *fakeStore) SaveOrder(_ context.Context, o *Order) error {
	if f.failOn == "SaveOrder" {
		return errors.New("boom")
	}
	f.saved = append(f.saved, *o)
	f.orders[o.ID] = o
	return nil
}

func (f *fakeStore) GetOrder(_ context.Context, id string) (*Order, error) {
	if o, ok := f.orders[id]; ok {
		cp := *o
		return &cp, nil
	}
	return nil, ErrOrderNotFound
}

func (f *fakeStore) AppendOutbox(_ context.Context, e OutboxEvent) error {
	if f.failOn == "AppendOutbox" {
		return errors.New("boom")
	}
	f.outbox = append(f.outbox, e)
	return nil
}

func (f *fakeStore) UpdateOrder(_ context.Context, o *Order) error {
	if f.failOn == "UpdateOrder" {
		return errors.New("boom")
	}
	f.orders[o.ID] = o
	for i := range f.saved {
		if f.saved[i].ID == o.ID {
			f.saved[i] = *o
		}
	}
	return nil
}

func (f *fakeStore) InsertProcessedEvent(_ context.Context, eventID string) (bool, error) {
	if f.failOn == "InsertProcessedEvent" {
		return false, errors.New("boom")
	}
	if f.processed[eventID] {
		return false, nil
	}
	f.processed[eventID] = true
	return true, nil
}

// fakeTx models a real DB transaction: rollback restores prior state on error.
type fakeTx struct{ store *fakeStore }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context, Store) error) error {
	savedSnap := append([]Order(nil), f.store.saved...)
	outboxSnap := append([]OutboxEvent(nil), f.store.outbox...)
	ordersSnap := make(map[string]*Order, len(f.store.orders))
	for k, v := range f.store.orders {
		cp := *v
		ordersSnap[k] = &cp
	}
	processedSnap := make(map[string]bool, len(f.store.processed))
	for k, v := range f.store.processed {
		processedSnap[k] = v
	}
	if err := fn(ctx, f.store); err != nil {
		f.store.saved = savedSnap
		f.store.outbox = outboxSnap
		f.store.orders = ordersSnap
		f.store.processed = processedSnap
		return err
	}
	return nil
}

// fakeEncoder stands in for the Avro driven adapter.
type fakeEncoder struct{}

func (fakeEncoder) OrderPlaced(o Order) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-" + o.ID,
		RecordType:  "orderflow.order.OrderPlaced",
		AggregateID: o.ID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

func (fakeEncoder) OrderConfirmed(o Order) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-c-" + o.ID,
		RecordType:  "orderflow.order.OrderConfirmed",
		AggregateID: o.ID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

func TestPlaceOrderPersistsOrderAndOutboxEvent(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})

	dto := PlaceOrderInput{
		CustomerID: "cus-1",
		Items:      []Item{{SKU: "SKU-A", Qty: 2, UnitPriceCents: 1500}},
	}
	o, err := svc.PlaceOrder(context.Background(), dto)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if o.Status != StatusAwaitingStock || o.TotalCents != 3000 {
		t.Errorf("order = %q/%d, want AWAITING_STOCK/3000", o.Status, o.TotalCents)
	}
	if o.ID == "" {
		t.Error("order id must be generated")
	}
	if len(store.saved) != 1 {
		t.Fatalf("saved orders = %d, want 1", len(store.saved))
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.AggregateID != o.ID {
		t.Errorf("event aggregate = %q, want order id %q", ev.AggregateID, o.ID)
	}
	if ev.RecordType != "orderflow.order.OrderPlaced" {
		t.Errorf("record type = %q, want orderflow.order.OrderPlaced", ev.RecordType)
	}
	if len(ev.Payload) == 0 {
		t.Error("payload must encode the OrderPlaced record")
	}
}

func TestPlaceOrderValidationPropagates(t *testing.T) {
	svc := NewService(&fakeTx{newFakeStore()}, newFakeStore(), fakeEncoder{})
	_, err := svc.PlaceOrder(context.Background(), PlaceOrderInput{CustomerID: "cus-1"})
	if err == nil {
		t.Fatal("expected validation error for empty items")
	}
}

func TestPlaceOrderAtomicOnStoreFailure(t *testing.T) {
	store := newFakeStore()
	store.failOn = "AppendOutbox"
	svc := NewService(&fakeTx{store}, store, fakeEncoder{})
	_, err := svc.PlaceOrder(context.Background(), PlaceOrderInput{
		CustomerID: "cus-1",
		Items:      []Item{{SKU: "SKU-A", Qty: 1, UnitPriceCents: 100}},
	})
	if err == nil {
		t.Fatal("expected error from AppendOutbox failure")
	}
	// The whole unit of work must roll back: no order persisted.
	if len(store.saved) != 0 {
		t.Errorf("saved orders = %d, want 0 (rollback)", len(store.saved))
	}
}
