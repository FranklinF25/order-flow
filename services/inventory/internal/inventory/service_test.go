package inventory

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
)

// fakeStore is an in-memory Store for deterministic app-service tests.
type fakeStore struct {
	processed    map[string]bool
	stock        map[string]int
	reservations []Reservation
	outbox       []OutboxEvent
	failOn       string // method name to force an error
	gets         int    // availability reads (proves the duplicate fast-path skips them)
}

func newFakeStore(seed map[string]int) *fakeStore {
	if seed == nil {
		seed = map[string]int{}
	}
	return &fakeStore{processed: map[string]bool{}, stock: seed}
}

func (f *fakeStore) MarkProcessed(_ context.Context, eventID string) error {
	if f.failOn == "MarkProcessed" {
		return errors.New("boom")
	}
	if f.processed[eventID] {
		return ErrDuplicateEvent
	}
	f.processed[eventID] = true
	return nil
}

func (f *fakeStore) GetStock(_ context.Context, sku string) (*Stock, error) {
	f.gets++
	if f.failOn == "GetStock" {
		return nil, errors.New("boom")
	}
	if n, ok := f.stock[sku]; ok {
		return &Stock{SKU: sku, Available: n}, nil
	}
	return nil, ErrStockNotFound
}

func (f *fakeStore) ListStock(_ context.Context) ([]Stock, error) {
	if f.failOn == "ListStock" {
		return nil, errors.New("boom")
	}
	out := make([]Stock, 0, len(f.stock))
	for sku, n := range f.stock {
		out = append(out, Stock{SKU: sku, Available: n})
	}
	slices.SortFunc(out, func(a, b Stock) int { return strings.Compare(a.SKU, b.SKU) })
	return out, nil
}

func (f *fakeStore) DecrementStock(_ context.Context, sku string, qty int) error {
	if f.failOn == "DecrementStock" {
		return errors.New("boom")
	}
	f.stock[sku] -= qty
	return nil
}

func (f *fakeStore) SaveReservation(_ context.Context, r Reservation) error {
	if f.failOn == "SaveReservation" {
		return errors.New("boom")
	}
	f.reservations = append(f.reservations, r)
	return nil
}

func (f *fakeStore) GetReservation(_ context.Context, orderID string) (*Reservation, error) {
	if f.failOn == "GetReservation" {
		return nil, errors.New("boom")
	}
	for _, r := range f.reservations {
		if r.OrderID == orderID {
			copy := r // mirrors SELECT: caller mutations must not leak into the store
			return &copy, nil
		}
	}
	return nil, ErrReservationNotFound
}

func (f *fakeStore) ReleaseReservation(_ context.Context, orderID string) (bool, error) {
	if f.failOn == "ReleaseReservation" {
		return false, errors.New("boom")
	}
	for i := range f.reservations {
		if f.reservations[i].OrderID == orderID {
			if f.reservations[i].Status != StatusReserved {
				return false, nil
			}
			f.reservations[i].Status = StatusReleased
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) IncrementStock(_ context.Context, sku string, qty int) error {
	if f.failOn == "IncrementStock" {
		return errors.New("boom")
	}
	f.stock[sku] += qty
	return nil
}

func (f *fakeStore) AppendOutbox(_ context.Context, e OutboxEvent) error {
	if f.failOn == "AppendOutbox" {
		return errors.New("boom")
	}
	f.outbox = append(f.outbox, e)
	return nil
}

// fakeTx models a real DB transaction: rollback restores the full snapshot.
type fakeTx struct{ store *fakeStore }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context, Store) error) error {
	processedSnap := maps.Clone(f.store.processed)
	stockSnap := maps.Clone(f.store.stock)
	resSnap := slices.Clone(f.store.reservations)
	outboxSnap := slices.Clone(f.store.outbox)
	if err := fn(ctx, f.store); err != nil {
		f.store.processed = processedSnap
		f.store.stock = stockSnap
		f.store.reservations = resSnap
		f.store.outbox = outboxSnap
		return err
	}
	return nil
}

// fakeEncoder stands in for the Avro driven adapter and records its calls.
type fakeEncoder struct {
	reserved    []Reservation
	unavailable []struct {
		orderID   string
		shortages []Shortage
	}
	released []struct {
		orderID string
		items   []Item
	}
}

func (f *fakeEncoder) StockReserved(r Reservation) (OutboxEvent, error) {
	f.reserved = append(f.reserved, r)
	return OutboxEvent{
		EventID:     "evt-r-" + r.OrderID,
		RecordType:  "orderflow.inventory.StockReserved",
		AggregateID: r.OrderID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

func (f *fakeEncoder) StockUnavailable(orderID string, shortages []Shortage) (OutboxEvent, error) {
	f.unavailable = append(f.unavailable, struct {
		orderID   string
		shortages []Shortage
	}{orderID, shortages})
	return OutboxEvent{
		EventID:     "evt-u-" + orderID,
		RecordType:  "orderflow.inventory.StockUnavailable",
		AggregateID: orderID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

func (f *fakeEncoder) StockReleased(orderID string, items []Item) (OutboxEvent, error) {
	f.released = append(f.released, struct {
		orderID string
		items   []Item
	}{orderID, items})
	return OutboxEvent{
		EventID:     "evt-rel-" + orderID,
		RecordType:  "orderflow.inventory.StockReleased",
		AggregateID: orderID,
		Payload:     []byte("avro-bytes"),
	}, nil
}

func placeInput(eventID, orderID, customerID string, items ...Item) ReserveInput {
	return ReserveInput{EventID: eventID, OrderID: orderID, CustomerID: customerID, TotalCents: 3000, Items: items}
}

func releaseInput(eventID, orderID string) ReleaseInput {
	return ReleaseInput{EventID: eventID, OrderID: orderID}
}

// seedReservation inserts a RESERVED reservation directly into the store.
func seedReservation(t *testing.T, store *fakeStore, orderID string, items ...Item) {
	t.Helper()
	r, err := NewReservation(orderID, "cus-1", 3000, items)
	if err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	store.reservations = append(store.reservations, r)
}

func TestHandleOrderPlacedSuccess(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100, "SKU-B": 50})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
		"evt-1", "ord-1", "cus-1", Item{SKU: "SKU-A", Qty: 2}, Item{SKU: "SKU-B", Qty: 1},
	))
	if err != nil {
		t.Fatalf("HandleOrderPlaced: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true")
	}
	if store.stock["SKU-A"] != 98 || store.stock["SKU-B"] != 49 {
		t.Errorf("stock = %v, want SKU-A:98 SKU-B:49", store.stock)
	}
	if store.gets != 2 {
		t.Errorf("availability reads = %d, want 2", store.gets)
	}
	if len(store.processed) != 1 || !store.processed["evt-1"] {
		t.Errorf("processed events = %v, want evt-1", store.processed)
	}
	if len(store.reservations) != 1 {
		t.Fatalf("reservations = %d, want 1", len(store.reservations))
	}
	r := store.reservations[0]
	if r.OrderID != "ord-1" || r.TotalCents != 3000 || len(r.Items) != 2 {
		t.Errorf("reservation = %+v, want ord-1/3000/2 items", r)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.inventory.StockReserved" || ev.AggregateID != "ord-1" {
		t.Errorf("event = %q/%q, want orderflow.inventory.StockReserved/ord-1", ev.RecordType, ev.AggregateID)
	}
	if len(ev.Payload) == 0 {
		t.Error("payload must encode the StockReserved record")
	}
	if len(enc.reserved) != 1 || enc.reserved[0].TotalCents != 3000 {
		t.Errorf("encoder reserved = %+v, want total 3000", enc.reserved)
	}
}

func TestHandleOrderPlacedAtomicShortfall(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100, "SKU-B": 5})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
		"evt-1", "ord-1", "cus-1", Item{SKU: "SKU-A", Qty: 2}, Item{SKU: "SKU-B", Qty: 10},
	))
	if err != nil {
		t.Fatalf("HandleOrderPlaced: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true (the shortage fact itself is applied)")
	}
	// No partial decrement: an unavailable line must leave ALL stock untouched.
	if store.stock["SKU-A"] != 100 || store.stock["SKU-B"] != 5 {
		t.Errorf("stock = %v, want untouched SKU-A:100 SKU-B:5", store.stock)
	}
	if len(store.reservations) != 0 {
		t.Errorf("reservations = %d, want 0", len(store.reservations))
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.inventory.StockUnavailable" || ev.AggregateID != "ord-1" {
		t.Errorf("event = %q/%q, want orderflow.inventory.StockUnavailable/ord-1", ev.RecordType, ev.AggregateID)
	}
	if len(ev.Payload) == 0 {
		t.Error("payload must encode the StockUnavailable record")
	}
	if len(enc.unavailable) != 1 {
		t.Fatalf("encoder unavailable calls = %d, want 1", len(enc.unavailable))
	}
	got := enc.unavailable[0].shortages
	want := []Shortage{{SKU: "SKU-B", Requested: 10, Available: 5}}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("shortages = %+v, want %+v", got, want)
	}
}

func TestHandleOrderPlacedUnknownSkuIsZeroAvailable(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 10})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
		"evt-1", "ord-1", "cus-1", Item{SKU: "SKU-A", Qty: 2}, Item{SKU: "SKU-Z", Qty: 1},
	))
	if err != nil {
		t.Fatalf("HandleOrderPlaced: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true")
	}
	if store.stock["SKU-A"] != 10 {
		t.Errorf("stock SKU-A = %d, want untouched 10", store.stock["SKU-A"])
	}
	got := enc.unavailable[0].shortages
	want := []Shortage{{SKU: "SKU-Z", Requested: 1, Available: 0}}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("shortages = %+v, want %+v", got, want)
	}
}

func TestHandleOrderPlacedDuplicateEventSkips(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100})
	store.processed["evt-1"] = true
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
		"evt-1", "ord-1", "cus-1", Item{SKU: "SKU-A", Qty: 2},
	))
	if err != nil {
		t.Fatalf("HandleOrderPlaced: %v", err)
	}
	if applied {
		t.Error("applied = true, want false (duplicate skip)")
	}
	if store.gets != 0 {
		t.Errorf("availability reads = %d, want 0 (skip before any check)", store.gets)
	}
	if store.stock["SKU-A"] != 100 {
		t.Errorf("stock SKU-A = %d, want untouched 100", store.stock["SKU-A"])
	}
	if len(store.reservations) != 0 || len(store.outbox) != 0 {
		t.Errorf("side effects after skip: reservations=%d outbox=%d, want 0/0",
			len(store.reservations), len(store.outbox))
	}
}

func TestHandleOrderPlacedRollbackOnStoreFailure(t *testing.T) {
	for _, failOn := range []string{"SaveReservation", "AppendOutbox"} {
		t.Run(failOn, func(t *testing.T) {
			store := newFakeStore(map[string]int{"SKU-A": 100, "SKU-B": 50})
			store.failOn = failOn
			enc := &fakeEncoder{}
			svc := NewService(&fakeTx{store}, store, enc)

			applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
				"evt-1", "ord-1", "cus-1", Item{SKU: "SKU-A", Qty: 2}, Item{SKU: "SKU-B", Qty: 1},
			))
			if err == nil {
				t.Fatal("expected error from store failure")
			}
			if applied {
				t.Error("applied = true, want false on failure")
			}
			// The whole unit of work must roll back: stock restored, nothing persisted.
			if store.stock["SKU-A"] != 100 || store.stock["SKU-B"] != 50 {
				t.Errorf("stock = %v, want rolled back SKU-A:100 SKU-B:50", store.stock)
			}
			if len(store.reservations) != 0 {
				t.Errorf("reservations = %d, want 0 (rollback)", len(store.reservations))
			}
			if len(store.outbox) != 0 {
				t.Errorf("outbox events = %d, want 0 (rollback)", len(store.outbox))
			}
			if store.processed["evt-1"] {
				t.Error("evt-1 marked processed after rollback — idempotency guard must roll back too")
			}
		})
	}
}

func TestHandleOrderPlacedValidationPropagates(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	_, err := svc.HandleOrderPlaced(context.Background(), ReserveInput{EventID: "evt-1"})
	if err == nil {
		t.Fatal("expected validation error for empty order id / no items")
	}
	if store.gets != 0 || len(store.outbox) != 0 {
		t.Errorf("store touched on validation error: gets=%d outbox=%d", store.gets, len(store.outbox))
	}
}

// Rule 1 (M2 S3): StockReserved v3 echoes OrderPlaced.customer_id so payment's
// simulated gateway can see the failure marker through its only subscription.
func TestHandleOrderPlacedEchoesCustomerID(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandleOrderPlaced(context.Background(), placeInput(
		"evt-1", "ord-1", "decline@test", Item{SKU: "SKU-A", Qty: 2},
	))
	if err != nil {
		t.Fatalf("HandleOrderPlaced: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true")
	}
	if len(enc.reserved) != 1 {
		t.Fatalf("encoder reserved calls = %d, want 1", len(enc.reserved))
	}
	if got := enc.reserved[0].CustomerID; got != "decline@test" {
		t.Errorf("echoed customer id = %q, want decline@test", got)
	}
	if got := store.reservations[0].CustomerID; got != "decline@test" {
		t.Errorf("persisted reservation customer id = %q, want decline@test", got)
	}
}

// Rule 3 (M2 S1): PaymentFailed releases the reservation in ONE tx —
// processed_events + RESERVED→RELEASED + stock restored + StockReleased outbox.
func TestHandlePaymentFailedReleasesReservation(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 98, "SKU-B": 49})
	seedReservation(t, store, "ord-1", Item{SKU: "SKU-A", Qty: 2}, Item{SKU: "SKU-B", Qty: 1})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandlePaymentFailed(context.Background(), releaseInput("evt-p-1", "ord-1"))
	if err != nil {
		t.Fatalf("HandlePaymentFailed: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true")
	}
	if store.stock["SKU-A"] != 100 || store.stock["SKU-B"] != 50 {
		t.Errorf("stock = %v, want restored SKU-A:100 SKU-B:50", store.stock)
	}
	if got := store.reservations[0].Status; got != StatusReleased {
		t.Errorf("reservation status = %s, want %s", got, StatusReleased)
	}
	if !store.processed["evt-p-1"] {
		t.Error("payment event must be marked processed in the same tx")
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 StockReleased", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.inventory.StockReleased" || ev.AggregateID != "ord-1" {
		t.Errorf("event = %q/%q, want orderflow.inventory.StockReleased/ord-1", ev.RecordType, ev.AggregateID)
	}
	if len(ev.Payload) == 0 {
		t.Error("payload must encode the StockReleased record")
	}
	if len(enc.released) != 1 || enc.released[0].orderID != "ord-1" || len(enc.released[0].items) != 2 {
		t.Errorf("encoder released = %+v, want ord-1 with 2 items", enc.released)
	}
}

// Rule 4a: a replayed PaymentFailed is absorbed — offset commits, but NO second
// stock restore and NO re-emitted StockReleased.
func TestHandlePaymentFailedDuplicateEventAbsorbed(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 98})
	seedReservation(t, store, "ord-1", Item{SKU: "SKU-A", Qty: 2})
	store.processed["evt-p-1"] = true // replay: the guard fires first
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandlePaymentFailed(context.Background(), releaseInput("evt-p-1", "ord-1"))
	if err != nil {
		t.Fatalf("HandlePaymentFailed: %v", err)
	}
	if applied {
		t.Error("applied = true, want false (duplicate absorbed)")
	}
	if store.stock["SKU-A"] != 98 {
		t.Errorf("stock SKU-A = %d, want untouched 98 (no second restore)", store.stock["SKU-A"])
	}
	if got := store.reservations[0].Status; got != StatusReserved {
		t.Errorf("reservation status = %s, want unchanged %s", got, StatusReserved)
	}
	if len(store.outbox) != 0 {
		t.Errorf("outbox events = %d, want 0 (no re-emitted StockReleased)", len(store.outbox))
	}
}

// Rule 4b: a release for an order already RELEASED (different PaymentFailed
// event id) is absorbed — no restore, no re-emit, no error.
func TestHandlePaymentFailedAlreadyReleasedAbsorbed(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100}) // stock back to pre-reservation level
	seedReservation(t, store, "ord-1", Item{SKU: "SKU-A", Qty: 2})
	store.reservations[0].Status = StatusReleased // already released earlier
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandlePaymentFailed(context.Background(), releaseInput("evt-p-2", "ord-1"))
	if err != nil {
		t.Fatalf("HandlePaymentFailed: %v", err)
	}
	if applied {
		t.Error("applied = true, want false (already released)")
	}
	if store.stock["SKU-A"] != 100 {
		t.Errorf("stock SKU-A = %d, want untouched 100 (no double restore)", store.stock["SKU-A"])
	}
	if len(store.outbox) != 0 {
		t.Errorf("outbox events = %d, want 0 (no re-emitted StockReleased)", len(store.outbox))
	}
	if !store.processed["evt-p-2"] {
		t.Error("absorbed event must still be marked processed (offset commits)")
	}
}

// Rule 4c: PaymentFailed for an order with no reservation at all (never placed,
// or StockUnavailable path) is absorbed safely — no error, no outbox.
func TestHandlePaymentFailedUnknownOrderAbsorbed(t *testing.T) {
	store := newFakeStore(map[string]int{"SKU-A": 100})
	enc := &fakeEncoder{}
	svc := NewService(&fakeTx{store}, store, enc)

	applied, err := svc.HandlePaymentFailed(context.Background(), releaseInput("evt-p-1", "ord-unknown"))
	if err != nil {
		t.Fatalf("HandlePaymentFailed: %v (unknown order must absorb, not error)", err)
	}
	if applied {
		t.Error("applied = true, want false (nothing to release)")
	}
	if store.stock["SKU-A"] != 100 {
		t.Errorf("stock SKU-A = %d, want untouched 100", store.stock["SKU-A"])
	}
	if len(store.outbox) != 0 {
		t.Errorf("outbox events = %d, want 0", len(store.outbox))
	}
	if !store.processed["evt-p-1"] {
		t.Error("absorbed event must still be marked processed (offset commits)")
	}
}

// Rule 3 atomicity: any mid-release failure rolls the whole unit of work back —
// stock untouched, reservation still RESERVED, outbox empty, guard not marked.
func TestHandlePaymentFailedRollbackOnStoreFailure(t *testing.T) {
	for _, failOn := range []string{"GetReservation", "ReleaseReservation", "IncrementStock", "AppendOutbox"} {
		t.Run(failOn, func(t *testing.T) {
			store := newFakeStore(map[string]int{"SKU-A": 98})
			seedReservation(t, store, "ord-1", Item{SKU: "SKU-A", Qty: 2})
			store.failOn = failOn
			enc := &fakeEncoder{}
			svc := NewService(&fakeTx{store}, store, enc)

			applied, err := svc.HandlePaymentFailed(context.Background(), releaseInput("evt-p-1", "ord-1"))
			if err == nil {
				t.Fatal("expected error from store failure")
			}
			if applied {
				t.Error("applied = true, want false on failure")
			}
			if store.stock["SKU-A"] != 98 {
				t.Errorf("stock SKU-A = %d, want rolled back 98", store.stock["SKU-A"])
			}
			if got := store.reservations[0].Status; got != StatusReserved {
				t.Errorf("reservation status = %s, want rolled back %s", got, StatusReserved)
			}
			if len(store.outbox) != 0 {
				t.Errorf("outbox events = %d, want 0 (rollback)", len(store.outbox))
			}
			if store.processed["evt-p-1"] {
				t.Error("evt-p-1 marked processed after rollback — guard must roll back too")
			}
		})
	}
}
