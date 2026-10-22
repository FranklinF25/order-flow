package payment

import (
	"context"
	"errors"
	"strconv"
	"testing"
)

// fakeStore is an in-memory Store for deterministic app-service tests.
type fakeStore struct {
	payments  map[string]*Payment // keyed by order_id (UNIQUE constraint stand-in)
	processed map[string]bool
	outbox    []OutboxEvent
	failOn    string // method name to force an error
}

func newFakeStore() *fakeStore {
	return &fakeStore{payments: map[string]*Payment{}, processed: map[string]bool{}}
}

func (f *fakeStore) SavePayment(_ context.Context, p *Payment) (bool, error) {
	if f.failOn == "SavePayment" {
		return false, errors.New("boom")
	}
	if _, ok := f.payments[p.OrderID]; ok {
		return false, nil // unique order_id hit: no-op
	}
	cp := *p
	f.payments[p.OrderID] = &cp
	return true, nil
}

func (f *fakeStore) AppendProcessedEvent(_ context.Context, eventID string) (bool, error) {
	if f.failOn == "AppendProcessedEvent" {
		return false, errors.New("boom")
	}
	if f.processed[eventID] {
		return false, nil // already processed: skip
	}
	f.processed[eventID] = true
	return true, nil
}

func (f *fakeStore) AppendOutbox(_ context.Context, e OutboxEvent) error {
	if f.failOn == "AppendOutbox" {
		return errors.New("boom")
	}
	f.outbox = append(f.outbox, e)
	return nil
}

// fakeTx models a real DB transaction: rollback restores prior state on error.
type fakeTx struct{ store *fakeStore }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context, Store) error) error {
	paySnap := make(map[string]*Payment, len(f.store.payments))
	for k, v := range f.store.payments {
		cp := *v
		paySnap[k] = &cp
	}
	procSnap := make(map[string]bool, len(f.store.processed))
	for k, v := range f.store.processed {
		procSnap[k] = v
	}
	outboxSnap := append([]OutboxEvent(nil), f.store.outbox...)
	if err := fn(ctx, f.store); err != nil {
		f.store.payments = paySnap
		f.store.processed = procSnap
		f.store.outbox = outboxSnap
		return err
	}
	return nil
}

// fakeGateway records capture attempts and returns a fixed payment id.
type fakeGateway struct {
	fail     bool
	orderIDs []string
	amounts  []int64
}

func (f *fakeGateway) Capture(_ context.Context, orderID string, amountCents int64) (string, error) {
	if f.fail {
		return "", errors.New("gateway down")
	}
	f.orderIDs = append(f.orderIDs, orderID)
	f.amounts = append(f.amounts, amountCents)
	return "pay-fixed-1", nil
}

// fakeEncoder stands in for the Avro driven adapter; the payload carries the
// amount so tests can assert what would travel on the wire.
type fakeEncoder struct{}

func (fakeEncoder) PaymentCaptured(p Payment) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-" + p.ID,
		RecordType:  "orderflow.payment.PaymentCaptured",
		AggregateID: p.OrderID,
		Payload:     []byte(strconv.FormatInt(p.AmountCents, 10)),
	}, nil
}

var captureInput = CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 4998}

func TestCapturePersistsPaymentGuardAndOutboxEvent(t *testing.T) {
	store := newFakeStore()
	gw := &fakeGateway{}
	svc := NewService(&fakeTx{store}, gw, fakeEncoder{})

	if err := svc.Capture(context.Background(), captureInput); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	p := store.payments["ord-1"]
	if p == nil {
		t.Fatal("payment not persisted")
	}
	if p.Status != StatusCaptured || p.AmountCents != 4998 || p.ID != "pay-fixed-1" {
		t.Errorf("payment = %+v, want CAPTURED/4998/pay-fixed-1", p)
	}
	if !store.processed["evt-stock-1"] {
		t.Error("processed event guard not persisted")
	}
	if len(gw.orderIDs) != 1 || gw.orderIDs[0] != "ord-1" || gw.amounts[0] != 4998 {
		t.Errorf("gateway capture = %v/%v, want ord-1/4998", gw.orderIDs, gw.amounts)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.AggregateID != "ord-1" {
		t.Errorf("event aggregate = %q, want order id %q", ev.AggregateID, "ord-1")
	}
	if ev.RecordType != "orderflow.payment.PaymentCaptured" {
		t.Errorf("record type = %q, want orderflow.payment.PaymentCaptured", ev.RecordType)
	}
	if string(ev.Payload) != "4998" {
		t.Errorf("payload = %q, want the charged amount 4998", ev.Payload)
	}
}

func TestCaptureDuplicateStockReservedEventIsNoOp(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, &fakeGateway{}, fakeEncoder{})

	ctx := context.Background()
	if err := svc.Capture(ctx, captureInput); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	// Same StockReserved delivered again (at-least-once redelivery).
	if err := svc.Capture(ctx, captureInput); err != nil {
		t.Fatalf("replayed Capture: %v", err)
	}
	if len(store.payments) != 1 {
		t.Errorf("payments = %d, want 1 (duplicate is a no-op)", len(store.payments))
	}
	if len(store.outbox) != 1 {
		t.Errorf("outbox events = %d, want 1 (no duplicate fact)", len(store.outbox))
	}
}

func TestCaptureSecondCaptureForSameOrderIsNoOp(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, &fakeGateway{}, fakeEncoder{})

	ctx := context.Background()
	if err := svc.Capture(ctx, captureInput); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	// A second capture for the same order under a different event id must be
	// absorbed by the order_id UNIQUE constraint, not error.
	second := captureInput
	second.EventID = "evt-stock-2"
	if err := svc.Capture(ctx, second); err != nil {
		t.Fatalf("second Capture: %v", err)
	}
	if len(store.payments) != 1 {
		t.Errorf("payments = %d, want 1", len(store.payments))
	}
	if len(store.outbox) != 1 {
		t.Errorf("outbox events = %d, want 1 (no duplicate PaymentCaptured)", len(store.outbox))
	}
}

func TestCaptureRollsBackOnOutboxFailure(t *testing.T) {
	store := newFakeStore()
	store.failOn = "AppendOutbox"
	svc := NewService(&fakeTx{store}, &fakeGateway{}, fakeEncoder{})

	err := svc.Capture(context.Background(), captureInput)
	if err == nil {
		t.Fatal("expected error from AppendOutbox failure")
	}
	// The whole unit of work must roll back: payment AND the processed-event
	// guard disappear so the redelivered event can be handled again.
	if len(store.payments) != 0 {
		t.Errorf("payments = %d, want 0 (rollback)", len(store.payments))
	}
	if len(store.processed) != 0 {
		t.Errorf("processed events = %d, want 0 (rollback)", len(store.processed))
	}
}

func TestCaptureRollsBackOnSavePaymentFailure(t *testing.T) {
	store := newFakeStore()
	store.failOn = "SavePayment"
	svc := NewService(&fakeTx{store}, &fakeGateway{}, fakeEncoder{})

	err := svc.Capture(context.Background(), captureInput)
	if err == nil {
		t.Fatal("expected error from SavePayment failure")
	}
	if len(store.processed) != 0 {
		t.Errorf("processed events = %d, want 0 (rollback)", len(store.processed))
	}
	if len(store.outbox) != 0 {
		t.Errorf("outbox events = %d, want 0 (rollback)", len(store.outbox))
	}
}

func TestCaptureGatewayErrorPropagatesWithoutPersisting(t *testing.T) {
	store := newFakeStore()
	gw := &fakeGateway{fail: true}
	svc := NewService(&fakeTx{store}, gw, fakeEncoder{})

	err := svc.Capture(context.Background(), captureInput)
	if err == nil {
		t.Fatal("expected gateway error to propagate")
	}
	if len(store.payments) != 0 || len(store.processed) != 0 || len(store.outbox) != 0 {
		t.Errorf("gateway failure must not persist: payments=%d processed=%d outbox=%d",
			len(store.payments), len(store.processed), len(store.outbox))
	}
}

func TestCaptureValidationPropagates(t *testing.T) {
	svc := NewService(&fakeTx{newFakeStore()}, &fakeGateway{}, fakeEncoder{})
	if err := svc.Capture(context.Background(), CaptureInput{EventID: "evt-1"}); err == nil {
		t.Error("expected validation error for empty order id")
	}
	if err := svc.Capture(context.Background(), CaptureInput{OrderID: "ord-1"}); err == nil {
		t.Error("expected validation error for empty event id")
	}
}
