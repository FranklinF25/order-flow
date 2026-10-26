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

// fakeGateway records capture attempts and returns a fixed payment id. It
// stands in for an always-capturing gateway; declined charges are exercised
// through the real SimulatedGateway, gateway outages through fail.
type fakeGateway struct {
	fail        bool
	orderIDs    []string
	amounts     []int64
	customerIDs []string
}

func (f *fakeGateway) Capture(_ context.Context, orderID string, amountCents int64, customerID string) (string, error) {
	if f.fail {
		return "", errors.New("gateway down")
	}
	f.orderIDs = append(f.orderIDs, orderID)
	f.amounts = append(f.amounts, amountCents)
	f.customerIDs = append(f.customerIDs, customerID)
	return "pay-fixed-1", nil
}

// fakeEncoder stands in for the Avro driven adapter; payloads carry what
// would travel on the wire so tests can assert the facts' content.
type fakeEncoder struct{}

func (fakeEncoder) PaymentCaptured(p Payment) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-" + p.ID,
		RecordType:  "orderflow.payment.PaymentCaptured",
		AggregateID: p.OrderID,
		Payload:     []byte(strconv.FormatInt(p.AmountCents, 10)),
	}, nil
}

func (fakeEncoder) PaymentFailed(orderID string, amountCents int64, reason FailureReason) (OutboxEvent, error) {
	return OutboxEvent{
		EventID:     "evt-failed-" + orderID,
		RecordType:  "orderflow.payment.PaymentFailed",
		AggregateID: orderID,
		Payload:     []byte(string(reason) + ":" + strconv.FormatInt(amountCents, 10)),
	}, nil
}

var captureInput = CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 4998, CustomerID: "cus-ok-1"}

// TestCaptureDeclinedMarkerPersistsFailedPaymentAndOutbox proves the M2 fail
// path (S1/S3): a marker customer reaches the gateway, comes back declined,
// and ONE transaction persists the processed-event guard, the FAILED payment
// row (amount from StockReserved.total_cents) and the PaymentFailed fact —
// with NO PaymentCaptured emitted.
func TestCaptureDeclinedMarkerPersistsFailedPaymentAndOutbox(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, NewSimulatedGateway(), fakeEncoder{})

	in := CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 4998, CustomerID: "decline@x.com"}
	if _, err := svc.Capture(context.Background(), in); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	p := store.payments["ord-1"]
	if p == nil {
		t.Fatal("FAILED payment not persisted")
	}
	if p.Status != StatusFailed || p.AmountCents != 4998 {
		t.Errorf("payment = %+v, want FAILED/4998", p)
	}
	if !store.processed["evt-stock-1"] {
		t.Error("processed event guard not persisted")
	}
	if len(store.outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1 PaymentFailed", len(store.outbox))
	}
	ev := store.outbox[0]
	if ev.RecordType != "orderflow.payment.PaymentFailed" {
		t.Errorf("record type = %q, want orderflow.payment.PaymentFailed", ev.RecordType)
	}
	if ev.AggregateID != "ord-1" {
		t.Errorf("aggregate = %q, want order id", ev.AggregateID)
	}
	if string(ev.Payload) != "CARD_DECLINED:4998" {
		t.Errorf("payload = %q, want reason CARD_DECLINED with amount 4998", ev.Payload)
	}
}

// TestCaptureMarkerReasonsReachPaymentFailed proves all three marker reasons
// survive the whole app service (real gateway adapter → PaymentFailed fact),
// including case-insensitivity of the marker.
func TestCaptureMarkerReasonsReachPaymentFailed(t *testing.T) {
	cases := []struct {
		name, customerID string
		wantReason       FailureReason
	}{
		{"decline", "decline@x.com", ReasonCardDeclined},
		{"insufficient, uppercase", "INSUFFICIENT@x.com", ReasonInsufficientFunds},
		{"gateway error", "ERROR-x", ReasonGatewayError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			svc := NewService(&fakeTx{store}, NewSimulatedGateway(), fakeEncoder{})
			in := CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 2500, CustomerID: tc.customerID}
			if _, err := svc.Capture(context.Background(), in); err != nil {
				t.Fatalf("Capture: %v", err)
			}
			if p := store.payments["ord-1"]; p == nil || p.Status != StatusFailed {
				t.Fatalf("payment = %+v, want FAILED", p)
			}
			if len(store.outbox) != 1 {
				t.Fatalf("outbox events = %d, want 1", len(store.outbox))
			}
			if got := string(store.outbox[0].Payload); got != string(tc.wantReason)+":2500" {
				t.Errorf("payload = %q, want %s:2500", got, tc.wantReason)
			}
		})
	}
}

// TestCaptureReplayedDeclinedEventIsNoOp proves idempotency on the fail path:
// a replayed StockReserved (same event id) and a second decline under a new
// event id both absorb against the guard/UNIQUE constraints — one payment row,
// one outbox row.
func TestCaptureReplayedDeclinedEventIsNoOp(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, NewSimulatedGateway(), fakeEncoder{})

	ctx := context.Background()
	in := CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 4998, CustomerID: "decline@x.com"}
	if _, err := svc.Capture(ctx, in); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	if _, err := svc.Capture(ctx, in); err != nil {
		t.Fatalf("replayed Capture: %v", err)
	}
	second := in
	second.EventID = "evt-stock-2"
	if _, err := svc.Capture(ctx, second); err != nil {
		t.Fatalf("second-event Capture: %v", err)
	}
	if len(store.payments) != 1 {
		t.Errorf("payments = %d, want 1", len(store.payments))
	}
	if len(store.outbox) != 1 {
		t.Errorf("outbox events = %d, want 1 (no duplicate PaymentFailed)", len(store.outbox))
	}
}

// TestCapturePlainCustomerStillCaptures proves the success path is unchanged
// for non-marker customers and that the customer id reaches the gateway port.
func TestCapturePlainCustomerStillCaptures(t *testing.T) {
	store := newFakeStore()
	gw := &fakeGateway{}
	svc := NewService(&fakeTx{store}, gw, fakeEncoder{})

	if _, err := svc.Capture(context.Background(), captureInput); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	p := store.payments["ord-1"]
	if p == nil || p.Status != StatusCaptured {
		t.Fatalf("payment = %+v, want CAPTURED", p)
	}
	if len(store.outbox) != 1 || store.outbox[0].RecordType != "orderflow.payment.PaymentCaptured" {
		t.Errorf("outbox = %+v, want one PaymentCaptured", store.outbox)
	}
	if len(gw.customerIDs) != 1 || gw.customerIDs[0] != "cus-ok-1" {
		t.Errorf("gateway customer ids = %v, want [cus-ok-1]", gw.customerIDs)
	}
}

func TestCapturePersistsPaymentGuardAndOutboxEvent(t *testing.T) {
	store := newFakeStore()
	gw := &fakeGateway{}
	svc := NewService(&fakeTx{store}, gw, fakeEncoder{})

	if _, err := svc.Capture(context.Background(), captureInput); err != nil {
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
	if _, err := svc.Capture(ctx, captureInput); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	// Same StockReserved delivered again (at-least-once redelivery).
	if _, err := svc.Capture(ctx, captureInput); err != nil {
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
	if _, err := svc.Capture(ctx, captureInput); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	// A second capture for the same order under a different event id must be
	// absorbed by the order_id UNIQUE constraint, not error.
	second := captureInput
	second.EventID = "evt-stock-2"
	if _, err := svc.Capture(ctx, second); err != nil {
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

	_, err := svc.Capture(context.Background(), captureInput)
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

	_, err := svc.Capture(context.Background(), captureInput)
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

	_, err := svc.Capture(context.Background(), captureInput)
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
	if _, err := svc.Capture(context.Background(), CaptureInput{EventID: "evt-1"}); err == nil {
		t.Error("expected validation error for empty order id")
	}
	if _, err := svc.Capture(context.Background(), CaptureInput{OrderID: "ord-1"}); err == nil {
		t.Error("expected validation error for empty event id")
	}
}

// TestCaptureReportsFreshVsAbsorbed proves the M3 metric-contract seam: the
// first delivery of an event id is FRESH (the processed_events guard inserted
// it — outcome=processed for orderflow_events_total) and a redelivered event
// id is a duplicate-marker replay the DB absorbs (fresh=false —
// outcome=absorbed). A second capture for the same order under a NEW event id
// is still fresh: the order_id UNIQUE no-op absorbs the write, not the event.
func TestCaptureReportsFreshVsAbsorbed(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, &fakeGateway{}, fakeEncoder{})

	ctx := context.Background()
	fresh, err := svc.Capture(ctx, captureInput)
	if err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	if !fresh {
		t.Error("first delivery of an event id must report fresh=true (outcome=processed)")
	}
	fresh, err = svc.Capture(ctx, captureInput)
	if err != nil {
		t.Fatalf("replayed Capture: %v", err)
	}
	if fresh {
		t.Error("duplicate-marker replay must report fresh=false (outcome=absorbed)")
	}
	second := captureInput
	second.EventID = "evt-stock-2"
	fresh, err = svc.Capture(ctx, second)
	if err != nil {
		t.Fatalf("second-event Capture: %v", err)
	}
	if !fresh {
		t.Error("new event id on an already-captured order is still a fresh EVENT (guard inserted it)")
	}
}

// The fail path mirrors the guard semantics: a fresh declined marker is
// fresh, its replay is absorbed.
func TestCaptureFailPathReportsFreshVsAbsorbed(t *testing.T) {
	store := newFakeStore()
	svc := NewService(&fakeTx{store}, NewSimulatedGateway(), fakeEncoder{})

	in := CaptureInput{EventID: "evt-stock-1", OrderID: "ord-1", AmountCents: 2500, CustomerID: "decline@x.com"}
	fresh, err := svc.Capture(context.Background(), in)
	if err != nil {
		t.Fatalf("declined Capture: %v", err)
	}
	if !fresh {
		t.Error("first declined delivery must report fresh=true")
	}
	fresh, err = svc.Capture(context.Background(), in)
	if err != nil {
		t.Fatalf("replayed declined Capture: %v", err)
	}
	if fresh {
		t.Error("replayed declined event must report fresh=false (guard absorbed it)")
	}
}
