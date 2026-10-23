package events

import (
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

// Rule 1 (M2 S3): the embedded stock-reserved.avsc is v3 and the encoder
// writes customer_id (echo of OrderPlaced.customer_id) into the payload.
func TestStockReservedEncodesCustomerID(t *testing.T) {
	r, err := inventory.NewReservation("ord-1", "decline@test", 29700,
		[]inventory.Item{{SKU: "SKU-A", Qty: 3}})
	if err != nil {
		t.Fatalf("NewReservation: %v", err)
	}
	ev, err := NewEncoder().StockReserved(r)
	if err != nil {
		t.Fatalf("StockReserved: %v", err)
	}

	c, err := codec("stock-reserved")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	var got StockReserved
	if err := avro.Unmarshal(c, ev.Payload, &got); err != nil {
		t.Fatalf("unmarshal StockReserved: %v", err)
	}
	if got.CustomerID != "decline@test" {
		t.Errorf("customer_id = %q, want decline@test (v3 echo)", got.CustomerID)
	}
	if got.OrderID != "ord-1" || got.TotalCents != 29700 || len(got.Items) != 1 || got.Items[0].SKU != "SKU-A" || got.Items[0].Qty != 3 {
		t.Errorf("record = %+v, want ord-1/29700/[SKU-A x3]", got)
	}
}

// Rule 3 (M2 S1): StockReleased encodes event_id/order_id/occurred_at/items and
// the outbox event_id matches the wire event_id.
func TestEncoderStockReleased(t *testing.T) {
	ev, err := NewEncoder().StockReleased("ord-1", []inventory.Item{{SKU: "SKU-A", Qty: 2}, {SKU: "SKU-B", Qty: 1}})
	if err != nil {
		t.Fatalf("StockReleased: %v", err)
	}
	if ev.RecordType != StockReleasedType || ev.AggregateID != "ord-1" {
		t.Errorf("outbox event = %q/%q, want %s/ord-1", ev.RecordType, ev.AggregateID, StockReleasedType)
	}

	c, err := codec("stock-released")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	var got StockReleased
	if err := avro.Unmarshal(c, ev.Payload, &got); err != nil {
		t.Fatalf("unmarshal StockReleased: %v", err)
	}
	if got.EventID == "" || got.EventID != ev.EventID {
		t.Errorf("wire event_id = %q, outbox event_id = %q (must match)", got.EventID, ev.EventID)
	}
	if got.OrderID != "ord-1" || got.OccurredAt == "" {
		t.Errorf("record = %+v, want ord-1 with occurred_at set", got)
	}
	if len(got.Items) != 2 || got.Items[0] != (StockItem{SKU: "SKU-A", Qty: 2}) || got.Items[1] != (StockItem{SKU: "SKU-B", Qty: 1}) {
		t.Errorf("items = %+v, want SKU-A x2 + SKU-B x1", got.Items)
	}
}

// Rule 2 (M2 S1): the embedded payment-failed.avsc decodes the payment
// service's PaymentFailed fact (amount_cents + failure_reason enum).
func TestDecodePaymentFailed(t *testing.T) {
	c, err := codec("payment-failed")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	payload, err := avro.Marshal(c, PaymentFailed{
		EventID:       "evt-p-1",
		OrderID:       "ord-1",
		OccurredAt:    "2026-10-23T00:00:00Z",
		AmountCents:   29700,
		FailureReason: "CARD_DECLINED",
	})
	if err != nil {
		t.Fatalf("marshal PaymentFailed: %v", err)
	}

	got, err := DecodePaymentFailed(payload)
	if err != nil {
		t.Fatalf("DecodePaymentFailed: %v", err)
	}
	if got.EventID != "evt-p-1" || got.OrderID != "ord-1" || got.AmountCents != 29700 || got.FailureReason != "CARD_DECLINED" {
		t.Errorf("decoded = %+v, want evt-p-1/ord-1/29700/CARD_DECLINED", got)
	}
}
