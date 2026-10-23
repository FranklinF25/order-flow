package events

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

func TestPaymentCapturedEncodesForTheWire(t *testing.T) {
	p := payment.Payment{ID: "pay-1", OrderID: "ord-1", AmountCents: 4998, Status: payment.StatusCaptured}
	ev, err := Encoder{}.PaymentCaptured(p)
	if err != nil {
		t.Fatalf("PaymentCaptured: %v", err)
	}
	if ev.RecordType != PaymentCapturedType {
		t.Errorf("record type = %q, want %q", ev.RecordType, PaymentCapturedType)
	}
	if ev.AggregateID != "ord-1" {
		t.Errorf("aggregate = %q, want order id", ev.AggregateID)
	}
	if ev.EventID == "" {
		t.Error("event id must be generated")
	}

	c, err := codec("payment-captured")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	var wire PaymentCaptured
	if err := avro.Unmarshal(c, ev.Payload, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wire.OrderID != "ord-1" || wire.PaymentID != "pay-1" || wire.AmountCents != 4998 {
		t.Errorf("wire = %+v, want ord-1/pay-1/4998", wire)
	}
	if wire.EventID != ev.EventID || wire.OccurredAt == "" {
		t.Errorf("wire ids = %q/%q, event id and occurred_at must be set", wire.EventID, wire.OccurredAt)
	}
}

func TestDecodeStockReservedWireFormat(t *testing.T) {
	// Build a registry-shaped value: magic + int32 BE schema id + Avro body.
	value := wireValue(t, "", StockReserved{
		EventID:    "evt-stock-1",
		OrderID:    "ord-1",
		OccurredAt: "2026-10-22T13:00:00Z",
		Items:      []StockItem{{SKU: "SKU-A", Qty: 2}},
		TotalCents: 4998,
	})

	evt, err := Decoder{}.DecodeStockReserved(value)
	if err != nil {
		t.Fatalf("DecodeStockReserved: %v", err)
	}
	if evt.EventID != "evt-stock-1" || evt.OrderID != "ord-1" || evt.TotalCents != 4998 {
		t.Errorf("event = %+v, want evt-stock-1/ord-1/4998", evt)
	}
	if len(evt.Items) != 1 || evt.Items[0].SKU != "SKU-A" || evt.Items[0].Qty != 2 {
		t.Errorf("items = %+v, want [{SKU-A 2}]", evt.Items)
	}
}

// stockReservedV2Schema is the pre-v3 writer schema (without customer_id),
// used to prove the BACKWARD_TRANSITIVE read contract: a v2 record must decode
// against the embedded v3 reader with customer_id at its default.
const stockReservedV2Schema = `{
  "type": "record",
  "name": "StockReserved",
  "namespace": "orderflow.inventory",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "occurred_at", "type": "string"},
    {"name": "items", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "StockItem",
        "fields": [
          {"name": "sku", "type": "string"},
          {"name": "qty", "type": "int"}
        ]
      }
    }},
    {"name": "total_cents", "type": "long", "default": 0}
  ]
}`

// wireValue marshals evt with the embedded schema (schemaText "") or the given
// inline schema, wrapped in the 5-byte Confluent header.
func wireValue(t *testing.T, schemaText string, evt StockReserved) []byte {
	t.Helper()
	var c avro.Schema
	var err error
	if schemaText == "" {
		c, err = codec("stock-reserved")
	} else {
		c, err = avro.Parse(schemaText)
	}
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	body, err := avro.Marshal(c, evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	value := make([]byte, 5+len(body))
	value[0] = 0x00
	binary.BigEndian.PutUint32(value[1:5], 42)
	copy(value[5:], body)
	return value
}

// TestDecodeStockReservedV3CarriesCustomerID proves the v3 echo (M2 marker
// carrier) survives the consume path: customer_id lands on the decoded event.
func TestDecodeStockReservedV3CarriesCustomerID(t *testing.T) {
	value := wireValue(t, "", StockReserved{
		EventID:    "evt-stock-3",
		OrderID:    "ord-3",
		OccurredAt: "2026-10-23T10:00:00Z",
		Items:      []StockItem{{SKU: "SKU-B", Qty: 3}},
		TotalCents: 29700,
		CustomerID: "decline@x.com",
	})
	evt, err := Decoder{}.DecodeStockReserved(value)
	if err != nil {
		t.Fatalf("DecodeStockReserved: %v", err)
	}
	if evt.CustomerID != "decline@x.com" {
		t.Errorf("customer_id = %q, want decline@x.com (the failure marker)", evt.CustomerID)
	}
}

// TestDecodeStockReservedV2AppliesDefaultCustomerID proves inventory's pre-v3
// records still decode: customer_id falls back to its "" default (L5/L3
// additive-schema precedent).
func TestDecodeStockReservedV2AppliesDefaultCustomerID(t *testing.T) {
	value := wireValue(t, stockReservedV2Schema, StockReserved{
		EventID:    "evt-stock-2",
		OrderID:    "ord-2",
		OccurredAt: "2026-10-22T13:00:00Z",
		Items:      []StockItem{{SKU: "SKU-A", Qty: 2}},
		TotalCents: 4998,
	})
	evt, err := Decoder{}.DecodeStockReserved(value)
	if err != nil {
		t.Fatalf("DecodeStockReserved: %v", err)
	}
	if evt.CustomerID != "" {
		t.Errorf("customer_id = %q, want the default \"\" for v2 records", evt.CustomerID)
	}
}

// TestPaymentFailedEncodesForTheWire proves the compensation-B fact carries
// order, amount and the failure reason enum on the wire.
func TestPaymentFailedEncodesForTheWire(t *testing.T) {
	ev, err := Encoder{}.PaymentFailed("ord-1", 4998, payment.ReasonCardDeclined)
	if err != nil {
		t.Fatalf("PaymentFailed: %v", err)
	}
	if ev.RecordType != PaymentFailedType {
		t.Errorf("record type = %q, want %q", ev.RecordType, PaymentFailedType)
	}
	if ev.AggregateID != "ord-1" {
		t.Errorf("aggregate = %q, want order id", ev.AggregateID)
	}
	if ev.EventID == "" {
		t.Error("event id must be generated")
	}

	c, err := codec("payment-failed")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	var wire PaymentFailed
	if err := avro.Unmarshal(c, ev.Payload, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wire.OrderID != "ord-1" || wire.AmountCents != 4998 {
		t.Errorf("wire = %+v, want ord-1/4998", wire)
	}
	if wire.FailureReason != "CARD_DECLINED" {
		t.Errorf("failure_reason = %q, want CARD_DECLINED", wire.FailureReason)
	}
	if wire.EventID != ev.EventID || wire.OccurredAt == "" {
		t.Errorf("wire ids = %q/%q, event id and occurred_at must be set", wire.EventID, wire.OccurredAt)
	}
}

func TestDecodeStockReservedRejectsBadWireFormat(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
	}{
		{"too short", []byte{0x00, 0x00}},
		{"wrong magic", append([]byte{0x03, 0, 0, 0, 1}, []byte("junk")...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := (Decoder{}).DecodeStockReserved(tc.value); err == nil {
				t.Fatal("expected wire-format error")
			} else if !strings.Contains(err.Error(), "wire") && !strings.Contains(err.Error(), "unmarshal") {
				t.Errorf("error = %v, want wire or unmarshal failure", err)
			}
		})
	}
}
