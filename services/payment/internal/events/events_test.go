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
	c, err := codec("stock-reserved")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	body, err := avro.Marshal(c, StockReserved{
		EventID:    "evt-stock-1",
		OrderID:    "ord-1",
		OccurredAt: "2026-10-22T13:00:00Z",
		Items:      []StockItem{{SKU: "SKU-A", Qty: 2}},
		TotalCents: 4998,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	value := make([]byte, 5+len(body))
	value[0] = 0x00
	binary.BigEndian.PutUint32(value[1:5], 42)
	copy(value[5:], body)

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
