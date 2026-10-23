package events

import (
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

// Rule T2.3: the service must embed a byte-equal copy of
// schemas/order/order-cancelled.avsc (canonical source stays schemas/; the
// copy itself is verified with cmp outside the suite).
func TestOrderCancelledSchemaIsEmbedded(t *testing.T) {
	text, err := SchemaText("order-cancelled")
	if err != nil {
		t.Fatalf("embedded schemas/order-cancelled.avsc must exist: %v", err)
	}
	if _, err := avro.Parse(text); err != nil {
		t.Fatalf("embedded order-cancelled.avsc must parse: %v", err)
	}
}

// Encoder.OrderCancelled produces an Avro OrderCancelled with the aggregate's
// cancel reason; the payload round-trips through the embedded codec.
func TestEncoderOrderCancelledRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		reason order.CancelReason
	}{
		{order.CancelStockUnavailable},
		{order.CancelPaymentFailed},
	} {
		o := order.Order{
			ID:           "ord-1",
			Status:       order.StatusCancelled,
			CancelReason: tc.reason,
		}
		ev, err := NewEncoder().OrderCancelled(o)
		if err != nil {
			t.Fatalf("OrderCancelled(%s): %v", tc.reason, err)
		}
		if ev.RecordType != OrderCancelledType {
			t.Errorf("record type = %q, want %q", ev.RecordType, OrderCancelledType)
		}
		if ev.AggregateID != o.ID {
			t.Errorf("aggregate = %q, want %q", ev.AggregateID, o.ID)
		}
		c, err := codec("order-cancelled")
		if err != nil {
			t.Fatalf("codec: %v", err)
		}
		var got OrderCancelled
		if err := avro.Unmarshal(c, ev.Payload, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.Reason != string(tc.reason) {
			t.Errorf("reason = %q, want %q", got.Reason, tc.reason)
		}
		if got.OrderID != o.ID {
			t.Errorf("order id = %q, want %q", got.OrderID, o.ID)
		}
		if got.EventID == "" || got.OccurredAt == "" {
			t.Error("event id and occurred_at must be populated")
		}
	}
}

// Compensation consumers decode their new subscription facts through the
// embedded schemas (same mechanism as StockReserved/PaymentCaptured).
func TestDecodeStockUnavailable(t *testing.T) {
	text, err := SchemaText("stock-unavailable")
	if err != nil {
		t.Fatalf("embedded stock-unavailable.avsc must exist: %v", err)
	}
	schema, err := avro.Parse(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	payload, err := avro.Marshal(schema, StockUnavailable{
		EventID:    "evt-su-1",
		OrderID:    "ord-1",
		OccurredAt: "2026-10-23T00:00:00Z",
		Shortages:  []Shortage{{SKU: "SKU-A", Requested: 3, Available: 1}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := DecodeStockUnavailable(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EventID != "evt-su-1" || got.OrderID != "ord-1" {
		t.Errorf("ids = %q/%q, want evt-su-1/ord-1", got.EventID, got.OrderID)
	}
	if len(got.Shortages) != 1 || got.Shortages[0].SKU != "SKU-A" ||
		got.Shortages[0].Requested != 3 || got.Shortages[0].Available != 1 {
		t.Errorf("shortages = %+v, want [{SKU-A 3 1}]", got.Shortages)
	}
}

func TestDecodePaymentFailed(t *testing.T) {
	text, err := SchemaText("payment-failed")
	if err != nil {
		t.Fatalf("embedded payment-failed.avsc must exist: %v", err)
	}
	schema, err := avro.Parse(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	payload, err := avro.Marshal(schema, PaymentFailed{
		EventID:       "evt-pf-1",
		OrderID:       "ord-1",
		OccurredAt:    "2026-10-23T00:00:00Z",
		AmountCents:   29700,
		FailureReason: "CARD_DECLINED",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := DecodePaymentFailed(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EventID != "evt-pf-1" || got.OrderID != "ord-1" {
		t.Errorf("ids = %q/%q, want evt-pf-1/ord-1", got.EventID, got.OrderID)
	}
	if got.AmountCents != 29700 || got.FailureReason != "CARD_DECLINED" {
		t.Errorf("payment = %d/%q, want 29700/CARD_DECLINED", got.AmountCents, got.FailureReason)
	}
}
