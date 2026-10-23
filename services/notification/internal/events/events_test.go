package events

import (
	"encoding/binary"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

func TestNotificationSentEncodesForTheWire(t *testing.T) {
	n := notification.Notification{ID: "ntf-1", OrderID: "ord-1", Kind: notification.KindOrderConfirmed, Channel: notification.ChannelEmail}
	ev, err := Encoder{}.NotificationSent(n)
	if err != nil {
		t.Fatalf("NotificationSent: %v", err)
	}
	if ev.RecordType != NotificationSentType {
		t.Errorf("record type = %q, want %q", ev.RecordType, NotificationSentType)
	}
	if ev.AggregateID != "ord-1" {
		t.Errorf("aggregate = %q, want order id", ev.AggregateID)
	}
	if ev.EventID == "" {
		t.Error("event id must be generated")
	}

	c, err := codec("notification-sent")
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	var wire NotificationSent
	if err := avro.Unmarshal(c, ev.Payload, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wire.OrderID != "ord-1" || wire.Kind != "ORDER_CONFIRMED" || wire.Channel != "EMAIL" {
		t.Errorf("wire = %+v, want ord-1/ORDER_CONFIRMED/EMAIL", wire)
	}
	if wire.EventID != ev.EventID || wire.OccurredAt == "" {
		t.Errorf("wire ids = %q/%q, event id and occurred_at must be set", wire.EventID, wire.OccurredAt)
	}
}

func TestDecodeOrderEventsWireFormat(t *testing.T) {
	confirmedBody, err := avro.Marshal(mustCodec(t, "order-confirmed"), OrderConfirmed{
		EventID: "evt-oc-1", OrderID: "ord-1", OccurredAt: "2026-10-23T13:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal OrderConfirmed: %v", err)
	}
	cancelledBody, err := avro.Marshal(mustCodec(t, "order-cancelled"), OrderCancelled{
		EventID: "evt-oc-2", OrderID: "ord-2", OccurredAt: "2026-10-23T13:01:00Z",
		Reason: "PAYMENT_FAILED",
	})
	if err != nil {
		t.Fatalf("marshal OrderCancelled: %v", err)
	}

	confirmed := wireValue(t, confirmedBody)
	ev, err := Decoder{}.DecodeOrderConfirmed(confirmed)
	if err != nil {
		t.Fatalf("DecodeOrderConfirmed: %v", err)
	}
	if ev.EventID != "evt-oc-1" || ev.OrderID != "ord-1" {
		t.Errorf("confirmed event = %+v, want evt-oc-1/ord-1", ev)
	}

	cancelled := wireValue(t, cancelledBody)
	cev, err := Decoder{}.DecodeOrderCancelled(cancelled)
	if err != nil {
		t.Fatalf("DecodeOrderCancelled: %v", err)
	}
	if cev.EventID != "evt-oc-2" || cev.OrderID != "ord-2" || cev.Reason != "PAYMENT_FAILED" {
		t.Errorf("cancelled event = %+v, want evt-oc-2/ord-2/PAYMENT_FAILED", cev)
	}
}

func TestDecodeRejectsBadWireFormat(t *testing.T) {
	if _, err := (Decoder{}).DecodeOrderConfirmed([]byte{0x01, 0x02}); err == nil {
		t.Error("expected error for bad magic byte")
	}
	if _, err := (Decoder{}).DecodeOrderConfirmed([]byte{0x00}); err == nil {
		t.Error("expected error for truncated wire payload")
	}
}

func mustCodec(t *testing.T, name string) avro.Schema {
	t.Helper()
	c, err := codec(name)
	if err != nil {
		t.Fatalf("codec %s: %v", name, err)
	}
	return c
}

func wireValue(t *testing.T, body []byte) []byte {
	t.Helper()
	value := make([]byte, 5+len(body))
	value[0] = 0x00
	binary.BigEndian.PutUint32(value[1:5], 42)
	copy(value[5:], body)
	return value
}
