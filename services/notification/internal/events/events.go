// Package events is the Avro translation layer between the notification
// domain and the wire: embedded .avsc contracts (copies; canonical source is
// schemas/ at the repo root), compiled codecs, the notification.EventEncoder
// implementation and the order.events decoders for the consume side.
package events

import (
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

//go:embed schemas/*.avsc
var schemasFS embed.FS

// Record types (subjects under RecordNameStrategy).
const (
	NotificationSentType = "orderflow.notification.NotificationSent" // produced to notification.events
	OrderConfirmedType   = "orderflow.order.OrderConfirmed"          // consumed from order.events
	OrderCancelledType   = "orderflow.order.OrderCancelled"          // consumed from order.events
)

// NotificationSent is the audit fact emitted when a message is handed to the
// RabbitMQ worker (topology §5).
type NotificationSent struct {
	EventID    string `avro:"event_id"`
	OrderID    string `avro:"order_id"`
	OccurredAt string `avro:"occurred_at"`
	Kind       string `avro:"kind"`
	Channel    string `avro:"channel"`
}

// OrderConfirmed is consumed from order.events.
type OrderConfirmed struct {
	EventID    string `avro:"event_id"`
	OrderID    string `avro:"order_id"`
	OccurredAt string `avro:"occurred_at"`
}

// OrderCancelled is consumed from order.events.
type OrderCancelled struct {
	EventID    string `avro:"event_id"`
	OrderID    string `avro:"order_id"`
	OccurredAt string `avro:"occurred_at"`
	Reason     string `avro:"reason"`
}

var (
	codecMu sync.Mutex
	codecs  = map[string]avro.Schema{}
)

func codec(name string) (avro.Schema, error) {
	codecMu.Lock()
	defer codecMu.Unlock()
	if c, ok := codecs[name]; ok {
		return c, nil
	}
	raw, err := schemasFS.ReadFile("schemas/" + name + ".avsc")
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	c, err := avro.Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
	}
	codecs[name] = c
	return c, nil
}

// SchemaText returns the raw .avsc content for Schema Registry registration.
func SchemaText(name string) (string, error) {
	raw, err := schemasFS.ReadFile("schemas/" + name + ".avsc")
	return string(raw), err
}

// Encoder implements notification.EventEncoder with Avro (driven adapter).
type Encoder struct{}

// NewEncoder builds the Avro encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// NotificationSent encodes the fact of the same name; the fact mints its own
// event id (the consumed order.events id is guarded by processed_events).
func (Encoder) NotificationSent(n notification.Notification) (notification.OutboxEvent, error) {
	id, err := notification.NewUUID()
	if err != nil {
		return notification.OutboxEvent{}, err
	}
	c, err := codec("notification-sent")
	if err != nil {
		return notification.OutboxEvent{}, err
	}
	payload, err := avro.Marshal(c, NotificationSent{
		EventID:    id,
		OrderID:    n.OrderID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Kind:       string(n.Kind),
		Channel:    string(n.Channel),
	})
	if err != nil {
		return notification.OutboxEvent{}, fmt.Errorf("marshal NotificationSent: %w", err)
	}
	return notification.OutboxEvent{
		EventID:     id,
		RecordType:  NotificationSentType,
		AggregateID: n.OrderID,
		Payload:     payload,
	}, nil
}

// Decoder translates incoming wire records into domain inputs (driven adapter).
//
// Consume choice (sibling precedent): strip the 5-byte Confluent header
// (magic byte + int32 big-endian schema id) and decode the rest with the
// EMBEDDED schema. The registry subject holds exactly this schema (idempotent
// CreateSchema at boot), so the id carries no information we need to decode;
// resolving arbitrary runtime schema ids arrives with schema CI (M4).
type Decoder struct{}

// NewDecoder builds the Avro decoder.
func NewDecoder() *Decoder { return &Decoder{} }

// DecodeOrderConfirmed decodes a wire-format order.events value.
func (Decoder) DecodeOrderConfirmed(value []byte) (OrderConfirmed, error) {
	var ev OrderConfirmed
	if err := decodeWire(value, "order-confirmed", &ev); err != nil {
		return OrderConfirmed{}, err
	}
	return ev, nil
}

// DecodeOrderCancelled decodes a wire-format order.events value.
func (Decoder) DecodeOrderCancelled(value []byte) (OrderCancelled, error) {
	var ev OrderCancelled
	if err := decodeWire(value, "order-cancelled", &ev); err != nil {
		return OrderCancelled{}, err
	}
	return ev, nil
}

func decodeWire(value []byte, name string, v any) error {
	if len(value) < 5 || value[0] != 0x00 {
		return fmt.Errorf("invalid confluent wire format")
	}
	c, err := codec(name)
	if err != nil {
		return err
	}
	if err := avro.Unmarshal(c, value[5:], v); err != nil {
		return fmt.Errorf("unmarshal %s: %w", name, err)
	}
	return nil
}
