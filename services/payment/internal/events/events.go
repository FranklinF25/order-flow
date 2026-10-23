// Package events is the Avro translation layer between the payment domain and
// the wire: embedded .avsc contracts (copies; canonical source is schemas/ at
// the repo root), compiled codecs, the payment.EventEncoder implementation and
// the StockReserved decoder for the consume side.
package events

import (
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

//go:embed schemas/*.avsc
var schemasFS embed.FS

// Record types (subjects under RecordNameStrategy).
const (
	StockReservedType   = "orderflow.inventory.StockReserved" // consumed from inventory.events
	PaymentCapturedType = "orderflow.payment.PaymentCaptured" // produced to payment.events
	PaymentFailedType   = "orderflow.payment.PaymentFailed"   // produced to payment.events (compensation B)
)

// StockItem is the wire shape of a reserved line inside StockReserved.
type StockItem struct {
	SKU string `avro:"sku"`
	Qty int    `avro:"qty"`
}

// StockReserved (v3) is consumed from inventory.events; payment charges
// TotalCents (the v2 additive echo of OrderPlaced.total_cents, topology §5)
// and reads CustomerID (the v3 additive echo of OrderPlaced.customer_id) as
// the simulated gateway's failure marker (M2, S3).
type StockReserved struct {
	EventID    string      `avro:"event_id"`
	OrderID    string      `avro:"order_id"`
	OccurredAt string      `avro:"occurred_at"`
	Items      []StockItem `avro:"items"`
	TotalCents int64       `avro:"total_cents"`
	CustomerID string      `avro:"customer_id"`
}

// PaymentCaptured is emitted when the simulated capture succeeds.
type PaymentCaptured struct {
	EventID     string `avro:"event_id"`
	OrderID     string `avro:"order_id"`
	OccurredAt  string `avro:"occurred_at"`
	PaymentID   string `avro:"payment_id"`
	AmountCents int64  `avro:"amount_cents"`
}

// PaymentFailed is emitted when the marker-driven simulated gateway rejects
// the charge (compensation B trigger, topology §6).
type PaymentFailed struct {
	EventID       string `avro:"event_id"`
	OrderID       string `avro:"order_id"`
	OccurredAt    string `avro:"occurred_at"`
	AmountCents   int64  `avro:"amount_cents"`
	FailureReason string `avro:"failure_reason"`
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

// Encoder implements payment.EventEncoder with Avro (driven adapter).
type Encoder struct{}

// NewEncoder builds the Avro encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// PaymentCaptured encodes the fact of the same name.
func (Encoder) PaymentCaptured(p payment.Payment) (payment.OutboxEvent, error) {
	id, err := payment.NewUUID()
	if err != nil {
		return payment.OutboxEvent{}, err
	}
	c, err := codec("payment-captured")
	if err != nil {
		return payment.OutboxEvent{}, err
	}
	payload, err := avro.Marshal(c, PaymentCaptured{
		EventID:     id,
		OrderID:     p.OrderID,
		OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		PaymentID:   p.ID,
		AmountCents: p.AmountCents,
	})
	if err != nil {
		return payment.OutboxEvent{}, fmt.Errorf("marshal PaymentCaptured: %w", err)
	}
	return payment.OutboxEvent{
		EventID:     id,
		RecordType:  PaymentCapturedType,
		AggregateID: p.OrderID,
		Payload:     payload,
	}, nil
}

// PaymentFailed encodes the fact of the same name (compensation B trigger).
func (Encoder) PaymentFailed(orderID string, amountCents int64, reason payment.FailureReason) (payment.OutboxEvent, error) {
	id, err := payment.NewUUID()
	if err != nil {
		return payment.OutboxEvent{}, err
	}
	c, err := codec("payment-failed")
	if err != nil {
		return payment.OutboxEvent{}, err
	}
	payload, err := avro.Marshal(c, PaymentFailed{
		EventID:       id,
		OrderID:       orderID,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
		AmountCents:   amountCents,
		FailureReason: string(reason),
	})
	if err != nil {
		return payment.OutboxEvent{}, fmt.Errorf("marshal PaymentFailed: %w", err)
	}
	return payment.OutboxEvent{
		EventID:     id,
		RecordType:  PaymentFailedType,
		AggregateID: orderID,
		Payload:     payload,
	}, nil
}

// Decoder translates incoming wire records into domain inputs (driven adapter).
//
// Consume choice: strip the 5-byte Confluent header (magic byte + int32
// big-endian schema id) and decode the rest with the EMBEDDED StockReserved v3
// schema. The registry subject holds versions [1,2,3] under BACKWARD_TRANSITIVE
// (L5/L3 additive precedent), so every wire version decodes against this reader
// — customer_id defaults to "" for v1/v2 records; resolving arbitrary runtime
// schema ids arrives with schema CI (M4).
type Decoder struct{}

// NewDecoder builds the Avro decoder.
func NewDecoder() *Decoder { return &Decoder{} }

// DecodeStockReserved decodes a wire-format inventory.events value.
func (Decoder) DecodeStockReserved(value []byte) (StockReserved, error) {
	if len(value) < 5 || value[0] != 0x00 {
		return StockReserved{}, fmt.Errorf("invalid confluent wire format")
	}
	c, err := codec("stock-reserved")
	if err != nil {
		return StockReserved{}, err
	}
	var evt StockReserved
	if err := avro.Unmarshal(c, value[5:], &evt); err != nil {
		return StockReserved{}, fmt.Errorf("unmarshal StockReserved: %w", err)
	}
	return evt, nil
}
