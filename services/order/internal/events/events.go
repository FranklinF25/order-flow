// Package events is the Avro translation layer between the order domain and
// the wire: embedded .avsc contracts (copies; canonical source is schemas/ at
// the repo root), compiled codecs and the order.EventEncoder implementation.
package events

import (
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

//go:embed schemas/*.avsc
var schemasFS embed.FS

// Record types (subjects under RecordNameStrategy).
const (
	OrderPlacedType    = "orderflow.order.OrderPlaced"
	OrderConfirmedType = "orderflow.order.OrderConfirmed"
)

// OrderItem is the wire shape of an order line inside OrderPlaced.
type OrderItem struct {
	SKU            string `avro:"sku"`
	Qty            int    `avro:"qty"`
	UnitPriceCents int64  `avro:"unit_price_cents"`
}

// OrderPlaced is emitted when an order is accepted (status AWAITING_STOCK).
type OrderPlaced struct {
	EventID     string      `avro:"event_id"`
	OrderID     string      `avro:"order_id"`
	OccurredAt  string      `avro:"occurred_at"`
	CustomerID  string      `avro:"customer_id"`
	Items       []OrderItem `avro:"items"`
	TotalCents  int64       `avro:"total_cents"`
}

// OrderConfirmed is emitted when payment for an order is captured.
type OrderConfirmed struct {
	EventID    string `avro:"event_id"`
	OrderID    string `avro:"order_id"`
	OccurredAt string `avro:"occurred_at"`
}

// Consume-side records (topology §6 subscriptions).

const (
	StockReservedType   = "orderflow.inventory.StockReserved"
	PaymentCapturedType = "orderflow.payment.PaymentCaptured"
)

// StockItem is a reserved line on the inventory stream.
type StockItem struct {
	SKU string `avro:"sku"`
	Qty int    `avro:"qty"`
}

// StockReserved is consumed from inventory.events.
type StockReserved struct {
	EventID    string      `avro:"event_id"`
	OrderID    string      `avro:"order_id"`
	OccurredAt string      `avro:"occurred_at"`
	Items      []StockItem `avro:"items"`
	TotalCents int64       `avro:"total_cents"`
}

// PaymentCaptured is consumed from payment.events.
type PaymentCaptured struct {
	EventID     string `avro:"event_id"`
	OrderID     string `avro:"order_id"`
	OccurredAt  string `avro:"occurred_at"`
	PaymentID   string `avro:"payment_id"`
	AmountCents int64  `avro:"amount_cents"`
}

// DecodeStockReserved decodes a StockReserved Avro body (wire envelope already stripped).
func DecodeStockReserved(payload []byte) (StockReserved, error) {
	c, err := codec("stock-reserved")
	if err != nil {
		return StockReserved{}, err
	}
	var ev StockReserved
	if err := avro.Unmarshal(c, payload, &ev); err != nil {
		return StockReserved{}, fmt.Errorf("unmarshal StockReserved: %w", err)
	}
	return ev, nil
}

// DecodePaymentCaptured decodes a PaymentCaptured Avro body (wire envelope already stripped).
func DecodePaymentCaptured(payload []byte) (PaymentCaptured, error) {
	c, err := codec("payment-captured")
	if err != nil {
		return PaymentCaptured{}, err
	}
	var ev PaymentCaptured
	if err := avro.Unmarshal(c, payload, &ev); err != nil {
		return PaymentCaptured{}, fmt.Errorf("unmarshal PaymentCaptured: %w", err)
	}
	return ev, nil
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

// Encoder implements order.EventEncoder with Avro (driven adapter).
type Encoder struct{}

// NewEncoder builds the Avro encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// OrderPlaced encodes the fact of the same name.
func (Encoder) OrderPlaced(o order.Order) (order.OutboxEvent, error) {
	id, err := order.NewUUID()
	if err != nil {
		return order.OutboxEvent{}, err
	}
	c, err := codec("order-placed")
	if err != nil {
		return order.OutboxEvent{}, err
	}
	payload, err := avro.Marshal(c, OrderPlaced{
		EventID:    id,
		OrderID:    o.ID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		CustomerID: o.CustomerID,
		Items:      wireItems(o.Items),
		TotalCents: o.TotalCents,
	})
	if err != nil {
		return order.OutboxEvent{}, fmt.Errorf("marshal OrderPlaced: %w", err)
	}
	return order.OutboxEvent{
		EventID:     id,
		RecordType:  OrderPlacedType,
		AggregateID: o.ID,
		Payload:     payload,
	}, nil
}

// OrderConfirmed encodes the fact of the same name.
func (Encoder) OrderConfirmed(o order.Order) (order.OutboxEvent, error) {
	id, err := order.NewUUID()
	if err != nil {
		return order.OutboxEvent{}, err
	}
	c, err := codec("order-confirmed")
	if err != nil {
		return order.OutboxEvent{}, err
	}
	payload, err := avro.Marshal(c, OrderConfirmed{
		EventID:    id,
		OrderID:    o.ID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return order.OutboxEvent{}, fmt.Errorf("marshal OrderConfirmed: %w", err)
	}
	return order.OutboxEvent{
		EventID:     id,
		RecordType:  OrderConfirmedType,
		AggregateID: o.ID,
		Payload:     payload,
	}, nil
}

func wireItems(items []order.Item) []OrderItem {
	out := make([]OrderItem, len(items))
	for i, it := range items {
		out[i] = OrderItem{SKU: it.SKU, Qty: it.Qty, UnitPriceCents: it.UnitPriceCents}
	}
	return out
}
