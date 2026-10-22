// Package events is the Avro translation layer between the inventory domain
// and the wire: embedded .avsc contracts (copies; canonical source is schemas/
// at the repo root), compiled schemas, the OrderPlaced decode side and the
// inventory.EventEncoder implementation.
package events

import (
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

//go:embed schemas/*.avsc
var schemasFS embed.FS

// Record types (subjects under RecordNameStrategy).
const (
	OrderPlacedType      = "orderflow.order.OrderPlaced"
	StockReservedType    = "orderflow.inventory.StockReserved"
	StockUnavailableType = "orderflow.inventory.StockUnavailable"
)

// OrderItem is the wire shape of an order line inside OrderPlaced.
type OrderItem struct {
	SKU            string `avro:"sku"`
	Qty            int    `avro:"qty"`
	UnitPriceCents int64  `avro:"unit_price_cents"`
}

// OrderPlaced is the inventory-side view of the order service's fact. The
// inventory service only consumes this one record type on order.events.
type OrderPlaced struct {
	EventID    string      `avro:"event_id"`
	OrderID    string      `avro:"order_id"`
	OccurredAt string      `avro:"occurred_at"`
	CustomerID string      `avro:"customer_id"`
	Items      []OrderItem `avro:"items"`
	TotalCents int64       `avro:"total_cents"`
}

// StockItem is the wire shape of a reserved line.
type StockItem struct {
	SKU string `avro:"sku"`
	Qty int    `avro:"qty"`
}

// StockReserved is emitted when a reservation for an order commits.
// total_cents is the v2 field (echo of OrderPlaced.total_cents) that the
// payment service charges from.
type StockReserved struct {
	EventID    string      `avro:"event_id"`
	OrderID    string      `avro:"order_id"`
	OccurredAt string      `avro:"occurred_at"`
	Items      []StockItem `avro:"items"`
	TotalCents int64       `avro:"total_cents"`
}

// Shortage is the wire shape of an unfulfillable line.
type Shortage struct {
	SKU       string `avro:"sku"`
	Requested int    `avro:"requested"`
	Available int    `avro:"available"`
}

// StockUnavailable is emitted when a reservation cannot be satisfied; no
// stock was decremented for the order.
type StockUnavailable struct {
	EventID    string     `avro:"event_id"`
	OrderID    string     `avro:"order_id"`
	OccurredAt string     `avro:"occurred_at"`
	Shortages  []Shortage `avro:"shortages"`
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

// DecodeOrderPlaced decodes an OrderPlaced Avro body (already stripped of the
// Confluent wire envelope) against the embedded schema.
func DecodeOrderPlaced(payload []byte) (OrderPlaced, error) {
	c, err := codec("order-placed")
	if err != nil {
		return OrderPlaced{}, err
	}
	var ev OrderPlaced
	if err := avro.Unmarshal(c, payload, &ev); err != nil {
		return OrderPlaced{}, fmt.Errorf("unmarshal OrderPlaced: %w", err)
	}
	return ev, nil
}

// Encoder implements inventory.EventEncoder with Avro (driven adapter).
type Encoder struct{}

// NewEncoder builds the Avro encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// StockReserved encodes the fact of the same name.
func (Encoder) StockReserved(r inventory.Reservation) (inventory.OutboxEvent, error) {
	id, err := inventory.NewUUID()
	if err != nil {
		return inventory.OutboxEvent{}, err
	}
	c, err := codec("stock-reserved")
	if err != nil {
		return inventory.OutboxEvent{}, err
	}
	items := make([]StockItem, len(r.Items))
	for i, it := range r.Items {
		items[i] = StockItem{SKU: it.SKU, Qty: it.Qty}
	}
	payload, err := avro.Marshal(c, StockReserved{
		EventID:    id,
		OrderID:    r.OrderID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Items:      items,
		TotalCents: r.TotalCents,
	})
	if err != nil {
		return inventory.OutboxEvent{}, fmt.Errorf("marshal StockReserved: %w", err)
	}
	return inventory.OutboxEvent{
		EventID:     id,
		RecordType:  StockReservedType,
		AggregateID: r.OrderID,
		Payload:     payload,
	}, nil
}

// StockUnavailable encodes the fact of the same name.
func (Encoder) StockUnavailable(orderID string, shortages []inventory.Shortage) (inventory.OutboxEvent, error) {
	id, err := inventory.NewUUID()
	if err != nil {
		return inventory.OutboxEvent{}, err
	}
	c, err := codec("stock-unavailable")
	if err != nil {
		return inventory.OutboxEvent{}, err
	}
	wire := make([]Shortage, len(shortages))
	for i, s := range shortages {
		wire[i] = Shortage{SKU: s.SKU, Requested: s.Requested, Available: s.Available}
	}
	payload, err := avro.Marshal(c, StockUnavailable{
		EventID:    id,
		OrderID:    orderID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Shortages:  wire,
	})
	if err != nil {
		return inventory.OutboxEvent{}, fmt.Errorf("marshal StockUnavailable: %w", err)
	}
	return inventory.OutboxEvent{
		EventID:     id,
		RecordType:  StockUnavailableType,
		AggregateID: orderID,
		Payload:     payload,
	}, nil
}
