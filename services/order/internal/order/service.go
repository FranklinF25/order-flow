package order

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// OutboxEvent is an unpublished domain fact persisted in the same transaction
// as the state change (transactional outbox, topology §7).
type OutboxEvent struct {
	EventID     string
	RecordType  string // full Avro record name, e.g. orderflow.order.OrderPlaced
	AggregateID string // kafka message key
	Payload     []byte // Avro-encoded record body
}

// ErrOrderNotFound is returned by Store.GetOrder for unknown ids.
var ErrOrderNotFound = errors.New("order not found")

// Store is the persistence port (consumer-side; implemented by the postgres adapter).
type Store interface {
	SaveOrder(ctx context.Context, o *Order) error
	GetOrder(ctx context.Context, id string) (*Order, error)
	UpdateOrder(ctx context.Context, o *Order) error
	AppendOutbox(ctx context.Context, e OutboxEvent) error
	// InsertProcessedEvent is the consume-side idempotency guard: false means
	// the event was already processed (replay → silent no-op).
	InsertProcessedEvent(ctx context.Context, eventID string) (bool, error)
}

// Tx runs a unit of work atomically: the Store handed to fn is scoped to one
// database transaction — state change and outbox append commit or roll back together.
type Tx interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// EventEncoder turns aggregate facts into outbox events (driven port; the kafka
// adapter implements it with Avro).
type EventEncoder interface {
	OrderPlaced(o Order) (OutboxEvent, error)
	OrderConfirmed(o Order) (OutboxEvent, error)
}

// PlaceOrderInput is the driving command for the REST adapter.
type PlaceOrderInput struct {
	CustomerID string
	Items      []Item
}

// Service is the order application service.
type Service struct {
	tx    Tx
	reads Store
	enc   EventEncoder
}

// NewService wires the application service.
func NewService(tx Tx, reads Store, enc EventEncoder) *Service {
	return &Service{tx: tx, reads: reads, enc: enc}
}

// PlaceOrder validates and persists a new order plus its OrderPlaced fact in
// one transaction, returning the persisted aggregate.
func (s *Service) PlaceOrder(ctx context.Context, in PlaceOrderInput) (*Order, error) {
	id, err := NewUUID()
	if err != nil {
		return nil, err
	}
	o, err := NewOrder(id, in.CustomerID, in.Items)
	if err != nil {
		return nil, err
	}
	ev, err := s.enc.OrderPlaced(o)
	if err != nil {
		return nil, err
	}
	err = s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		if err := st.SaveOrder(ctx, &o); err != nil {
			return err
		}
		return st.AppendOutbox(ctx, ev)
	})
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// GetOrder exposes the read side for the REST adapter.
func (s *Service) GetOrder(ctx context.Context, id string) (*Order, error) {
	return s.reads.GetOrder(ctx, id)
}

// OnStockReserved applies the inventory.events StockReserved fact:
// AWAITING_STOCK → AWAITING_PAYMENT. Duplicate event ids are silent no-ops
// (processed_events guard inside the same transaction).
func (s *Service) OnStockReserved(ctx context.Context, eventID, orderID string) error {
	return s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.InsertProcessedEvent(ctx, eventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		o, err := st.GetOrder(ctx, orderID)
		if err != nil {
			return err
		}
		if err := o.OnStockReserved(); err != nil {
			return err
		}
		return st.UpdateOrder(ctx, o)
	})
}

// OnPaymentCaptured applies the payment.events PaymentCaptured fact:
// AWAITING_PAYMENT → CONFIRMED, appending OrderConfirmed to the outbox in the
// same transaction. Duplicate event ids are silent no-ops.
func (s *Service) OnPaymentCaptured(ctx context.Context, eventID, orderID string) error {
	return s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.InsertProcessedEvent(ctx, eventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		o, err := st.GetOrder(ctx, orderID)
		if err != nil {
			return err
		}
		if err := o.OnPaymentCaptured(); err != nil {
			return err
		}
		if err := st.UpdateOrder(ctx, o); err != nil {
			return err
		}
		ev, err := s.enc.OrderConfirmed(*o)
		if err != nil {
			return err
		}
		return st.AppendOutbox(ctx, ev)
	})
}

// NewUUID returns a random RFC 4122 v4 UUID string (stdlib only).
func NewUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	u := hex.EncodeToString(b[:])
	return u[0:8] + "-" + u[8:12] + "-" + u[12:16] + "-" + u[16:20] + "-" + u[20:32], nil
}
