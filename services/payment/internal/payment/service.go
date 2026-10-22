package payment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// OutboxEvent is an unpublished domain fact persisted in the same transaction
// as the state change (transactional outbox, topology §7).
type OutboxEvent struct {
	EventID     string
	RecordType  string // full Avro record name, e.g. orderflow.payment.PaymentCaptured
	AggregateID string // kafka message key
	Payload     []byte // Avro-encoded record body
}

// Store is the persistence port (consumer-side; implemented by the postgres adapter).
// SavePayment and AppendProcessedEvent report whether the row was inserted:
// false means an idempotency constraint absorbed the write (no error).
type Store interface {
	SavePayment(ctx context.Context, p *Payment) (inserted bool, err error)
	AppendProcessedEvent(ctx context.Context, eventID string) (inserted bool, err error)
	AppendOutbox(ctx context.Context, e OutboxEvent) error
}

// Tx runs a unit of work atomically: the Store handed to fn is scoped to one
// database transaction — state change and outbox append commit or roll back together.
type Tx interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// Gateway is the payment gateway port (driven; injectable like order's EventEncoder).
type Gateway interface {
	// Capture charges amountCents for orderID and returns the gateway payment id.
	Capture(ctx context.Context, orderID string, amountCents int64) (paymentID string, err error)
}

// EventEncoder turns captured payments into outbox events (driven port; the
// kafka adapter implements it with Avro).
type EventEncoder interface {
	PaymentCaptured(p Payment) (OutboxEvent, error)
}

// CaptureInput carries the StockReserved fields payment reacts to (topology §5:
// the charge amount is StockReserved.total_cents).
type CaptureInput struct {
	EventID     string
	OrderID     string
	AmountCents int64
}

// Service is the payment application service.
type Service struct {
	tx  Tx
	gw  Gateway
	enc EventEncoder
}

// NewService wires the application service.
func NewService(tx Tx, gw Gateway, enc EventEncoder) *Service {
	return &Service{tx: tx, gw: gw, enc: enc}
}

// Capture charges the gateway and persists the CAPTURED payment, the
// processed-event guard and the PaymentCaptured fact in ONE transaction
// (topology §6/§7). The gateway runs first because its payment id feeds the
// row and the fact; a failure there persists nothing. Redelivery never
// double-charges the ledger: the processed_events guard skips duplicate
// event ids and the order_id UNIQUE constraint makes a second capture a no-op.
func (s *Service) Capture(ctx context.Context, in CaptureInput) error {
	if in.EventID == "" {
		return errors.New("event id is required")
	}
	if in.OrderID == "" {
		return errors.New("order id is required")
	}
	paymentID, err := s.gw.Capture(ctx, in.OrderID, in.AmountCents)
	if err != nil {
		return fmt.Errorf("gateway capture: %w", err)
	}
	p, err := NewPayment(paymentID, in.OrderID, in.AmountCents)
	if err != nil {
		return err
	}
	ev, err := s.enc.PaymentCaptured(p)
	if err != nil {
		return err
	}
	return s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		fresh, err := st.AppendProcessedEvent(ctx, in.EventID)
		if err != nil {
			return err
		}
		if !fresh {
			return nil // duplicate StockReserved event: idempotent skip, nothing written
		}
		inserted, err := st.SavePayment(ctx, &p)
		if err != nil {
			return err
		}
		if !inserted {
			return nil // second capture for the same order: unique order_id no-op
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
