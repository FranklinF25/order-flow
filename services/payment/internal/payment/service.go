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
	// Capture charges amountCents for orderID. customerID carries the M2
	// failure marker; a deterministic simulated rejection comes back as a
	// *DeclineError naming the FailureReason, an outage as an ordinary error.
	Capture(ctx context.Context, orderID string, amountCents int64, customerID string) (paymentID string, err error)
}

// EventEncoder turns payment facts into outbox events (driven port; the
// kafka adapter implements it with Avro).
type EventEncoder interface {
	PaymentCaptured(p Payment) (OutboxEvent, error)
	PaymentFailed(orderID string, amountCents int64, reason FailureReason) (OutboxEvent, error)
}

// CaptureInput carries the StockReserved fields payment reacts to (topology §5:
// the charge amount is StockReserved.total_cents; customer_id is the v3 echo
// that feeds the failure marker).
type CaptureInput struct {
	EventID     string
	OrderID     string
	AmountCents int64
	CustomerID  string
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

// Capture runs the simulated charge and persists the outcome in ONE
// transaction (topology §6/§7): the processed-event guard, the payments row
// and the fact — PaymentCaptured when the gateway captures, or a FAILED row
// plus PaymentFailed (failure_reason, amount_cents) when it declines on the
// customer_id marker. The gateway runs first because its verdict feeds the
// row and the fact; an outage error persists nothing. Redelivery never
// double-charges the ledger: the processed_events guard skips duplicate
// event ids and the order_id UNIQUE constraint makes a second write for the
// same order a no-op — on both the capture and the decline path.
//
// The fresh return reports whether the processed_events guard inserted this
// event id (same convention as the Store's inserted bools): true = the event
// was processed, false = a duplicate-marker replay was absorbed. The consumer
// turns it into the pinned outcome label of orderflow_events_total
// (processed | absorbed, M3 metric contract). Errors persist nothing and are
// not a commit decision, so fresh is meaningless alongside a non-nil error.
func (s *Service) Capture(ctx context.Context, in CaptureInput) (fresh bool, err error) {
	if in.EventID == "" {
		return false, errors.New("event id is required")
	}
	if in.OrderID == "" {
		return false, errors.New("order id is required")
	}
	paymentID, gwErr := s.gw.Capture(ctx, in.OrderID, in.AmountCents, in.CustomerID)
	var declined *DeclineError
	if errors.As(gwErr, &declined) {
		return s.fail(ctx, in, declined.Reason)
	}
	if gwErr != nil {
		return false, fmt.Errorf("gateway capture: %w", gwErr)
	}
	p, err := NewPayment(paymentID, in.OrderID, in.AmountCents)
	if err != nil {
		return false, err
	}
	ev, err := s.enc.PaymentCaptured(p)
	if err != nil {
		return false, err
	}
	fresh = false // a later error leaves fresh meaningless (see doc above)
	return fresh, s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.AppendProcessedEvent(ctx, in.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil // duplicate StockReserved event: idempotent skip, nothing written
		}
		fresh = true // the guard inserted this event id: a fresh, processed event
		saved, err := st.SavePayment(ctx, &p)
		if err != nil {
			return err
		}
		if !saved {
			return nil // second capture for the same order: unique order_id no-op
		}
		return st.AppendOutbox(ctx, ev)
	})
}

// fail persists a marker decline in ONE transaction: the processed-event
// guard, the FAILED payments row (amount from StockReserved.total_cents,
// order_id UNIQUE as the idempotency real guard) and the PaymentFailed fact
// that triggers compensation B (order cancel + inventory release). It mirrors
// the capture path's guards so a replayed StockReserved absorbs as a no-op.
func (s *Service) fail(ctx context.Context, in CaptureInput, reason FailureReason) (bool, error) {
	id, err := NewUUID()
	if err != nil {
		return false, err
	}
	p, err := NewFailedPayment(id, in.OrderID, in.AmountCents)
	if err != nil {
		return false, err
	}
	ev, err := s.enc.PaymentFailed(in.OrderID, in.AmountCents, reason)
	if err != nil {
		return false, err
	}
	fresh := false // a later error leaves fresh meaningless (see Capture doc above)
	return fresh, s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.AppendProcessedEvent(ctx, in.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil // duplicate StockReserved event: idempotent skip, nothing written
		}
		fresh = true // the guard inserted this event id: a fresh, processed event
		saved, err := st.SavePayment(ctx, &p)
		if err != nil {
			return err
		}
		if !saved {
			return nil // a payment for this order already exists: unique order_id no-op
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
