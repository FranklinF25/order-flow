package inventory

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
	RecordType  string // full Avro record name, e.g. orderflow.inventory.StockReserved
	AggregateID string // kafka message key (order_id)
	Payload     []byte // Avro-encoded record body
}

var (
	// ErrDuplicateEvent is returned by Store.MarkProcessed for a known event_id.
	ErrDuplicateEvent = errors.New("event already processed")
	// ErrStockNotFound is returned by Store.GetStock for unknown SKUs
	// (treated as zero availability, i.e. a shortage).
	ErrStockNotFound = errors.New("stock not found")
	// ErrReservationNotFound is returned by Store.GetReservation for an order
	// without a reservation (nothing to release: absorb the compensation).
	ErrReservationNotFound = errors.New("reservation not found")
)

// Store is the persistence port (consumer-side; implemented by the postgres adapter).
type Store interface {
	MarkProcessed(ctx context.Context, eventID string) error
	GetStock(ctx context.Context, sku string) (*Stock, error)
	ListStock(ctx context.Context) ([]Stock, error)
	DecrementStock(ctx context.Context, sku string, qty int) error
	SaveReservation(ctx context.Context, r Reservation) error
	AppendOutbox(ctx context.Context, e OutboxEvent) error
	GetReservation(ctx context.Context, orderID string) (*Reservation, error)
	// ReleaseReservation flips the reservation RESERVED→RELEASED and reports
	// whether THIS transaction took the transition (SQL WHERE status =
	// 'RESERVED' is the concurrency backstop on the state machine).
	ReleaseReservation(ctx context.Context, orderID string) (bool, error)
	IncrementStock(ctx context.Context, sku string, qty int) error
}

// Tx runs a unit of work atomically: the Store handed to fn is scoped to one
// database transaction — the idempotency guard, the state change and the
// outbox append commit or roll back together.
type Tx interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// EventEncoder turns aggregate facts into outbox events (driven port; the kafka
// adapter implements it with Avro).
type EventEncoder interface {
	StockReserved(r Reservation) (OutboxEvent, error)
	StockUnavailable(orderID string, shortages []Shortage) (OutboxEvent, error)
	StockReleased(orderID string, items []Item) (OutboxEvent, error)
}

// ReserveInput is the driving command translated from the OrderPlaced fact.
type ReserveInput struct {
	EventID    string
	OrderID    string
	CustomerID string
	TotalCents int64
	Items      []Item
}

// ReleaseInput is the driving command translated from the PaymentFailed fact
// (compensation B). The payment payload carries no lines — the stored
// reservation is the source of truth for what to restore.
type ReleaseInput struct {
	EventID string
	OrderID string
}

// Service is the inventory application service.
type Service struct {
	tx    Tx
	reads Store
	enc   EventEncoder
}

// NewService wires the application service.
func NewService(tx Tx, reads Store, enc EventEncoder) *Service {
	return &Service{tx: tx, reads: reads, enc: enc}
}

// HandleOrderPlaced reserves every line of an OrderPlaced fact in one
// transaction: the processed_events guard runs first (a known event_id skips
// silently — replay after crash is a no-op), then availability is checked for
// ALL lines. Any shortage emits StockUnavailable and decrements nothing;
// otherwise every line is decremented, the reservation persisted and
// StockReserved (with the echoed total_cents) queued. applied reports whether
// the event was applied; a duplicate returns (false, nil).
func (s *Service) HandleOrderPlaced(ctx context.Context, in ReserveInput) (bool, error) {
	r, err := NewReservation(in.OrderID, in.CustomerID, in.TotalCents, in.Items)
	if err != nil {
		return false, err
	}
	var applied bool
	err = s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		if err := st.MarkProcessed(ctx, in.EventID); err != nil {
			if errors.Is(err, ErrDuplicateEvent) {
				return nil // replayed event: skip silently, commit the no-op tx
			}
			return err
		}
		// Check every line before touching any stock (atomic shortfall).
		available := make(map[string]*Stock, len(r.Items))
		var shortages []Shortage
		for _, it := range r.Items {
			sk, err := st.GetStock(ctx, it.SKU)
			if errors.Is(err, ErrStockNotFound) {
				shortages = append(shortages, Shortage{SKU: it.SKU, Requested: it.Qty, Available: 0})
				continue
			}
			if err != nil {
				return err
			}
			if err := sk.Reserve(it.Qty); err != nil {
				shortages = append(shortages, Shortage{SKU: it.SKU, Requested: it.Qty, Available: sk.Available})
				continue
			}
			available[it.SKU] = sk
		}
		if len(shortages) > 0 {
			ev, err := s.enc.StockUnavailable(r.OrderID, shortages)
			if err != nil {
				return err
			}
			applied = true // the event itself is applied: it produced a fact
			return st.AppendOutbox(ctx, ev)
		}
		for _, it := range r.Items {
			if _, ok := available[it.SKU]; ok {
				if err := st.DecrementStock(ctx, it.SKU, it.Qty); err != nil {
					return err
				}
			}
		}
		if err := st.SaveReservation(ctx, r); err != nil {
			return err
		}
		ev, err := s.enc.StockReserved(r)
		if err != nil {
			return err
		}
		applied = true
		return st.AppendOutbox(ctx, ev)
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// ListStock exposes the read side for the REST adapter.
func (s *Service) ListStock(ctx context.Context) ([]Stock, error) {
	return s.reads.ListStock(ctx)
}

// HandlePaymentFailed releases an order's reservation as compensation B, in
// one transaction: the processed_events guard runs first (a replayed
// PaymentFailed is an absorbed no-op), then the reservation state machine
// RESERVED→RELEASED is taken exactly once — the conditional
// ReleaseReservation is the concurrency backstop. Stock quantities are
// restored per stored reservation line and StockReleased is queued ONLY when
// this transaction took the transition. Absorbed cases (duplicate event,
// unknown order, already RELEASED) return (false, nil): the caller commits
// the offset without error and without side effects.
func (s *Service) HandlePaymentFailed(ctx context.Context, in ReleaseInput) (bool, error) {
	var applied bool
	err := s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		if err := st.MarkProcessed(ctx, in.EventID); err != nil {
			if errors.Is(err, ErrDuplicateEvent) {
				return nil // replayed PaymentFailed: absorb, commit the no-op tx
			}
			return err
		}
		r, err := st.GetReservation(ctx, in.OrderID)
		if errors.Is(err, ErrReservationNotFound) {
			return nil // nothing to release (never reserved, or shortage path): absorb
		}
		if err != nil {
			return err
		}
		if err := r.Release(); err != nil {
			return nil // already RELEASED: absorb — no restore, no re-emit
		}
		transitioned, err := st.ReleaseReservation(ctx, in.OrderID)
		if err != nil {
			return err
		}
		if !transitioned {
			return nil // a concurrent tx already took RESERVED→RELEASED: absorb
		}
		for _, it := range r.Items {
			if err := st.IncrementStock(ctx, it.SKU, it.Qty); err != nil {
				return err
			}
		}
		ev, err := s.enc.StockReleased(r.OrderID, r.Items)
		if err != nil {
			return err
		}
		applied = true
		return st.AppendOutbox(ctx, ev)
	})
	if err != nil {
		return false, err
	}
	return applied, nil
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
