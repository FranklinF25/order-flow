package notification

import (
	"context"
	"errors"
	"log"
)

// OutboxEvent is an unpublished domain fact persisted in the same transaction
// as the audit row (transactional outbox, topology §7).
type OutboxEvent struct {
	EventID     string
	RecordType  string // full Avro record name, e.g. orderflow.notification.NotificationSent
	AggregateID string // kafka message key
	Payload     []byte // Avro-encoded record body
}

// Store is the persistence port (consumer-side; implemented by the postgres adapter).
type Store interface {
	SaveNotification(ctx context.Context, n *Notification) error
	AppendOutbox(ctx context.Context, e OutboxEvent) error
	// InsertProcessedEvent is the consume-side idempotency guard: false means
	// the event was already processed (replay → silent no-op).
	InsertProcessedEvent(ctx context.Context, eventID string) (bool, error)
}

// Tx runs a unit of work atomically: the Store handed to fn is scoped to one
// database transaction — audit row and outbox append commit or roll back together.
type Tx interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

// Publisher is the RabbitMQ publish port (driven; implemented by the rabbitmq
// adapter). Publish intent goes out AFTER the tx commits; failures are
// best-effort per topology §8 — logged, never propagated.
type Publisher interface {
	Publish(ctx context.Context, n Notification) error
}

// EventEncoder turns notifications into outbox events (driven port; the kafka
// adapter implements it with Avro).
type EventEncoder interface {
	NotificationSent(n Notification) (OutboxEvent, error)
}

// NotifyInput carries the consumed order.events fields notification reacts to
// (topology §5: OrderConfirmed / OrderCancelled).
type NotifyInput struct {
	EventID string
	OrderID string
	Kind    Kind
}

// Service is the notification application service.
type Service struct {
	tx  Tx
	pub Publisher
	enc EventEncoder
}

// NewService wires the application service.
func NewService(tx Tx, pub Publisher, enc EventEncoder) *Service {
	return &Service{tx: tx, pub: pub, enc: enc}
}

// Notify absorbs the consumed order.events fact into ONE transaction —
// processed_events guard + notifications audit row + NotificationSent outbox
// event — and, only after that transaction commits, hands the publish intent
// to RabbitMQ. A fresh event id is minted for the emitted fact; the consumed
// event id is guarded by processed_events. Duplicate event ids are silent
// no-ops: no rows, no publish intent, nil error (offset commits safely).
// It reports whether the event was FRESH (business effect applied) or an
// absorbed duplicate replay — the consumer's metric outcome label needs it.
func (s *Service) Notify(ctx context.Context, in NotifyInput) (bool, error) {
	if in.EventID == "" {
		return false, errors.New("event id is required")
	}
	id, err := NewUUID()
	if err != nil {
		return false, err
	}
	n, err := NewNotification(id, in.OrderID, in.Kind)
	if err != nil {
		return false, err
	}
	ev, err := s.enc.NotificationSent(n)
	if err != nil {
		return false, err
	}
	fresh := false
	err = s.tx.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.InsertProcessedEvent(ctx, in.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil // duplicate replay: absorbed, nothing written
		}
		fresh = true
		if err := st.SaveNotification(ctx, &n); err != nil {
			return err
		}
		return st.AppendOutbox(ctx, ev)
	})
	if err != nil {
		return false, err
	}
	if !fresh {
		return false, nil // duplicate replay: absorbed, offset commits safely
	}
	// Post-commit publish intent: best-effort per topology §8.
	s.publishBestEffort(ctx, n)
	return true, nil
}

// publishBestEffort delivers the notification intent to RabbitMQ after the
// transaction has committed. Per topology §8 drops are acceptable: failures
// are logged and swallowed — the Kafka outbox is the audit of record.
func (s *Service) publishBestEffort(ctx context.Context, n Notification) {
	if err := s.pub.Publish(ctx, n); err != nil {
		log.Printf("rabbit publish best-effort failed for order %s: %v (audited via NotificationSent outbox)", n.OrderID, err)
	}
}
