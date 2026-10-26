// Package postgres is the driven persistence adapter for the payment service:
// pgx pool, embedded migrations, payment.Store implementation and the outbox
// read side for the relay. One transaction = processed-events guard + payment
// row + outbox append (topology §6/§7).
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FranklinF25/order-flow/services/payment/internal/payment"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store owns the pgx pool and implements payment.Store, payment.Tx and the
// relay outbox port.
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens the pool.
func Connect(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies embedded migrations (golang-migrate, pgx5 driver).
func (s *Store) Migrate(dsn string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate source: %w", err)
	}
	url := strings.Replace(dsn, "postgres://", "pgx5://", 1)
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// WithinTx runs fn atomically; the Store passed to fn is scoped to the tx.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, payment.Store) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(ctx, &txStore{tx: tx}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

type txStore struct{ tx pgx.Tx }

func (t *txStore) SavePayment(ctx context.Context, p *payment.Payment) (bool, error) {
	return savePayment(ctx, t.tx, p)
}

func (t *txStore) AppendProcessedEvent(ctx context.Context, eventID string) (bool, error) {
	return appendProcessedEvent(ctx, t.tx, eventID)
}

func (t *txStore) AppendOutbox(ctx context.Context, e payment.OutboxEvent) error {
	return appendOutbox(ctx, t.tx, e)
}

// Pool-level Store methods (writes outside an explicit tx).

func (s *Store) SavePayment(ctx context.Context, p *payment.Payment) (bool, error) {
	return savePayment(ctx, s.pool, p)
}

func (s *Store) AppendProcessedEvent(ctx context.Context, eventID string) (bool, error) {
	return appendProcessedEvent(ctx, s.pool, eventID)
}

func (s *Store) AppendOutbox(ctx context.Context, e payment.OutboxEvent) error {
	return appendOutbox(ctx, s.pool, e)
}

// savePayment inserts the CAPTURED payment. A second capture for the same
// order hits the order_id UNIQUE constraint and becomes a silent no-op
// (topology §6: payments idempotent per order_id).
func savePayment(ctx context.Context, ex execer, p *payment.Payment) (bool, error) {
	tag, err := ex.Exec(ctx,
		`INSERT INTO payments (id, order_id, amount_cents, status)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (order_id) DO NOTHING`,
		p.ID, p.OrderID, p.AmountCents, string(p.Status),
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// appendProcessedEvent marks a consumed event id. ON CONFLICT DO NOTHING +
// RowsAffected is the duplicate signal the service uses to skip silently.
func appendProcessedEvent(ctx context.Context, ex execer, eventID string) (bool, error) {
	tag, err := ex.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1)
		 ON CONFLICT (event_id) DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func appendOutbox(ctx context.Context, ex execer, e payment.OutboxEvent) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO outbox (event_id, record_type, aggregate_id, payload) VALUES ($1, $2, $3, $4)`,
		e.EventID, e.RecordType, e.AggregateID, e.Payload,
	)
	return err
}

// CountUnpublished counts pending outbox rows (published_at IS NULL) — the
// orderflow_outbox_pending gauge (M3 pinned contract) refreshes from it on
// every relay tick.
func (s *Store) CountUnpublished(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// FetchUnpublished returns pending outbox events in creation order.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]payment.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, record_type, aggregate_id, payload FROM outbox
		 WHERE published_at IS NULL ORDER BY created_at, event_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []payment.OutboxEvent
	for rows.Next() {
		var e payment.OutboxEvent
		if err := rows.Scan(&e.EventID, &e.RecordType, &e.AggregateID, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// MarkPublished marks the given events as published (audit retention).
func (s *Store) MarkPublished(ctx context.Context, eventIDs []string, at time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET published_at = $1 WHERE event_id = ANY($2)`, at, eventIDs)
	return err
}
