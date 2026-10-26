// Package postgres is the driven persistence adapter for the notification
// service: pgx pool, embedded migrations, notification.Store implementation
// and the outbox read side for the relay. One transaction = audit row +
// outbox append (+ processed_events guard).
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

	"github.com/FranklinF25/order-flow/services/notification/internal/notification"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store owns the pgx pool and implements notification.Store, notification.Tx
// and the relay outbox port.
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
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, notification.Store) error) error {
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

func (t *txStore) SaveNotification(ctx context.Context, n *notification.Notification) error {
	return saveNotification(ctx, t.tx, n)
}

func (t *txStore) AppendOutbox(ctx context.Context, e notification.OutboxEvent) error {
	return appendOutbox(ctx, t.tx, e)
}

func (t *txStore) InsertProcessedEvent(ctx context.Context, eventID string) (bool, error) {
	return insertProcessedEvent(ctx, t.tx, eventID)
}

// Pool-level Store methods (writes outside an explicit tx).

func (s *Store) SaveNotification(ctx context.Context, n *notification.Notification) error {
	return saveNotification(ctx, s.pool, n)
}

func (s *Store) AppendOutbox(ctx context.Context, e notification.OutboxEvent) error {
	return appendOutbox(ctx, s.pool, e)
}

func (s *Store) InsertProcessedEvent(ctx context.Context, eventID string) (bool, error) {
	return insertProcessedEvent(ctx, s.pool, eventID)
}

func saveNotification(ctx context.Context, ex execer, n *notification.Notification) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO notifications (id, order_id, kind, channel) VALUES ($1, $2, $3, $4)`,
		n.ID, n.OrderID, string(n.Kind), string(n.Channel),
	)
	return err
}

func insertProcessedEvent(ctx context.Context, ex execer, eventID string) (bool, error) {
	tag, err := ex.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func appendOutbox(ctx context.Context, ex execer, e notification.OutboxEvent) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO outbox (event_id, record_type, aggregate_id, payload) VALUES ($1, $2, $3, $4)`,
		e.EventID, e.RecordType, e.AggregateID, e.Payload,
	)
	return err
}

// CountUnpublished returns the pending outbox row count (published_at IS
// NULL) — the relay refreshes orderflow_outbox_pending with it every tick.
func (s *Store) CountUnpublished(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// FetchUnpublished returns pending outbox events in creation order.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]notification.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, record_type, aggregate_id, payload FROM outbox
		 WHERE published_at IS NULL ORDER BY created_at, event_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []notification.OutboxEvent
	for rows.Next() {
		var e notification.OutboxEvent
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
