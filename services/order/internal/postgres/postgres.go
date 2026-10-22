// Package postgres is the driven persistence adapter for the order service:
// pgx pool, embedded migrations, order.Store implementation and the outbox
// read side for the relay. One transaction = state change + outbox append.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
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

	"github.com/FranklinF25/order-flow/services/order/internal/order"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store owns the pgx pool and implements order.Store, order.Tx and the relay
// outbox port.
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
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// WithinTx runs fn atomically; the Store passed to fn is scoped to the tx.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, order.Store) error) error {
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

func (t *txStore) SaveOrder(ctx context.Context, o *order.Order) error {
	return saveOrder(ctx, t.tx, o)
}

func (t *txStore) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	return getOrder(ctx, t.tx, id)
}

func (t *txStore) AppendOutbox(ctx context.Context, e order.OutboxEvent) error {
	return appendOutbox(ctx, t.tx, e)
}

// Pool-level Store methods (reads and writes outside an explicit tx).

func (s *Store) SaveOrder(ctx context.Context, o *order.Order) error {
	return saveOrder(ctx, s.pool, o)
}

func (s *Store) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	return getOrder(ctx, s.pool, id)
}

func (s *Store) AppendOutbox(ctx context.Context, e order.OutboxEvent) error {
	return appendOutbox(ctx, s.pool, e)
}

func saveOrder(ctx context.Context, ex execer, o *order.Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}
	_, err = ex.Exec(ctx,
		`INSERT INTO orders (id, customer_id, items, total_cents, status, cancel_reason)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		o.ID, o.CustomerID, items, o.TotalCents, string(o.Status), string(o.CancelReason),
	)
	return err
}

func getOrder(ctx context.Context, ex execer, id string) (*order.Order, error) {
	var (
		o            order.Order
		status       string
		cancelReason string
		items        []byte
	)
	err := ex.QueryRow(ctx,
		`SELECT id, customer_id, items, total_cents, status, cancel_reason FROM orders WHERE id = $1`,
		id,
	).Scan(&o.ID, &o.CustomerID, &items, &o.TotalCents, &status, &cancelReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, order.ErrOrderNotFound
	}
	if err != nil {
		return nil, err
	}
	o.Status = order.Status(status)
	o.CancelReason = order.CancelReason(cancelReason)
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return nil, fmt.Errorf("unmarshal items: %w", err)
	}
	return &o, nil
}

func appendOutbox(ctx context.Context, ex execer, e order.OutboxEvent) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO outbox (event_id, record_type, aggregate_id, payload) VALUES ($1, $2, $3, $4)`,
		e.EventID, e.RecordType, e.AggregateID, e.Payload,
	)
	return err
}

// FetchUnpublished returns pending outbox events in creation order.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]order.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, record_type, aggregate_id, payload FROM outbox
		 WHERE published_at IS NULL ORDER BY created_at, event_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []order.OutboxEvent
	for rows.Next() {
		var e order.OutboxEvent
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
