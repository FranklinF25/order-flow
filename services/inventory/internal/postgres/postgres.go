// Package postgres is the driven persistence adapter for the inventory service:
// pgx pool, embedded migrations (seeded stock catalog), inventory.Store
// implementation and the outbox read side for the relay. One transaction =
// processed_events guard + stock/reservation state change + outbox append.
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

	"github.com/FranklinF25/order-flow/services/inventory/internal/inventory"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store owns the pgx pool and implements inventory.Store, inventory.Tx and the
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

// Migrate applies embedded migrations (golang-migrate, pgx5 driver); 0001 seeds
// the stock catalog.
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
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// WithinTx runs fn atomically; the Store passed to fn is scoped to the tx.
func (s *Store) WithinTx(ctx context.Context, fn func(context.Context, inventory.Store) error) error {
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

func (t *txStore) MarkProcessed(ctx context.Context, eventID string) error {
	return markProcessed(ctx, t.tx, eventID)
}

func (t *txStore) GetStock(ctx context.Context, sku string) (*inventory.Stock, error) {
	return getStock(ctx, t.tx, sku)
}

func (t *txStore) ListStock(ctx context.Context) ([]inventory.Stock, error) {
	return listStock(ctx, t.tx)
}

func (t *txStore) DecrementStock(ctx context.Context, sku string, qty int) error {
	return decrementStock(ctx, t.tx, sku, qty)
}

func (t *txStore) SaveReservation(ctx context.Context, r inventory.Reservation) error {
	return saveReservation(ctx, t.tx, r)
}

func (t *txStore) AppendOutbox(ctx context.Context, e inventory.OutboxEvent) error {
	return appendOutbox(ctx, t.tx, e)
}

func (t *txStore) GetReservation(ctx context.Context, orderID string) (*inventory.Reservation, error) {
	return getReservation(ctx, t.tx, orderID)
}

func (t *txStore) ReleaseReservation(ctx context.Context, orderID string) (bool, error) {
	return releaseReservation(ctx, t.tx, orderID)
}

func (t *txStore) IncrementStock(ctx context.Context, sku string, qty int) error {
	return incrementStock(ctx, t.tx, sku, qty)
}

// Pool-level Store methods (reads and writes outside an explicit tx).

func (s *Store) MarkProcessed(ctx context.Context, eventID string) error {
	return markProcessed(ctx, s.pool, eventID)
}

func (s *Store) GetStock(ctx context.Context, sku string) (*inventory.Stock, error) {
	return getStock(ctx, s.pool, sku)
}

func (s *Store) ListStock(ctx context.Context) ([]inventory.Stock, error) {
	return listStock(ctx, s.pool)
}

func (s *Store) DecrementStock(ctx context.Context, sku string, qty int) error {
	return decrementStock(ctx, s.pool, sku, qty)
}

func (s *Store) SaveReservation(ctx context.Context, r inventory.Reservation) error {
	return saveReservation(ctx, s.pool, r)
}

func (s *Store) AppendOutbox(ctx context.Context, e inventory.OutboxEvent) error {
	return appendOutbox(ctx, s.pool, e)
}

func (s *Store) GetReservation(ctx context.Context, orderID string) (*inventory.Reservation, error) {
	return getReservation(ctx, s.pool, orderID)
}

func (s *Store) ReleaseReservation(ctx context.Context, orderID string) (bool, error) {
	return releaseReservation(ctx, s.pool, orderID)
}

func (s *Store) IncrementStock(ctx context.Context, sku string, qty int) error {
	return incrementStock(ctx, s.pool, sku, qty)
}

func markProcessed(ctx context.Context, ex execer, eventID string) error {
	tag, err := ex.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return inventory.ErrDuplicateEvent
	}
	return nil
}

func getStock(ctx context.Context, ex execer, sku string) (*inventory.Stock, error) {
	var st inventory.Stock
	err := ex.QueryRow(ctx, `SELECT sku, available FROM stock WHERE sku = $1`, sku).
		Scan(&st.SKU, &st.Available)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, inventory.ErrStockNotFound
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func listStock(ctx context.Context, q execer) ([]inventory.Stock, error) {
	rows, err := q.Query(ctx, `SELECT sku, available FROM stock ORDER BY sku`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []inventory.Stock
	for rows.Next() {
		var st inventory.Stock
		if err := rows.Scan(&st.SKU, &st.Available); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// decrementStock relies on the domain check that ran inside this same
// transaction; the stock CHECK (available >= 0) is the cross-partition
// concurrency backstop (a losing tx aborts and replays, never oversells).
func decrementStock(ctx context.Context, ex execer, sku string, qty int) error {
	_, err := ex.Exec(ctx, `UPDATE stock SET available = available - $2 WHERE sku = $1`, sku, qty)
	return err
}

func saveReservation(ctx context.Context, ex execer, r inventory.Reservation) error {
	items, err := json.Marshal(r.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}
	_, err = ex.Exec(ctx,
		`INSERT INTO reservations (order_id, items, total_cents) VALUES ($1, $2, $3)`,
		r.OrderID, items, r.TotalCents,
	)
	return err
}

func appendOutbox(ctx context.Context, ex execer, e inventory.OutboxEvent) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO outbox (event_id, record_type, aggregate_id, payload) VALUES ($1, $2, $3, $4)`,
		e.EventID, e.RecordType, e.AggregateID, e.Payload,
	)
	return err
}

func getReservation(ctx context.Context, ex execer, orderID string) (*inventory.Reservation, error) {
	var (
		r      inventory.Reservation
		items  []byte
		status string
	)
	err := ex.QueryRow(ctx,
		`SELECT order_id, items, total_cents, status FROM reservations WHERE order_id = $1`, orderID).
		Scan(&r.OrderID, &items, &r.TotalCents, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, inventory.ErrReservationNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(items, &r.Items); err != nil {
		return nil, fmt.Errorf("unmarshal reservation items: %w", err)
	}
	r.Status = inventory.ReservationStatus(status)
	return &r, nil
}

// releaseReservation takes RESERVED→RELEASED; the WHERE status = 'RESERVED'
// guard makes the transition atomic (mirrors the domain state machine and the
// stock CHECK backstop): a losing concurrent tx reports no transition and the
// caller absorbs instead of double-restoring or re-emitting.
func releaseReservation(ctx context.Context, ex execer, orderID string) (bool, error) {
	tag, err := ex.Exec(ctx,
		`UPDATE reservations SET status = 'RELEASED', released_at = now()
		 WHERE order_id = $1 AND status = 'RESERVED'`, orderID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// incrementStock restores quantities on release (compensation B); it is the
// exact inverse of decrementStock and runs in the same tx as the transition.
func incrementStock(ctx context.Context, ex execer, sku string, qty int) error {
	_, err := ex.Exec(ctx, `UPDATE stock SET available = available + $2 WHERE sku = $1`, sku, qty)
	return err
}

// CountUnpublished returns the outbox backlog (published_at IS NULL) — the
// relay refreshes orderflow_outbox_pending from it on every tick (M3 metric
// contract; parent of the head-of-line stall alert).
func (s *Store) CountUnpublished(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// FetchUnpublished returns pending outbox events in creation order.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]inventory.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, record_type, aggregate_id, payload FROM outbox
		 WHERE published_at IS NULL ORDER BY created_at, event_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []inventory.OutboxEvent
	for rows.Next() {
		var e inventory.OutboxEvent
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
