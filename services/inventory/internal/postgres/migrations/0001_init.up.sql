-- Stock is one row per SKU. CHECK (available >= 0) is the concurrency backstop:
-- two orders sharing a SKU can race across order.events partitions; the domain
-- check runs per transaction, and a losing transaction aborts on this
-- constraint and replays via at-least-once delivery instead of overselling.
CREATE TABLE stock (
    sku       TEXT PRIMARY KEY,
    available INTEGER NOT NULL CHECK (available >= 0)
);

-- Reservations are inventory-internal facts, idempotent per order_id.
CREATE TABLE reservations (
    order_id    TEXT PRIMARY KEY,
    items       JSONB NOT NULL,
    total_cents BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: facts committed with the state change, published async.
CREATE TABLE outbox (
    event_id     TEXT PRIMARY KEY,
    record_type  TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (event_id) WHERE published_at IS NULL;

-- Idempotency guard for consumed events (M1 real guard per topology §7).
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seeded catalog for the M1 happy path.
INSERT INTO stock (sku, available) VALUES
    ('SKU-A', 100),
    ('SKU-B', 50),
    ('SKU-C', 10);
