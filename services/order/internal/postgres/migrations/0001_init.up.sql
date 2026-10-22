CREATE TABLE orders (
    id            TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL,
    items         JSONB NOT NULL,
    total_cents   BIGINT NOT NULL,
    status        TEXT NOT NULL,
    cancel_reason TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
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
