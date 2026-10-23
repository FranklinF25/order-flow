-- Notification audit facts: one row per message handed to the mailer
-- (topology §8). Terminal on creation; replays are absorbed by processed_events.
CREATE TABLE notifications (
    id         TEXT PRIMARY KEY,
    order_id   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    channel    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: facts committed with the audit row, published async.
CREATE TABLE outbox (
    event_id     TEXT PRIMARY KEY,
    record_type  TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (event_id) WHERE published_at IS NULL;

-- Idempotency guard for consumed events (topology §7 real guard).
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
