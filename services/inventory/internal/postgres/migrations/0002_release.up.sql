-- M2 compensation B: the reservation gains its state machine. status starts at
-- RESERVED for every existing row (a NOT NULL default backfills in one pass);
-- releaseReservation flips it to RELEASED with a WHERE status = 'RESERVED'
-- guard, so a replayed PaymentFailed or a second failure fact for the same
-- order is a no-op — no double stock restore, no re-emitted StockReleased.
ALTER TABLE reservations
    ADD COLUMN status      TEXT NOT NULL DEFAULT 'RESERVED',
    ADD COLUMN released_at TIMESTAMPTZ;
