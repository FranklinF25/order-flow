ALTER TABLE reservations
    DROP COLUMN IF EXISTS released_at,
    DROP COLUMN IF EXISTS status;
