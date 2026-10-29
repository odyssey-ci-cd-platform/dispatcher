-- Steps gain lifecycle timestamps and error text (gh-95, AUD-025): the
-- event stream already carries both, the table just dropped them.

-- +goose Up
ALTER TABLE steps ADD COLUMN started_at TEXT;
ALTER TABLE steps ADD COLUMN finished_at TEXT;
ALTER TABLE steps ADD COLUMN error TEXT;

-- +goose Down
ALTER TABLE steps DROP COLUMN started_at;
ALTER TABLE steps DROP COLUMN finished_at;
ALTER TABLE steps DROP COLUMN error;
