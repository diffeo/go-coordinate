-- -*- mode: sql; sql-product: postgres -*-
--
-- Adds a max_retries field to work_spec.
--
-- +migrate Up
ALTER TABLE work_spec ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 0;

-- +migrate Down
ALTER TABLE work_spec DROP COLUMN max_retries;
