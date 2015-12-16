-- -*- mode: sql; sql-product: postgres -*-
--
-- This adds a "runtime" column to the work spec table.
--
-- +migrate Up
ALTER TABLE work_spec ADD COLUMN runtime VARCHAR NOT NULL DEFAULT '';

-- +migrate Down
ALTER TABLE work_spec DROP COLUMN runtime;
