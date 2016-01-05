-- -*- mode: sql; sql-product: postgres -*-
--
-- This adds a "not_before" column to the work unit table.
--
-- +migrate Up
ALTER TABLE work_unit ADD COLUMN not_before TIMESTAMP WITH TIME ZONE;

-- +migrate Down
ALTER TABLE work_unit DROP COLUMN not_before;
