-- -*- mode: sql; sql-product: postgres -*-
-- +migrate Up
ALTER TABLE work_spec ADD COLUMN min_memory_gb DOUBLE PRECISION NOT NULL DEFAULT 0;

-- +migrate Down
ALTER TABLE work_spec DROP COLUMN min_memory_gb;
