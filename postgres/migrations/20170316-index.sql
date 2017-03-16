-- -*- mode: sql; sql-product: postgres -*-
--
-- Adds index to worker.parent so a common lookup is optimized
--
-- +migrate Up

CREATE INDEX CONCURRENTLY worker_parent_idx on worker(parent);

-- +migrate Down
DROP INDEX worker_parent_idx;
