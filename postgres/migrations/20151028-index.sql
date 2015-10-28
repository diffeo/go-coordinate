-- -*- mode: sql; sql-product: postgres -*-
-- +migrate Up
CREATE INDEX attempt_status_expiration ON attempt(status, expiration_time);
CREATE INDEX attempt_worker ON attempt(worker_id);
CREATE INDEX work_unit_attempt ON work_unit(active_attempt_id);

-- +migrate Down
DROP INDEX work_unit_attempt;
DROP INDEX attempt_worker;
DROP INDEX attempt_status_expiration;
