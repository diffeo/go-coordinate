-- -*- mode: sql; sql-product: postgres -*-
--
-- This adds another restricted index to help find work specs with
-- available work, when many of the work units have been attempted
-- already.
--
-- +migrate Up
CREATE INDEX work_unit_spec_available ON work_unit(work_spec_id)
       WHERE active_attempt_id IS NULL;

-- +migrate Down
DROP INDEX work_unit_spec_available;
