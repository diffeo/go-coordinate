-- -*- mode: sql; sql-product: postgres -*-
--
-- This updates the index that finds work units with the highest priority
-- to skip over work units with active attempts, that can't be done.  It
-- also drops a redundant index of work specs and active attempt IDs; if
-- PostgreSQL needs both things it can ask both single-field indexes.
--
-- +migrate Up
DROP INDEX work_unit_ordering;
CREATE INDEX work_unit_ordering ON work_unit(priority DESC, name ASC)
       WHERE active_attempt_id IS NULL;
DROP INDEX work_unit_spec_attempt;

-- +migrate Down
DROP INDEX work_unit_ordering;
CREATE INDEX work_unit_ordering ON work_unit(priority DESC, name ASC);
CREATE INDEX work_unit_spec_attempt ON work_unit(work_spec_id, active_attempt_id);
