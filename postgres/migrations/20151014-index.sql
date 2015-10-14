-- -*- mode: sql; sql-product: postgres -*-
-- +migrate Up
CREATE INDEX work_unit_spec ON work_unit(work_spec_id);
CREATE INDEX work_unit_spec_attempt ON work_unit(work_spec_id, active_attempt_id);

-- +migrate Down
DROP INDEX work_unit_spec_attempt;
DROP INDEX work_unit_spec;
