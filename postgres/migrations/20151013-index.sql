-- -*- mode: sql; sql-product: postgres -*-
-- +migrate Up
CREATE INDEX work_unit_ordering ON work_unit(priority DESC, name ASC);

-- +migrate Down
DROP INDEX work_unit_ordering;
