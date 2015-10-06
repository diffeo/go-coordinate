-- -*- mode: sql; sql-product: postgres -*-
-- This changes the work_unit.name type from VARCHAR to BYTEA.  Any existing
-- work units are assumed to be UTF-8 encoded.

-- +migrate Up
ALTER TABLE work_unit ALTER COLUMN name SET DATA TYPE BYTEA
      USING convert_to(name, 'UTF8');

-- +migrate Down
ALTER TABLE work_unit ALTER COLUMN name SET DATA TYPE VARCHAR
      USING convert_from(name, 'UTF8');
