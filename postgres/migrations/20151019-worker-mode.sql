-- -*- mode: sql; sql-product: postgres -*-
--
-- This changes the worker mode from int to string.  Existing worker modes
-- are discarded.
--
-- +migrate Up
ALTER TABLE worker ALTER COLUMN mode SET DATA TYPE VARCHAR USING '';

-- +migrate Down
ALTER TABLE worker ALTER COLUMN mode SET DATA TYPE INTEGER USING 0;
