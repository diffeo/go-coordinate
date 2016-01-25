-- -*- mode: sql; sql-product: postgres -*-
--
-- This adds an index to efficiently find attempts by their work
-- units.  In particular, this fixes a problem where deleting large
-- numbers of finished work units is slow; experimentally it leads to
-- O(n^2) behavior on the single SQL DELETE command.
--
-- +migrate Up
CREATE INDEX attempt_work_unit ON attempt(work_unit_id);

-- +migrate Down
DROP INDEX attempt_work_unit;
