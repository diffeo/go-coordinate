-- -*- mode: sql; sql-product: postgres -*-
--
-- This adds a work_spec column to the attempt table, so that we can
-- efficiently find work specs with pending work.

-- +migrate Up
ALTER TABLE attempt
ADD COLUMN work_spec_id INTEGER
                        REFERENCES work_spec(id) ON DELETE CASCADE;
                        
UPDATE attempt
SET work_spec_id=work_unit.work_spec_id
FROM work_unit
WHERE work_unit.id=attempt.work_unit_id;

ALTER TABLE attempt
ALTER COLUMN work_spec_id SET NOT NULL;

-- +migrate Down
ALTER TABLE attempt
DROP COLUMN work_spec_id;
