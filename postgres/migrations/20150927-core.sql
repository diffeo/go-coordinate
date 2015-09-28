-- -*- mode: sql; sql-product: postgres -*-
-- +migrate Up
CREATE TABLE namespace(
       id SERIAL PRIMARY KEY,
       name VARCHAR UNIQUE NOT NULL
);

CREATE TABLE work_spec(
       id SERIAL PRIMARY KEY,
       namespace_id INTEGER NOT NULL
                    REFERENCES namespace(id) ON DELETE CASCADE,
       name VARCHAR NOT NULL,
       data BYTEA NOT NULL,
       priority INTEGER NOT NULL,
       weight INTEGER NOT NULL,
       paused BOOLEAN NOT NULL,
       continuous BOOLEAN NOT NULL,
       can_be_continuous BOOLEAN NOT NULL,
       interval INTERVAL NOT NULL,
       next_continuous TIMESTAMP WITH TIME ZONE,
       max_running INTEGER NOT NULL,
       max_attempts_returned INTEGER NOT NULL,
       next_work_spec_name VARCHAR NOT NULL,
       next_work_spec_preempts BOOLEAN NOT NULL,
       CONSTRAINT work_spec_unique_name UNIQUE(namespace_id, name)
);

CREATE TABLE work_unit(
       id SERIAL PRIMARY KEY,
       work_spec_id INTEGER NOT NULL
                    REFERENCES work_spec(id) ON DELETE CASCADE,
       name VARCHAR NOT NULL,
       data BYTEA NOT NULL,
       priority DOUBLE PRECISION NOT NULL,
       active_attempt_id INTEGER,
       CONSTRAINT work_unit_unique_name UNIQUE(work_spec_id, name)
);

CREATE TABLE worker(
       id SERIAL PRIMARY KEY,
       namespace_id INTEGER NOT NULL
                    REFERENCES namespace(id) ON DELETE CASCADE,
       name VARCHAR NOT NULL,
       parent INTEGER REFERENCES worker(id) ON DELETE SET NULL,
       active BOOLEAN NOT NULL,
       mode INTEGER NOT NULL,
       data BYTEA NOT NULL,
       expiration TIMESTAMP WITH TIME ZONE NOT NULL,
       last_update TIMESTAMP WITH TIME ZONE NOT NULL,
       CONSTRAINT worker_unique_name UNIQUE(namespace_id, name)
);

CREATE TYPE attempt_status AS ENUM('pending', 'expired', 'finished',
       'failed', 'retryable');

CREATE TABLE attempt(
       id SERIAL PRIMARY KEY,
       work_unit_id INTEGER NOT NULL
                    REFERENCES work_unit(id) ON DELETE CASCADE,
       worker_id INTEGER NOT NULL
                 REFERENCES worker(id) ON DELETE CASCADE,
       status attempt_status NOT NULL DEFAULT 'pending',
       data BYTEA,
       start_time TIMESTAMP WITH TIME ZONE NOT NULL,
       end_time TIMESTAMP WITH TIME ZONE,
       expiration_time TIMESTAMP WITH TIME ZONE NOT NULL,
       active BOOLEAN NOT NULL DEFAULT TRUE
);

ALTER TABLE work_unit
      ADD CONSTRAINT work_unit_active_attempt_valid
      FOREIGN KEY (active_attempt_id) REFERENCES attempt(id)
      ON DELETE SET NULL;

-- +migrate Down
ALTER TABLE work_unit DROP CONSTRAINT work_unit_active_attempt_valid;
DROP TABLE attempt;
DROP TYPE attempt_status;
DROP TABLE worker;
DROP TABLE work_unit;
DROP TABLE work_spec;
DROP TABLE namespace;
