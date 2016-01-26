// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"encoding/gob"
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/satori/go.uuid"
	"strings"
)

type pgCoordinate struct {
	db    *sql.DB
	clock clock.Clock
}

// New creates a new coordinate.Coordinate connection object using
// the provided PostgreSQL connection string.  The connection string
// may be an expanded PostgreSQL string, a "postgres:" URL, or a URL
// without a scheme.  These are all equivalent:
//
//     "host=localhost user=postgres password=postgres dbname=postgres"
//     "postgres://postgres:postgres@localhost/postgres"
//     "//postgres:postgres@localhost/postgres"
//
// See http://godoc.org/github.com/lib/pq for more details.  If
// parameters are missing from this string (or if you pass an empty
// string) they can be filled in from environment variables as well;
// see
// http://www.postgresql.org/docs/current/static/libpq-envars.html.
//
// The returned Coordinate object carries around a connection pool
// with it.  It can (and should) be shared across the application.
// This New() function should be called sparingly, ideally exactly once.
func New(connectionString string) (coordinate.Coordinate, error) {
	clk := clock.New()
	return NewWithClock(connectionString, clk)
}

// NewWithClock creates a new coordinate.Coordinate connection object,
// using an explicit time source.  See New() for further details.
// Most application code should call New(), and use the default (real)
// time source; this entry point is intended for tests that need to
// inject a mock time source.
func NewWithClock(connectionString string, clk clock.Clock) (coordinate.Coordinate, error) {
	// If the connection string is a destructured URL, turn it
	// back into a proper URL
	if len(connectionString) >= 2 && connectionString[0] == '/' && connectionString[1] == '/' {
		connectionString = "postgres:" + connectionString
	}

	// Add some custom parameters.
	//
	// We'd love to make the transaction isolation level
	// SERIALIZABLE, and the documentation suggests that it solves
	// all our concurrency problems.  In practice, at least on
	// PostgreSQL 9.3, there are issues with returning duplicate
	// attempts...even though that's a sequence
	//
	// SELECT ... FROM work_units WHERE active_attempt_id IS NULL
	// UPDATE work_units SET active_attempt_id=$1
	//
	// with an obvious conflict?
	if strings.Contains(connectionString, "://") {
		if strings.Contains(connectionString, "?") {
			connectionString += "&"
		} else {
			connectionString += "?"
		}
		connectionString += "default_transaction_isolation=repeatable%20read"
	} else {
		if len(connectionString) > 0 {
			connectionString += " "
		}
		connectionString += "default_transaction_isolation='repeatable read'"
	}

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	// TODO(dmaze): shouldn't unconditionally do this force-upgrade here
	err = Upgrade(db)
	if err != nil {
		return nil, err
	}
	// Make sure the gob library understands our data maps
	gob.Register(map[string]interface{}{})
	gob.Register(map[interface{}]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(cborrpc.PythonTuple{})
	gob.Register(uuid.UUID{})

	return &pgCoordinate{
		db:    db,
		clock: clk,
	}, nil
}

func (c *pgCoordinate) Coordinate() *pgCoordinate {
	return c
}

// coordinable describes the class of structures that can reach back to
// the root pgCoordinate object.
type coordinable interface {
	// Coordinate returns the object at the root of the object tree.
	Coordinate() *pgCoordinate
}
