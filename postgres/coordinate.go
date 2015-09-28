package postgres

import (
	"database/sql"
	"github.com/dmaze/goordinate/coordinate"

	// This Coordinate backend requires the PostgreSQL database/sql
	// driver library, and creates the connection pool here
	_ "github.com/lib/pq"
)

type pgCoordinate struct {
	db *sql.DB
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
	// If the connection string is a destructured URL, turn it
	// back into a proper URL
	if len(connectionString) >= 2 && connectionString[0] == '/' && connectionString[1] == '/' {
		connectionString = "postgres:" + connectionString
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
	return &pgCoordinate{
		db: db,
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

// theDB fetches the database handle from some object in the tree.
func theDB(c coordinable) *sql.DB {
	return c.Coordinate().db
}

// withTx calls some function with a database/sql transaction object.
// If f panics or returns a non-nil error, rolls the transaction back;
// otherwise commits it before returning.  Returns the error value from
// f, or some other error related to transaction management.
func withTx(c coordinable, f func(*sql.Tx) error) (err error) {
	var (
		tx   *sql.Tx
		done bool
	)

	// Create the transaction
	tx, err = theDB(c).Begin()
	if err != nil {
		return
	}

	// If we have a failure, roll back; and if that rollback fails
	// and we don't yet have an error, set the error (how do we
	// get there?)
	defer func() {
		if !done {
			err2 := tx.Rollback()
			if err == nil {
				err = err2
			}
		}
	}()

	// Call the callback function
	err = f(tx)

	// If that succeeded, commit
	if err == nil {
		err = tx.Commit()
		done = true
	}

	// Return, rolling back if needed
	return
}

// scanRows runs an SQL query and calls a function for each row in the
// result.  The callback function should only call the Scan() method on
// the provided Rows object; this function will take care of advancing
// through the list of rows and closing the iterator as required.
func scanRows(rows *sql.Rows, f func() error) (err error) {
	var done bool
	defer func() {
		if !done {
			err2 := rows.Close()
			if err == nil {
				err = err2
			}
		}
	}()

	for rows.Next() {
		err = f()
		if err != nil {
			return
		}
	}
	done = true
	err = rows.Err()
	return
}
