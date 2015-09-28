package postgres_test

import (
	"github.com/dmaze/goordinate/coordinate/coordinatetest"
	"github.com/dmaze/goordinate/postgres"
	"gopkg.in/check.v1"
	"testing"
)

// Test is the top-level entry point to run tests.
//
// This creates a PostgreSQL Coordinate backend using an empty string
// as the connection string.  This means that, when you run "go test",
// you must set environment variables as describe in
// http://www.postgresql.org/docs/current/static/libpq-envars.html
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	c, err := postgres.New("")
	if err != nil {
		panic(err)
	}
	check.Suite(&coordinatetest.Suite{Coordinate: c})
}
