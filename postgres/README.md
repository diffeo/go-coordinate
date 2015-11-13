PostgreSQL Coordinate Backend
=============================

This Coordinate backend uses no local state, but instead stores all of
the information required for the Coordinate system in a PostgreSQL
database.

Usage
-----

Connect to an existing PostgreSQL server:

```sh
coordinated -backend postgres://user:pass@postgres.example.com/database
```

You can also, carefully, use the connection-string format:

```sh
coordinated -backend 'postgres:host=postgres.example.com user=user ...'
```

Or, you can set the connection information in environment variables,
and use an empty connection string:

```sh
export PGHOST=postgres.example.com
export PGDATABASE=database
export PGUSER=user
export PGPASSWORD=password
coordinated -backend postgres:
```

As of this writing, all required tables will be created (and updated
to the current version) on first startup.  There is not yet a
provision to remove existing tables.

Under load, the database may print out log messages

    ERROR:  could not serialize access due to concurrent update

with corresponding SQL statements.  The transaction-handling code in
this module traps this error and correctly retries transactions.  This
error can be safely ignored.

Migrations
----------

Database migrations are implemented with
[sql-migrate](https://github.com/rubenv/sql-migrate).  To add a new
migration file, add it to the `migrations` subdirectory, and run

```sh
go get -u github.com/jteeuwen/go-bindata/...
go generate github.com/diffeo/go-coordinate/postgres
git add src/github.com/diffeo/go-coordinate/postgres/migrations.go
go build github.com/diffeo/go-coordinate/cmd/coordinated
```

This sequence regenerates the `migrations.go` file, which should be
checked in with your other changes.

In the current implementation, the migrations will run automatically
on first startup (or any other call to `postgres.New()`).  In
principle you can also manually run the `sql-migrate` tool, pointing
it at the migrations directory.

Testing
-------

If you use Docker for a temporary database:

```sh	
docker run -d --name postgres -p 5432:5432 postgres
```

Then you can run the tests as:

```sh
export PGHOST=127.0.0.1  # or $(docker-machine ip default)
export PGUSER=postgres
export PGDATABASE=postgres
export PGSSLMODE=disable
go test github.com/diffeo/go-coordinate/postgres
```

Implementation notes
--------------------

Most objects are lightweight records that carry an object's
in-database identifier, name, and parent, and little else.  This means
that calling e.g. `namespace.WorkSpec("foo")` will make a database
round-trip to ensure the work spec exists but will not attempt to
retrieve its definition or work units without explicit calls to fetch
them.

Source files are arranged around database tables, not Coordinate
interface objects.  `work_unit.go` contains all functions that
directly affect the `work_unit` table, including functions like
`coordinate.Namespace.SetWorkUnit()`.

Semi-structured data --- work spec definitions, work unit data, and
per-attempt updated work unit data --- are stored as
[CBOR](http://cbor.io/) data in `BYTEA` columns.  The extracted work
spec metadata is stored in its own columns in the `work_spec` table,
since this is allowed to change independently of the actual work spec
definition.  Consideration was given to a table of work spec ID, data
key, data value, which would be easier to query for specific data
fields, but there is no current use case for this.

Earlier versions of this code used
[gob](http://godoc.org/pkg/encoding/gob) instead, but that cannot (by
design) distinguish `nil` from an empty slice; and when this is
round-tripped to Python code, this causes empty lists to become the
constant `None` instead.  Using CBOR preserves nilness, but comes at
the cost of losing most of the non-kind Go type information (most maps
become `map[interface{}]interface{}`).  In Go land, using a library
like [mapstructure](https://github.com/mitchellh/mapstructure) is
almost necessary for dealing with JSONish objects, and hides this
typing issue.

We rely on the database to manage concurrency for us.  This means
cooperating with the database to tell it what we want, and it means
being able to tolerate (and retry) transaction failures in some cases.
Both of these things result in fairly database-specific code: the
exact syntax of `SELECT ... FOR UPDATE` calls is highly variable
across databases, and the actual error if the database engine traps a
concurrency error is database-specific.  Porting to other databases
involves understanding their concurrency semantics as well as just
updating syntax.

I chose [sql-migrate](https://github.com/rubenv/sql-migrate) as a
database migration tool.  It has the advantages of being able to run
in-process (and not strictly require an external tool or configuration
file) and being able to use
[go-bindata](https://github.com/jteeuwen/go-bindata) as a migration
source.  This also means that, if coordinated wants an ORMish system
in the future, [gorp](https://github.com/go-gorp/gorp) has indirectly
already been chosen.
