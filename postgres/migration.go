package postgres

import (
	"database/sql"
	"github.com/rubenv/sql-migrate"
)

// This file maintains the database migration code.  See
// https://github.com/rubenv/sql-migrate for details of what goes in
// here.  This runs "outside" the normal coordinate flow, either at
// initial startup or from an external tool.

//go:generate go-bindata -pkg postgres -o migrations.go migrations/

var migrationSource = &migrate.AssetMigrationSource{
	Asset:    Asset,
	AssetDir: AssetDir,
	Dir:      "migrations",
}

// Upgrade upgrades a database to the latest database schema version.
func Upgrade(db *sql.DB) error {
	_, err := migrate.Exec(db, "postgres", migrationSource, migrate.Up)
	return err
}

// Drop clears a database by running all of the migrations in reverse,
// ultimately resulting in dropping all of the tables.
func Drop(db *sql.DB) error {
	_, err := migrate.Exec(db, "postgres", migrationSource, migrate.Down)
	return err
}
