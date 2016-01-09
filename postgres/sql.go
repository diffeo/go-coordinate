// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

// This file contains extremely generic support code for PostgreSQL
// applications.  This is in fact exactly the sort of thing that would
// be broken out into a generic support library.  How much of this is
// included in, for instance, sqlx?
//
// There are four main things in here:
//
// (1) Functions to help with database/sql: withTx() to do work in a
//     transaction that can be retried, and scanRows() to loop over the
//     results of a multi-row SELECT
//
// (2) Data marshallers for time.Duration and time.Time
//
// (3) Helpers to build SQL SELECT and UPDATE statements (dealing
//     entirely in strings)
//
// (4) Helpers to manage potentially long query parameter lists:
//     queryParams is a parameter list that can produce $1, $2, ... out,
//     and fieldList is an INSERT/UPDATE key=value list

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"math"
	"strings"
	"time"
)

// withTx calls some function with a database/sql transaction object.
// If f panics or returns a non-nil error, rolls the transaction back;
// otherwise commits it before returning.  Returns the error value from
// f, or some other error related to transaction management.
func withTx(c coordinable, readOnly bool, f func(*sql.Tx) error) (err error) {
	var (
		tx   *sql.Tx
		done bool
	)

	// If we have a failure, roll back; and if that rollback fails
	// and we don't yet have an error, set the error (how do we
	// get there?)
	defer func() {
		if tx != nil && !done {
			err2 := tx.Rollback()
			if err == nil {
				err = err2
			}
		}
	}()

	// Run in a loop, repeating the work on serialization errors
	for {
		// Create the transaction
		tx, err = c.Coordinate().db.Begin()
		if err != nil {
			return
		}

		// We'd love to make this SERIALIZABLE, and the
		// documentation suggests that it solves all our
		// concurrency problems.  In practice, at least on
		// PostgreSQL 9.3, there are issues with returning
		// duplicate attempts...even though that's a sequence
		//
		// SELECT ... FROM work_units WHERE active_attempt_id IS NULL
		// UPDATE work_units SET active_attempt_id=$1
		//
		// with an obvious conflict?
		level := "REPEATABLE READ"
		if readOnly {
			level += " READ ONLY"
		}
		_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL " + level)
		if err != nil {
			return
		}

		// Call the callback function
		err = f(tx)

		// If that succeeded, commit
		if err == nil {
			err = tx.Commit()
			done = true
		}

		// If we specifically got a serialization error,
		// retry
		if pqerr, ok := err.(*pq.Error); ok {
			if pqerr.Code == "40001" {
				err = tx.Rollback()
				if err == sql.ErrTxDone {
					// We want to roll back, but we
					// can't, because we've already
					// rolled back; not an error
					err = nil
				} else if err != nil {
					return
				}
				tx = nil
				continue
			}
		}

		break
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

// queryAndScan establishes a read-only transaction, runs query on it
// with params, and calls f for each row in it.  It is the common case
// of combining withTx() and scanRows().
func queryAndScan(c coordinable, query string, params queryParams, f func(*sql.Rows) error) error {
	return withTx(c, true, func(tx *sql.Tx) error {
		rows, err := tx.Query(query, params...)
		if err != nil {
			return err
		}
		return scanRows(rows, func() error {
			return f(rows)
		})
	})
}

// execInTx establishes a read-write transaction and executes a
// statement, dropping the result.  It is the common case of combining
// withTx() and a simple tx.Exec().
func execInTx(c coordinable, query string, params queryParams) error {
	return withTx(c, false, func(tx *sql.Tx) error {
		_, err := tx.Exec(query, params...)
		return err
	})
}

// durationToSQL converts a time.Duration to ISO standard SQL syntax,
// e.g. "1 2:3:4" for one day, two hours, three minutes, and four seconds.
func durationToSQL(d time.Duration) []byte {
	dSeconds := d.Seconds()
	dMinutes, fSeconds := math.Modf(dSeconds / 60)
	seconds := fSeconds * 60
	dHours, fMinutes := math.Modf(dMinutes / 60)
	minutes := fMinutes * 60
	days, fHours := math.Modf(dHours / 24)
	hours := fHours * 24
	sql := fmt.Sprintf("%.0f %.0f:%.0f:%f", days, hours, minutes, seconds)
	return []byte(sql)
}

func sqlToDuration(sql string) (time.Duration, error) {
	var (
		days, hours, minutes int64
		seconds              float64
		err                  error
	)
	// Two shots
	_, err = fmt.Sscanf(sql, "%d:%d:%f", &hours, &minutes, &seconds)
	if err != nil {
		_, err = fmt.Sscanf(sql, "%d %d:%d:%f", &days, &hours, &minutes, &seconds)
	}
	// Duration's unit is nanoseconds; make sure everything has int64
	// type
	dHours := hours * 24 * days
	dMinutes := minutes + 60*dHours
	dSeconds := seconds + 60*float64(dMinutes)
	d := time.Duration(int64(float64(dSeconds * float64(time.Second))))
	return d, err
}

// timeToNullTime encodes a time as a pq-specific NullTime, by mapping the
// zero time to null.
func timeToNullTime(t time.Time) pq.NullTime {
	return pq.NullTime{Time: t, Valid: !t.IsZero()}
}

// nullTimeToTime decodes a pq-specific NullTime to a time, by mapping
// a null value to zero time.
func nullTimeToTime(nt pq.NullTime) time.Time {
	if nt.Valid {
		return nt.Time
	}
	return time.Time{}
}

// buildSelect constructs a simple SQL SELECT statement by string
// concatenation.  All of the conditions are ANDed together.
func buildSelect(outputs, tables, conditions []string) string {
	query := "SELECT "
	query += strings.Join(outputs, ", ")
	query += " FROM "
	query += strings.Join(tables, ", ")
	if len(conditions) > 0 {
		query += " WHERE "
		query += strings.Join(conditions, " AND ")
	}
	return query
}

// buildUpdate constructs a simple SQL UPDATE statement by string
// concatenation.  All of the conditions are ANDed together.
func buildUpdate(table string, changes, conditions []string) string {
	query := "UPDATE " + table
	if len(changes) > 0 {
		query += " SET " + strings.Join(changes, ", ")
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	return query
}

// queryParams wraps a list of query parameters.
type queryParams []interface{}

// Param adds a parameter to the query parameter list, returning its
// position as $1, $2, ...
func (qp *queryParams) Param(param interface{}) string {
	*qp = append(*qp, param)
	return fmt.Sprintf("$%v", len(*qp))
}

// fieldPair is a pair of values in a fieldList.
type fieldPair struct {
	Field string
	Value string
}

// Equals converts a pair into an (unquoted) "field=value" SQL fragment.
func (fp fieldPair) AsEquals() string {
	return fp.Field + "=" + fp.Value
}

// fieldList is a list of "field=value" pairs as appears in SQL INSERT
// and UPDATE statements.
type fieldList struct {
	Fields []fieldPair
}

// Adds a name and dynamic value to the field list.
func (f *fieldList) Add(qp *queryParams, field string, value interface{}) {
	f.AddDirect(field, qp.Param(value))
}

// Adds a name and fixed value to the field list.  value is an unquoted
// SQL string.
func (f *fieldList) AddDirect(field, value string) {
	f.Fields = append(f.Fields, fieldPair{Field: field, Value: value})
}

// MapFields converts a field list to a string slice by calling a
// function on every field pair.
func (f fieldList) MapFields(mf func(fp fieldPair) string) []string {
	result := make([]string, len(f.Fields))
	for i, field := range f.Fields {
		result[i] = mf(field)
	}
	return result
}

// FieldNames returns just the field names out as an array.
func (f fieldList) FieldNames() []string {
	return f.MapFields(func(fp fieldPair) string { return fp.Field })
}

// FieldValues returns just the field values out as an array.
func (f fieldList) FieldValues() []string {
	return f.MapFields(func(fp fieldPair) string { return fp.Value })
}

// InsertNames produces the names for an SQL INSERT statement as a
// comma-separated list with no additional punctuation.
func (f fieldList) InsertNames() string {
	return strings.Join(f.FieldNames(), ", ")
}

// InsertValues produces the values for an SQL INSERT statement as a
// comma-separated list with no additional punctuation.
func (f fieldList) InsertValues() string {
	return strings.Join(f.FieldValues(), ", ")
}

// InsertStatement produces a syntactically complete SQL INSERT statement.
func (f fieldList) InsertStatement(table string) string {
	return "INSERT INTO " + table + "(" + f.InsertNames() + ") VALUES(" + f.InsertValues() + ")"
}

// UpdateChanges converts a field list into a list of "field=value"
// statements, suitable for the "changes" part of an UPDATE statement.
func (f fieldList) UpdateChanges() []string {
	return f.MapFields(func(fp fieldPair) string { return fp.AsEquals() })
}
