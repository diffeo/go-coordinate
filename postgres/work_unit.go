// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/lib/pq"
	"strings"
	"time"
)

type workUnit struct {
	spec *workSpec
	id   int
	name string
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, meta coordinate.WorkUnitMeta) (coordinate.WorkUnit, error) {
	dataBytes, err := mapToBytes(data)
	if err != nil {
		return nil, err
	}
	return spec.addWorkUnit(name, dataBytes, meta)
}

// insertUnit attempts to INSERT a work unit into its table.  Failures
// include existence of another work unit with the same key; see
// isDuplicateUnitName() to check.  In addition to the other
// machinery, this function is intended for use in continuous work
// unit generation, where the unit is presumed to not already exist
// and where the transaction context can't be escaped.
func (spec *workSpec) insertWorkUnit(tx *sql.Tx, name string, dataBytes []byte, meta coordinate.WorkUnitMeta) (*workUnit, error) {
	unit := workUnit{spec: spec, name: name}
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "work_spec_id", spec.id)
	fields.Add(&params, "name", name)
	fields.Add(&params, "data", dataBytes)
	fields.Add(&params, "priority", meta.Priority)
	fields.Add(&params, "not_before", timeToNullTime(meta.NotBefore))
	query := fields.InsertStatement(workUnitTable) + " RETURNING id"
	err := tx.QueryRow(query, params...).Scan(&unit.id)
	return &unit, err
}

// isDuplicateUnitName decides if an error is specifically a PostgreSQL
// error due to a duplicate work unit key in workUnit.insert().
func isDuplicateUnitName(err error) bool {
	pqError, isPQ := err.(*pq.Error)
	if !isPQ {
		return false
	}
	if pqError.Code != "23505" {
		return false
	}
	if pqError.Constraint != "work_unit_unique_name" {
		return false
	}
	return true
}

// addWorkUnit does the work of AddWorkUnit, assuming that the data
// dictionary has already been encoded.  It creates its own
// transactions, principally because it needs to be able to retry on a
// failed INSERT.
func (spec *workSpec) addWorkUnit(name string, dataBytes []byte, meta coordinate.WorkUnitMeta) (unit *workUnit, err error) {
	// This is, fundamentally, an UPSERT.  PostgreSQL 9.5 has
	// support for it but is (as of this writing) extremely new.
	// SERIALIZABLE transaction mode should in theory help --
	// SELECT that the unit doesn't exist and then INSERT or
	// UPDATE it as appropriate, and if someone else did the same
	// thing, it should show up as a concurrency error -- but
	// (against PostgreSQL 9.3) this causes other issues,
	// particularly in retrieving work units for attempts.
	//
	// What we will do instead is a client-side loop.  Try to insert
	// the work unit (this should be the common case).  If it already
	// exists, try to update it.  If it doesn't exist at that point,
	// insert it again, and so on.
	for {
		// Step one: give the INSERT a shot.
		err = withTx(spec, false, func(tx *sql.Tx) error {
			var err error
			unit, err = spec.insertWorkUnit(tx, name, dataBytes, meta)
			return err
		})
		if err == nil {
			return
		}
		if !isDuplicateUnitName(err) {
			return
		}

		// Okay, so it already exists.  Let's try to UPDATE
		// an existing unit.
		unit = &workUnit{spec: spec, name: name}
		params := queryParams{}
		fields := fieldList{}
		fields.Add(&params, "data", dataBytes)
		fields.Add(&params, "priority", meta.Priority)
		fields.Add(&params, "not_before", timeToNullTime(meta.NotBefore))
		query := buildUpdate(workUnitTable,
			fields.UpdateChanges(),
			[]string{workUnitHasName(&params, name)}) +
			" RETURNING id"

		// Let's also set up a second query.  If that UPDATE
		// does return a work unit, and it has an active
		// attempt, and the attempt is not pending, then we
		// need to (within the same transaction) clear the
		// active attempt.  This is a little more complicated,
		// and involves some non-default syntax, so let's
		// write it out:
		queryAttempt := "UPDATE " + workUnitTable + " " +
			"SET active_attempt_id=NULL " +
			"FROM " + attemptTable + " " +
			"WHERE " + workUnitID + "=$1 " +
			"AND " + attemptIsTheActive + " " +
			"AND " + attemptStatus + "!='pending'"

		err = withTx(spec, false, func(tx *sql.Tx) error {
			row := tx.QueryRow(query, params...)
			err := row.Scan(&unit.id)
			// Could be ErrNoRows; we'll just return that
			// If that is successful, though, do the
			// second update for the active attempt
			if err == nil {
				_, err = tx.Exec(queryAttempt, unit.id)
			}
			return err
		})
		if err == nil {
			// Updated an existing unit
			return
		}
		if err != sql.ErrNoRows {
			// Something went wrong
			return
		}
		// Otherwise the update didn't find anything; reloop
	}
}

func (spec *workSpec) WorkUnit(name string) (coordinate.WorkUnit, error) {
	unit := workUnit{spec: spec, name: name}
	params := queryParams{}
	query := buildSelect([]string{
		workUnitID,
	}, []string{
		workUnitTable,
	}, []string{
		workUnitInSpec(&params, spec.id),
		workUnitHasName(&params, name),
	})
	err := withTx(spec, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, params...).Scan(&unit.id)
	})
	if err == sql.ErrNoRows {
		return nil, coordinate.ErrNoSuchWorkUnit{Name: name}
	}
	if err != nil {
		return nil, err
	}
	return &unit, nil
}

// selectUnits creates a SELECT statement from a work unit query.
// The returned values from the function are an SQL statement and an
// argument list.  The output of the query is a single column, "id",
// which is a work unit ID.
func (spec *workSpec) selectUnits(q coordinate.WorkUnitQuery, now time.Time) (string, queryParams) {
	// NB: github.com/jmoiron/sqlx has named-parameter binds which
	// will definitely help this.
	outputs := []string{workUnitID}
	tables := []string{workUnitTable}
	params := queryParams{}
	conditions := []string{workUnitInSpec(&params, spec.id)}

	if len(q.Names) > 0 {
		nameparams := make([]string, len(q.Names))
		for i, name := range q.Names {
			nameparams[i] = params.Param(name)
		}
		cond := "name IN (" + strings.Join(nameparams, ", ") + ")"
		conditions = append(conditions, cond)
	}

	if len(q.Statuses) > 0 {
		var statusBits []string
		var foundAny bool
		for _, status := range q.Statuses {
			switch status {
			case coordinate.AnyStatus:
				foundAny = true
			case coordinate.AvailableUnit:
				statusBits = append(statusBits, workUnitAvailable(&params, now))
			case coordinate.PendingUnit:
				statusBits = append(statusBits, attemptStatus+"='pending'")
			case coordinate.FinishedUnit:
				statusBits = append(statusBits, attemptStatus+"='finished'")
			case coordinate.FailedUnit:
				statusBits = append(statusBits, attemptStatus+"='failed'")
			case coordinate.DelayedUnit:
				statusBits = append(statusBits, workUnitDelayed(&params, now))
				// Anything else is an internal error but
				// returning that is irritating; ignore it
			}
		}
		// If AnyStatus was in the list, then this is really
		// a no-op; possibly AnyStatus should just go away
		if !foundAny {
			// Do an outer join on available attempt; this
			// replaces the plain "work_unit" table
			tables = []string{workUnitAttemptJoin}
			cond := "(" + strings.Join(statusBits, " OR ") + ")"
			conditions = append(conditions, cond)
		}
	}

	if q.PreviousName != "" {
		conditions = append(conditions, "name>"+params.Param(q.PreviousName))
	}

	query := buildSelect(outputs, tables, conditions)

	if q.Limit > 0 {
		query += fmt.Sprintf(" ORDER BY name ASC LIMIT %v", q.Limit)
	}

	return query, params
}

func (spec *workSpec) WorkUnits(q coordinate.WorkUnitQuery) (map[string]coordinate.WorkUnit, error) {
	_ = withTx(spec, false, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	query := buildSelect([]string{
		"id",
		"name",
	}, []string{
		"work_unit",
	}, []string{
		"id IN (" + cte + ")",
	})
	result := make(map[string]coordinate.WorkUnit)
	err := queryAndScan(spec, query, params, func(rows *sql.Rows) error {
		unit := workUnit{spec: spec}
		err := rows.Scan(&unit.id, &unit.name)
		if err == nil {
			result[unit.name] = &unit
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (spec *workSpec) CountWorkUnitStatus() (map[coordinate.WorkUnitStatus]int, error) {
	_ = withTx(spec, false, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	now := spec.Coordinate().clock.Now()
	result := make(map[coordinate.WorkUnitStatus]int)
	params := queryParams{}
	query := buildSelect([]string{
		attemptStatus,
		workUnitTooSoon(&params, now) + " AS delayed",
		"COUNT(*)",
	}, []string{
		workUnitAttemptJoin,
	}, []string{
		workUnitInSpec(&params, spec.id),
	}) + " GROUP BY " + attemptStatus + ", delayed"
	err := queryAndScan(spec, query, params, func(rows *sql.Rows) error {
		var (
			status     sql.NullString
			unitStatus coordinate.WorkUnitStatus
			count      int
			delayed    bool
			err        error
		)
		err = rows.Scan(&status, &delayed, &count)
		if err != nil {
			return err
		}
		if delayed {
			unitStatus = coordinate.DelayedUnit
		} else {
			unitStatus = coordinate.AvailableUnit
		}
		if status.Valid {
			switch status.String {
			case "expired", "retryable":
				// same as "available" more or less
			case "pending":
				unitStatus = coordinate.PendingUnit
			case "finished":
				unitStatus = coordinate.FinishedUnit
			case "failed":
				unitStatus = coordinate.FailedUnit
			default:
				return fmt.Errorf("unexpected work unit status %v", status)
			}
		}
		result[unitStatus] += count
		return nil
	})
	return result, err
}

func (spec *workSpec) SetWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	_ = withTx(spec, false, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	fields := fieldList{}
	fields.Add(&params, "priority", priority)
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		"id IN (" + cte + ")",
	})
	return execInTx(spec, query, params)
}

func (spec *workSpec) AdjustWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	_ = withTx(spec, false, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	fields := fieldList{}
	fields.AddDirect("priority", "priority+"+params.Param(priority))
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		"id IN (" + cte + ")",
	})
	return execInTx(spec, query, params)
}

func (spec *workSpec) DeleteWorkUnits(q coordinate.WorkUnitQuery) (count int, err error) {
	_ = withTx(spec, false, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	// If we're trying to delete *everything*, and work is still
	// ongoing, this is extremely likely to hit conflicts.  Do this
	// in smaller batches in a loop.  That makes this non-atomic,
	// but does mean it's extremely likely to complete.
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	query := "DELETE FROM work_unit WHERE id IN (" + cte + " LIMIT 100)"
	keepGoing := true
	for keepGoing && err == nil {
		err = withTx(spec, false, func(tx *sql.Tx) error {
			result, err := tx.Exec(query, params...)
			if err == nil {
				var count64 int64
				count64, err = result.RowsAffected()
				count += int(count64)
				keepGoing = count64 != 0
			}
			return err
		})
	}
	return
}

// WorkUnit interface

func (unit *workUnit) Name() string {
	return unit.name
}

func (unit *workUnit) Data() (map[string]interface{}, error) {
	var result map[string]interface{}
	err := withTx(unit, true, func(tx *sql.Tx) error {
		var dataBytes []byte

		// First try to get data from the active attempt
		row := tx.QueryRow("SELECT attempt.data FROM work_unit, attempt WHERE work_unit.id=$1 AND work_unit.active_attempt_id=attempt.id", unit.id)
		err := row.Scan(&dataBytes)

		// This could return nothing (e.g., active attempt is
		// null) // or it could return an attempt with no
		// data; in either case get the unit's original data
		if err == sql.ErrNoRows || (err == nil && dataBytes == nil) {
			row = tx.QueryRow("SELECT data FROM work_unit WHERE id=$1", unit.id)
			err = row.Scan(&dataBytes)
		}
		if err != nil {
			return err
		}
		result, err = bytesToMap(dataBytes)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (unit *workUnit) WorkSpec() coordinate.WorkSpec {
	return unit.spec
}

func (unit *workUnit) Status() (coordinate.WorkUnitStatus, error) {
	_ = withTx(unit, false, func(tx *sql.Tx) error {
		return expireAttempts(unit, tx)
	})
	now := unit.Coordinate().clock.Now()
	params := queryParams{}
	query := buildSelect([]string{
		attemptStatus,
		workUnitTooSoon(&params, now) + " AS delayed",
	}, []string{
		workUnitAttemptJoin,
	}, []string{
		isWorkUnit(&params, unit.id),
	})
	var ns sql.NullString
	var delayed bool
	err := withTx(unit, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, params...).Scan(&ns, &delayed)
	})
	if err != nil {
		return 0, err
	}
	if !ns.Valid {
		if delayed {
			return coordinate.DelayedUnit, nil
		}
		return coordinate.AvailableUnit, nil
	}
	switch ns.String {
	case "pending":
		return coordinate.PendingUnit, nil
	case "expired":
		return coordinate.AvailableUnit, nil
	case "finished":
		return coordinate.FinishedUnit, nil
	case "failed":
		return coordinate.FailedUnit, nil
	case "retryable":
		return coordinate.AvailableUnit, nil
	}
	return 0, fmt.Errorf("invalid attempt status in database %v", ns.String)
}

func (unit *workUnit) Meta() (meta coordinate.WorkUnitMeta, err error) {
	var notBefore pq.NullTime
	params := queryParams{}
	query := buildSelect([]string{
		workUnitPriority,
		workUnitNotBefore,
	}, []string{
		workUnitTable,
	}, []string{
		isWorkUnit(&params, unit.id),
	})
	err = withTx(unit, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, params...).Scan(&meta.Priority, &notBefore)
	})
	meta.NotBefore = nullTimeToTime(notBefore)
	return
}

func (unit *workUnit) SetMeta(meta coordinate.WorkUnitMeta) error {
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "priority", meta.Priority)
	fields.Add(&params, "not_before", timeToNullTime(meta.NotBefore))
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		isWorkUnit(&params, unit.id),
	})
	return execInTx(unit, query, params)
}

func (unit *workUnit) Priority() (priority float64, err error) {
	params := queryParams{}
	query := buildSelect([]string{workUnitPriority},
		[]string{workUnitTable},
		[]string{isWorkUnit(&params, unit.id)})
	err = withTx(unit, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, params...).Scan(&priority)
	})
	return
}

func (unit *workUnit) SetPriority(priority float64) error {
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "priority", priority)
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		isWorkUnit(&params, unit.id),
	})
	return execInTx(unit, query, params)
}

// coordinable interface

func (unit *workUnit) Coordinate() *pgCoordinate {
	return unit.spec.namespace.coordinate
}
