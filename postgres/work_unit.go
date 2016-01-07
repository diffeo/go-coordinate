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
	var unit *workUnit
	dataBytes, err := mapToBytes(data)
	if err != nil {
		return nil, err
	}
	err = withTx(spec, func(tx *sql.Tx) error {
		unit, err = spec.addWorkUnit(tx, name, dataBytes, meta)
		return err
	})
	return unit, err
}

// addWorkUnit does the work of AddWorkUnit, assuming a transaction
// context and that the data dictionary has already been encoded.
func (spec *workSpec) addWorkUnit(tx *sql.Tx, name string, dataBytes []byte, meta coordinate.WorkUnitMeta) (*workUnit, error) {
	unit := workUnit{spec: spec, name: name}

	// Lock the whole work unit table.
	//
	// This is a little unfortunate.  The problem we run into is
	// that SELECT ... FOR UPDATE works fine if the row already
	// exists, but if it doesn't, then nothing gets locked, and so
	// this function can hit a constraint violation if two
	// concurrent calls both SELECT, find nothing, and INSERT the
	// same work unit.
	//
	// Other approaches to this include making transactions
	// SERIALIZABLE (which causes surprisingly many issues; but
	// those should probably be addressed too) and trapping the
	// constraint violation after the INSERT.
	_, err := tx.Exec("LOCK TABLE " + workUnitTable + " IN SHARE ROW EXCLUSIVE MODE")
	if err != nil {
		return nil, err
	}

	// Does the unit already exist?
	params := queryParams{}
	query := buildSelect([]string{
		workUnitID,
		attemptStatus,
	}, []string{
		workUnitAttemptJoin,
	}, []string{
		workUnitInSpec(&params, spec.id),
		workUnitHasName(&params, name),
	}) + " FOR UPDATE OF " + workUnitTable
	row := tx.QueryRow(query, params...)
	var status sql.NullString
	err = row.Scan(&unit.id, &status)

	// Before we go too far, start building up whatever query we're
	// going to do; there are a lot of shared parts
	params = queryParams{}
	fields := fieldList{}
	fields.Add(&params, "data", dataBytes)
	fields.Add(&params, "priority", meta.Priority)
	fields.Add(&params, "not_before", timeToNullTime(meta.NotBefore))

	if err == nil {
		// The unit already exists and we've found its data
		isPending := status.Valid && (status.String == "pending")
		// In addition to the shared fields, if the work unit
		// is not pending, we should reset its active attempt
		if !isPending {
			fields.AddDirect("active_attempt_id", "NULL")
		}
		query = buildUpdate(workUnitTable,
			fields.UpdateChanges(),
			[]string{isWorkUnit(&params, unit.id)})
		_, err = tx.Exec(query, params...)
	} else if err == sql.ErrNoRows {
		// The work unit doesn't exist, yet
		fields.Add(&params, "work_spec_id", spec.id)
		fields.Add(&params, "name", name)
		query := fields.InsertStatement(workUnitTable)
		query += " RETURNING id"
		row := tx.QueryRow(query, params...)
		err = row.Scan(&unit.id)
	}
	if err != nil {
		return nil, err
	}
	return &unit, nil
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
	row := theDB(spec).QueryRow(query, params...)
	err := row.Scan(&unit.id)
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
	_ = withTx(spec, func(tx *sql.Tx) error {
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
	rows, err := theDB(spec).Query(query, params...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]coordinate.WorkUnit)
	err = scanRows(rows, func() error {
		unit := workUnit{spec: spec}
		if err := rows.Scan(&unit.id, &unit.name); err == nil {
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
	_ = withTx(spec, func(tx *sql.Tx) error {
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
	rows, err := theDB(spec).Query(query, params...)
	if err != nil {
		return nil, err
	}
	err = scanRows(rows, func() error {
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
	_ = withTx(spec, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	fields := fieldList{}
	fields.Add(&params, "priority", priority)
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		"id IN (" + cte + ")",
	})
	_, err := theDB(spec).Exec(query, params...)
	return err
}

func (spec *workSpec) AdjustWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	_ = withTx(spec, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	fields := fieldList{}
	fields.AddDirect("priority", "priority+"+params.Param(priority))
	query := buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		"id IN (" + cte + ")",
	})
	_, err := theDB(spec).Exec(query, params...)
	return err
}

func (spec *workSpec) DeleteWorkUnits(q coordinate.WorkUnitQuery) (int, error) {
	_ = withTx(spec, func(tx *sql.Tx) error {
		return expireAttempts(spec, tx)
	})
	cte, params := spec.selectUnits(q, spec.Coordinate().clock.Now())
	query := "DELETE FROM work_unit WHERE id IN (" + cte + ")"
	result, err := theDB(spec).Exec(query, params...)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// WorkUnit interface

func (unit *workUnit) Name() string {
	return unit.name
}

func (unit *workUnit) Data() (map[string]interface{}, error) {
	var result map[string]interface{}
	err := withTx(unit, func(tx *sql.Tx) error {
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
	_ = withTx(unit, func(tx *sql.Tx) error {
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
	row := theDB(unit).QueryRow(query, params...)
	var ns sql.NullString
	var delayed bool
	err := row.Scan(&ns, &delayed)
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
	row := theDB(unit).QueryRow(query, params...)
	err = row.Scan(&meta.Priority, &notBefore)
	meta.NotBefore = nullTimeToTime(notBefore)
	return
}

func (unit *workUnit) SetMeta(meta coordinate.WorkUnitMeta) (err error) {
	notBefore := timeToNullTime(meta.NotBefore)
	params := queryParams{}
	query := buildUpdate(workUnitTable, []string{
		"priority=" + params.Param(meta.Priority),
		"not_before=" + params.Param(notBefore),
	}, []string{
		isWorkUnit(&params, unit.id),
	})
	_, err = theDB(unit).Exec(query, params...)
	return
}

func (unit *workUnit) Priority() (priority float64, err error) {
	row := theDB(unit).QueryRow("SELECT priority FROM work_unit WHERE id=$1", unit.id)
	err = row.Scan(&priority)
	return
}

func (unit *workUnit) SetPriority(priority float64) error {
	_, err := theDB(unit).Exec("UPDATE work_unit SET priority=$2 WHERE id=$1", unit.id, priority)
	return err
}

// coordinable interface

func (unit *workUnit) Coordinate() *pgCoordinate {
	return unit.spec.namespace.coordinate
}
