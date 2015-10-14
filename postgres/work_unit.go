package postgres

import (
	"database/sql"
	"fmt"
	"github.com/dmaze/goordinate/coordinate"
	"strings"
)

type workUnit struct {
	spec *workSpec
	id   int
	name string
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, priority float64) (coordinate.WorkUnit, error) {
	unit := workUnit{spec: spec, name: name}
	dataGob, err := mapToGob(data)
	if err != nil {
		return nil, err
	}
	err = withTx(spec, func(tx *sql.Tx) error {
		// Does the unit already exist?
		query := buildSelect([]string{
			workUnitID,
			attemptStatus,
		}, []string{
			workUnitAttemptJoin,
		}, []string{
			inThisWorkSpec,
			workUnitName + "=$2",
		}) + " FOR UPDATE OF " + workUnitTable
		row := tx.QueryRow(query, spec.id, name)
		var status sql.NullString
		err := row.Scan(&unit.id, &status)
		if err == nil {
			// The unit already exists and we've found its data
			isPending := status.Valid && (status.String == "pending")
			// We need to update its data and priority, and
			// (if not pending) reset its active attempt
			changes := []string{
				"data=$2",
				"priority=$3",
			}
			if !isPending {
				changes = append(changes, "active_attempt_id=NULL")
			}
			query = buildUpdate(workUnitTable, changes, []string{isWorkUnit})
			_, err = tx.Exec(query, unit.id, dataGob, priority)
		} else if err == sql.ErrNoRows {
			// The work unit doesn't exist, yet
			row := tx.QueryRow("INSERT INTO work_unit(work_spec_id, name, data, priority) VALUES ($1, $2, $3, $4) RETURNING id", spec.id, name, dataGob, priority)
			err = row.Scan(&unit.id)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &unit, nil
}

func (spec *workSpec) WorkUnit(name string) (coordinate.WorkUnit, error) {
	unit := workUnit{spec: spec, name: name}
	row := theDB(spec).QueryRow("SELECT id FROM work_unit WHERE work_spec_id=$1 AND name=$2", spec.id, name)
	err := row.Scan(&unit.id)
	if err == sql.ErrNoRows {
		return nil, nil
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
func (spec *workSpec) selectUnits(q coordinate.WorkUnitQuery) (string, []interface{}) {
	// NB: github.com/jmoiron/sqlx has named-parameter binds which
	// will definitely help this.
	outputs := []string{"work_unit.id"}
	tables := []string{"work_unit"}
	conditions := []string{
		"work_spec_id=$1",
	}
	args := []interface{}{spec.id}

	if len(q.Names) > 0 {
		nameparams := make([]string, len(q.Names))
		for i, name := range q.Names {
			nameparams[i] = fmt.Sprintf("$%v", len(args)+1)
			args = append(args, name)
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
				statusBits = append(statusBits, " IS NULL", "='expired'", "='retryable'")
			case coordinate.PendingUnit:
				statusBits = append(statusBits, "='pending'")
			case coordinate.FinishedUnit:
				statusBits = append(statusBits, "='finished'")
			case coordinate.FailedUnit:
				statusBits = append(statusBits, "='failed'")
				// Anything else is an internal error but
				// returning that is irritating; ignore it
			}
		}
		// If AnyStatus was in the list, then this is really
		// a no-op; possibly AnyStatus should just go away
		if !foundAny {
			// Do an outer join on available attempt; this
			// replaces the plain "work_unit" table
			tables = []string{"work_unit LEFT OUTER JOIN attempt ON work_unit.active_attempt_id=attempt.id"}
			for i, bit := range statusBits {
				statusBits[i] = "attempt.status" + bit
			}
			cond := "(" + strings.Join(statusBits, " OR ") + ")"
			conditions = append(conditions, cond)
		}
	}

	if q.PreviousName != "" {
		dollars := fmt.Sprintf("$%v", len(args)+1)
		args = append(args, q.PreviousName)
		cond := "name > " + dollars
		conditions = append(conditions, cond)
	}

	query := buildSelect(outputs, tables, conditions)

	if q.Limit > 0 {
		query += fmt.Sprintf(" ORDER BY name ASC LIMIT %v", q.Limit)
	}

	return query, args
}

func (spec *workSpec) WorkUnits(q coordinate.WorkUnitQuery) (map[string]coordinate.WorkUnit, error) {
	cte, args := spec.selectUnits(q)
	query := buildSelect([]string{
		"id",
		"name",
	}, []string{
		"work_unit",
	}, []string{
		"id IN (" + cte + ")",
	})
	rows, err := theDB(spec).Query(query, args...)
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

func (spec *workSpec) SetWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	cte, args := spec.selectUnits(q)
	dollars := fmt.Sprintf("$%v", len(args)+1)
	args = append(args, priority)
	query := "UPDATE work_unit SET priority=" + dollars + " WHERE id IN (" + cte + ")"
	_, err := theDB(spec).Exec(query, args...)
	return err
}

func (spec *workSpec) AdjustWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	cte, args := spec.selectUnits(q)
	dollars := fmt.Sprintf("$%v", len(args)+1)
	args = append(args, priority)
	query := "UPDATE work_unit SET priority=priority+" + dollars + " WHERE id IN (" + cte + ")"
	_, err := theDB(spec).Exec(query, args...)
	return err
}

func (spec *workSpec) DeleteWorkUnits(q coordinate.WorkUnitQuery) (int, error) {
	cte, args := spec.selectUnits(q)
	query := "DELETE FROM work_unit WHERE id IN (" + cte + ")"
	result, err := theDB(spec).Exec(query, args...)
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
		var dataGob []byte

		// First try to get data from the active attempt
		row := tx.QueryRow("SELECT attempt.data FROM work_unit, attempt WHERE work_unit.id=$1 AND work_unit.active_attempt_id=attempt.id", unit.id)
		err := row.Scan(&dataGob)

		// This could return nothing (e.g., active attempt is
		// null) // or it could return an attempt with no
		// data; in either case get the unit's original data
		if err == sql.ErrNoRows || (err == nil && dataGob == nil) {
			row = tx.QueryRow("SELECT data FROM work_unit WHERE id=$1", unit.id)
			err = row.Scan(&dataGob)
		}
		if err != nil {
			return err
		}
		result, err = gobToMap(dataGob)
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
	// long hand approach
	query := buildSelect([]string{
		"attempt.status",
	}, []string{
		"work_unit LEFT OUTER JOIN attempt ON work_unit.active_attempt_id=attempt.id",
	}, []string{
		"work_unit.id=$1",
	})
	row := theDB(unit).QueryRow(query, unit.id)
	var ns sql.NullString
	err := row.Scan(&ns)
	if err != nil {
		return 0, err
	}
	if !ns.Valid {
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
