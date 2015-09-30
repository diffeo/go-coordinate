package postgres

import (
	"database/sql"
	"fmt"
	"github.com/dmaze/goordinate/coordinate"
	"github.com/lib/pq"
	"time"
)

type attempt struct {
	unit   *workUnit
	worker *worker
	id     int
}

// Attempt interface

func (a *attempt) WorkUnit() coordinate.WorkUnit {
	return a.unit
}

func (a *attempt) Worker() coordinate.Worker {
	return a.worker
}

func (a *attempt) Status() (coordinate.AttemptStatus, error) {
	var status string
	row := theDB(a).QueryRow("SELECT status FROM attempt WHERE id=$1", a.id)
	err := row.Scan(&status)
	if err != nil {
		return 0, err
	}
	switch status {
	case "pending":
		return coordinate.Pending, nil
	case "expired":
		return coordinate.Expired, nil
	case "finished":
		return coordinate.Finished, nil
	case "failed":
		return coordinate.Failed, nil
	case "retryable":
		return coordinate.Retryable, nil
	}
	return 0, fmt.Errorf("invalid attempt status in database %v", status)
}

func (a *attempt) Data() (map[string]interface{}, error) {
	var result map[string]interface{}
	err := withTx(a, func(tx *sql.Tx) error {
		var dataGob []byte
		row := tx.QueryRow("SELECT data FROM attempt WHERE id=$1", a.id)
		err := row.Scan(&dataGob)
		if err != nil {
			return err
		}
		if dataGob == nil {
			// null data in the attempt; get the unmodified
			// work unit data
			row = tx.QueryRow("SELECT data FROM work_unit WHERE id=$1", a.unit.id)
			err = row.Scan(&dataGob)
			if err != nil {
				return err
			}
		}
		result, err = gobToMap(dataGob)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

func (a *attempt) StartTime() (result time.Time, err error) {
	row := theDB(a).QueryRow("SELECT start_time FROM attempt WHERE id=$1", a.id)
	err = row.Scan(&result)
	return
}

func (a *attempt) EndTime() (time.Time, error) {
	var nt pq.NullTime
	row := theDB(a).QueryRow("SELECT end_time FROM attempt WHERE id=$1", a.id)
	err := row.Scan(&nt)
	if err != nil {
		return time.Time{}, err
	}
	result := nullTimeToTime(nt)
	return result, nil
}

func (a *attempt) ExpirationTime() (result time.Time, err error) {
	row := theDB(a).QueryRow("SELECT expiration_time FROM attempt WHERE id=$1", a.id)
	err = row.Scan(&result)
	return
}

func (a *attempt) Renew(extendDuration time.Duration, data map[string]interface{}) error {
	// TODO(dmaze): check valid state and active status
	now := time.Now()
	expiration := now.Add(extendDuration)
	conditions := []string{"id=$1"}
	changes := []string{
		"expiration_time=$2",
	}
	args := []interface{}{a.id, expiration}
	if data != nil {
		dataGob, err := mapToGob(data)
		if err != nil {
			return err
		}
		changes = append(changes, "data=$3")
		args = append(args, dataGob)
	}
	query := buildUpdate("attempt", changes, conditions)
	_, err := theDB(a).Exec(query, args...)
	return err
}

func (a *attempt) Expire(data map[string]interface{}) error {
	return a.complete(data, "expired")
}

func (a *attempt) Finish(data map[string]interface{}) error {
	return a.complete(data, "finished")
}

func (a *attempt) Fail(data map[string]interface{}) error {
	return a.complete(data, "failed")
}

func (a *attempt) Retry(data map[string]interface{}) error {
	return a.complete(data, "retryable")
}

func (a *attempt) complete(data map[string]interface{}, status string) error {
	// TODO(dmaze): check valid state transitions
	// TODO(dmaze): check if attempt is active if required
	conditions := []string{
		"id=$1",
	}
	changes := []string{
		"active=FALSE",
		"status=$2",
	}
	args := []interface{}{a.id, status}
	if data != nil {
		changes = append(changes, "data=$3")
		dataGob, err := mapToGob(data)
		if err != nil {
			return err
		}
		args = append(args, dataGob)
	}
	query := buildUpdate("attempt", changes, conditions)
	_, err := theDB(a).Exec(query, args...)
	return err
}

// WorkUnit attempt functions

func (unit *workUnit) ActiveAttempt() (coordinate.Attempt, error) {
	w := worker{namespace: unit.spec.namespace}
	a := attempt{unit: unit, worker: &w}
	query := buildSelect([]string{
		"attempt.id",
		"worker.id",
		"worker.name",
	}, []string{
		"attempt",
		"worker",
		"work_unit",
	}, []string{
		"work_unit.id=$1",
		"attempt.id=work_unit.active_attempt_id",
		"worker.id=attempt.worker_id",
	})
	row := theDB(unit).QueryRow(query, unit.id)
	err := row.Scan(&a.id, &w.id, &w.name)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (unit *workUnit) ClearActiveAttempt() error {
	query := buildUpdate("work_unit", []string{
		"active_attempt_id=NULL",
	}, []string{
		"id=$1",
	})
	_, err := theDB(unit).Exec(query, unit.id)
	return err
}

func (unit *workUnit) Attempts() ([]coordinate.Attempt, error) {
	query := buildSelect([]string{
		"attempt.id",
		"worker.id",
		"worker.name",
	}, []string{
		"attempt",
		"worker",
	}, []string{
		"attempt.work_unit_id=$1",
		"worker.id=attempt.worker_id",
	})
	var result []coordinate.Attempt
	rows, err := theDB(unit).Query(query, unit.id)
	if err != nil {
		return nil, err
	}
	err = scanRows(rows, func() error {
		w := worker{namespace: unit.spec.namespace}
		a := attempt{worker: &w, unit: unit}
		if err := rows.Scan(&a.id, &w.id, &w.name); err == nil {
			result = append(result, &a)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Worker attempt functions

func (w *worker) RequestAttempts(req coordinate.AttemptRequest) ([]coordinate.Attempt, error) {
	// This attempt will mirror the "memory" implementation and its
	// current inconsistencies/quirks.

	var attempts []coordinate.Attempt
	if req.NumberOfWorkUnits < 1 {
		req.NumberOfWorkUnits = 1
	}

	err := withTx(w, func(tx *sql.Tx) error {
		spec, err := w.chooseWorkSpec(tx, req.AvailableGb)
		if err != nil {
			return err
		}
		if spec == nil {
			return nil
		}
		units, err := w.chooseWorkUnits(tx, spec, req.NumberOfWorkUnits)
		if err != nil {
			return err
		}
		length := time.Duration(15) * time.Minute
		for _, unit := range units {
			a, err := makeAttempt(tx, unit, w, length)
			if err != nil {
				return err
			}
			attempts = append(attempts, a)
		}
		return nil
	})
	return attempts, err
}

// chooseWorkSpec chooses some work spec that has available work units.
// If no work spec has available work, return nil.
func (w *worker) chooseWorkSpec(tx *sql.Tx, availableGb float64) (*workSpec, error) {
	// This is a single query that picks some work spec that should
	// have an available work unit.
	//
	// This should consider returning the entire metadata object.
	// Or we should pull all of the metadata objects for all of the
	// work specs.
	query := "SELECT work_spec.id, work_spec.name "
	query += "FROM work_spec, work_unit LEFT OUTER JOIN attempt ON work_unit.active_attempt_id=attempt.id "
	query += "WHERE work_spec.namespace_id=$1 "
	query += "AND work_spec.paused=FALSE "
	query += "AND work_unit.work_spec_id=work_spec.id "
	query += "AND (attempt.status IS NULL OR attempt.status='expired' OR attempt.status='retryable') "
	query += "GROUP BY work_spec.id"
	row := tx.QueryRow(query, w.namespace.id)
	spec := workSpec{namespace: w.namespace}
	err := row.Scan(&spec.id, &spec.name)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &spec, nil
}

// chooseWorkUnits chooses up to a specified number of work units from
// some work spec.
func (w *worker) chooseWorkUnits(tx *sql.Tx, spec *workSpec, numUnits int) ([]*workUnit, error) {
	query := buildSelect([]string{
		"work_unit.id",
		"work_unit.name",
	}, []string{
		"work_unit LEFT OUTER JOIN attempt ON work_unit.active_attempt_id=attempt.id",
	}, []string{
		"work_unit.work_spec_id=$1",
		"(attempt.status IS NULL OR attempt.status='expired' OR attempt.status='retryable')",
	})
	query += fmt.Sprintf(" ORDER BY priority DESC, name ASC LIMIT %v", numUnits)
	rows, err := tx.Query(query, spec.id)
	if err != nil {
		return nil, err
	}
	var result []*workUnit
	err = scanRows(rows, func() error {
		unit := workUnit{spec: spec}
		if err := rows.Scan(&unit.id, &unit.name); err == nil {
			result = append(result, &unit)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (w *worker) MakeAttempt(cUnit coordinate.WorkUnit, length time.Duration) (coordinate.Attempt, error) {
	unit, ok := cUnit.(*workUnit)
	if !ok {
		return nil, coordinate.ErrWrongBackend
	}
	var a *attempt
	var err error
	err = withTx(w, func(tx *sql.Tx) error {
		a, err = makeAttempt(tx, unit, w, length)
		return err
	})
	if err != nil {
		return nil, err
	}
	return a, nil
}

func makeAttempt(tx *sql.Tx, unit *workUnit, w *worker, length time.Duration) (*attempt, error) {
	a := attempt{unit: unit, worker: w}
	now := time.Now()
	expiration := now.Add(length)
	row := tx.QueryRow("INSERT INTO attempt(work_unit_id, worker_id, start_time, expiration_time) VALUES ($1, $2, $3, $4) RETURNING id", unit.id, w.id, now, expiration)
	err := row.Scan(&a.id)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec("UPDATE work_unit SET active_attempt_id=$2 WHERE id=$1", unit.id, a.id)
	return &a, err
}

func (w *worker) ActiveAttempts() ([]coordinate.Attempt, error) {
	return w.findAttempts([]string{}, []string{
		"attempt.worker_id=$1",
		"attempt.active=TRUE",
	})
}

func (w *worker) AllAttempts() ([]coordinate.Attempt, error) {
	return w.findAttempts([]string{}, []string{
		"attempt.worker_id=$1",
	})
}

func (w *worker) ChildAttempts() ([]coordinate.Attempt, error) {
	return w.findAttempts([]string{
		"worker",
	}, []string{
		"attempt.worker_id=worker.id",
		"worker.parent=$1",
	})
}

func (w *worker) findAttempts(tables, conditions []string) ([]coordinate.Attempt, error) {
	outputs := []string{
		"attempt.id",
		"work_unit.id",
		"work_unit.name",
		"work_spec.id",
		"work_spec.name",
	}
	tables = append([]string{"attempt", "work_unit", "work_spec"}, tables...)
	conditions = append([]string{
		"attempt.work_unit_id=work_unit.id",
		"work_unit.work_spec_id=work_spec.id",
	}, conditions...)
	query := buildSelect(outputs, tables, conditions)
	rows, err := theDB(w).Query(query, w.id)
	if err != nil {
		return nil, err
	}
	var result []coordinate.Attempt
	err = scanRows(rows, func() error {
		spec := workSpec{namespace: w.namespace}
		unit := workUnit{spec: &spec}
		a := attempt{worker: w, unit: &unit}
		if err := rows.Scan(&a.id, &unit.id, &unit.name, &spec.id, &spec.name); err == nil {
			result = append(result, &a)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// coordinable interface

func (a *attempt) Coordinate() *pgCoordinate {
	return a.worker.namespace.coordinate
}
