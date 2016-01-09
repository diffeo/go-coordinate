// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
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
	_ = withTx(a, false, func(tx *sql.Tx) error {
		return expireAttempts(a, tx)
	})

	var status string
	err := withTx(a, true, func(tx *sql.Tx) error {
		return tx.QueryRow("SELECT status FROM attempt WHERE id=$1", a.id).Scan(&status)
	})
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
	err := withTx(a, true, func(tx *sql.Tx) error {
		var dataBytes []byte
		row := tx.QueryRow("SELECT data FROM attempt WHERE id=$1", a.id)
		err := row.Scan(&dataBytes)
		if err != nil {
			return err
		}
		if dataBytes == nil {
			// null data in the attempt; get the unmodified
			// work unit data
			row = tx.QueryRow("SELECT data FROM work_unit WHERE id=$1", a.unit.id)
			err = row.Scan(&dataBytes)
			if err != nil {
				return err
			}
		}
		result, err = bytesToMap(dataBytes)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

func (a *attempt) StartTime() (result time.Time, err error) {
	err = withTx(a, true, func(tx *sql.Tx) error {
		return tx.QueryRow("SELECT start_time FROM attempt WHERE id=$1", a.id).Scan(&result)
	})
	return
}

func (a *attempt) EndTime() (time.Time, error) {
	_ = withTx(a, false, func(tx *sql.Tx) error {
		return expireAttempts(a, tx)
	})

	var nt pq.NullTime
	err := withTx(a, true, func(tx *sql.Tx) error {
		return tx.QueryRow("SELECT end_time FROM attempt WHERE id=$1", a.id).Scan(&nt)
	})
	if err != nil {
		return time.Time{}, err
	}
	result := nullTimeToTime(nt)
	return result, nil
}

func (a *attempt) ExpirationTime() (result time.Time, err error) {
	_ = withTx(a, false, func(tx *sql.Tx) error {
		return expireAttempts(a, tx)
	})

	err = withTx(a, true, func(tx *sql.Tx) error {
		return tx.QueryRow("SELECT expiration_time FROM attempt WHERE id=$1", a.id).Scan(&result)
	})
	return
}

func (a *attempt) Renew(extendDuration time.Duration, data map[string]interface{}) error {
	// TODO(dmaze): check valid state and active status
	now := a.Coordinate().clock.Now()
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "expiration_time", now.Add(extendDuration))
	if data != nil {
		dataBytes, err := mapToBytes(data)
		if err != nil {
			return err
		}
		fields.Add(&params, "data", dataBytes)
	}
	query := buildUpdate(attemptTable, fields.UpdateChanges(), []string{
		isAttempt(&params, a.id),
	})
	return withTx(a, false, func(tx *sql.Tx) error {
		_, err := tx.Exec(query, params...)
		return err
	})
}

func (a *attempt) Expire(data map[string]interface{}) error {
	return withTx(a, false, func(tx *sql.Tx) error {
		return a.complete(tx, data, "expired")
	})
}

func (a *attempt) Finish(data map[string]interface{}) error {
	return withTx(a, false, func(tx *sql.Tx) error {
		err := a.complete(tx, data, "finished")
		if err != nil {
			return err
		}

		// Does the work unit data include an "output" key
		// that we understand?  We may need to ring back to
		// the work unit here; we need the next work spec name
		// too in any case
		outputs := []string{workSpecNextWorkSpec, workUnitAttempt}
		tables := []string{workSpecTable, workUnitTable}
		params := queryParams{}
		conditions := []string{isWorkUnit(&params, a.unit.id), workUnitInThisSpec}
		if data == nil {
			outputs = append(outputs, workUnitData, attemptData)
			tables = append(tables, attemptTable)
			conditions = append(conditions, attemptThisWorkUnit)
		}
		query := buildSelect(outputs, tables, conditions)

		row := tx.QueryRow(query, params...)
		var attemptID sql.NullInt64
		var nextWorkSpec string
		if data == nil {
			var unitData, attemptData []byte
			err = row.Scan(&nextWorkSpec, &attemptID, &unitData, &attemptData)
			if err == nil {
				if attemptData != nil {
					data, err = bytesToMap(attemptData)
				} else if unitData != nil {
					data, err = bytesToMap(unitData)
				} else {
					data = map[string]interface{}{}
				}
			}
		} else {
			err = row.Scan(&nextWorkSpec, &attemptID)
		}
		if err != nil {
			return err
		}
		if nextWorkSpec == "" {
			return nil // nothing to do
		}
		if !attemptID.Valid || (attemptID.Int64 != int64(a.id)) {
			return nil // no longer active attempt
		}

		// Ideally we'd extract the work spec ID in the previous
		// query with a join too; but this helps share some code
		spec := workSpec{
			namespace: a.unit.spec.namespace,
			name:      nextWorkSpec,
		}
		err = txWorkSpec(tx, &spec)
		if err != nil {
			if _, present := err.(coordinate.ErrNoSuchWorkSpec); !present {
				return nil // "then" work spec doesn't exist
			}
			return err // something else went wrong
		}

		units := coordinate.ExtractWorkUnitOutput(data["output"], a.Coordinate().clock.Now())
		if units == nil {
			return nil // nothing to do
		}
		for name, item := range units {
			var dataBytes []byte
			dataBytes, err = mapToBytes(item.Data)
			if err != nil {
				return err
			}
			_, err = spec.addWorkUnit(tx, name, dataBytes, item.Meta)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (a *attempt) Fail(data map[string]interface{}) error {
	return withTx(a, false, func(tx *sql.Tx) error {
		return a.complete(tx, data, "failed")
	})
}

func (a *attempt) Retry(data map[string]interface{}, delay time.Duration) error {
	return withTx(a, false, func(tx *sql.Tx) error {
		err := a.complete(tx, data, "retryable")
		if err == nil {
			// Also update the "not before" time on the work unit
			then := a.Coordinate().clock.Now().Add(delay)
			params := queryParams{}
			fields := fieldList{}
			fields.Add(&params, "not_before", then)
			query := buildUpdate(workUnitTable,
				fields.UpdateChanges(),
				[]string{
					isWorkUnit(&params, a.unit.id),
				})
			_, err = tx.Exec(query, params...)
		}
		return err
	})
}

func (a *attempt) complete(tx *sql.Tx, data map[string]interface{}, status string) error {
	// TODO(dmaze): check valid state transitions
	// TODO(dmaze): check if attempt is active if required

	// Mark the attempt as completed
	params := queryParams{}
	fields := fieldList{}
	fields.AddDirect("active", "FALSE")
	fields.Add(&params, "status", status)
	fields.Add(&params, "end_time", a.Coordinate().clock.Now())
	if data != nil {
		dataBytes, err := mapToBytes(data)
		if err != nil {
			return err
		}
		fields.Add(&params, "data", dataBytes)
	}
	query := buildUpdate(attemptTable, fields.UpdateChanges(), []string{
		isAttempt(&params, a.id),
	})
	_, err := tx.Exec(query, params...)
	if err != nil {
		return err
	}

	// If it was the active attempt, and this is a non-terminal
	// resolution, also reset that
	if status == "retryable" || status == "expired" {
		query = buildUpdate(workUnitTable, []string{
			"active_attempt_id=NULL",
		}, []string{
			"active_attempt_id=$1",
		})
		_, err = tx.Exec(query, a.id)
	}

	return err
}

// WorkUnit attempt functions

func (unit *workUnit) ActiveAttempt() (coordinate.Attempt, error) {
	_ = withTx(unit, false, func(tx *sql.Tx) error {
		return expireAttempts(unit, tx)
	})
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
	err := withTx(unit, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, unit.id).Scan(&a.id, &w.id, &w.name)
	})
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
	return withTx(unit, false, func(tx *sql.Tx) error {
		_, err := tx.Exec(query, unit.id)
		return err
	})
}

func (unit *workUnit) Attempts() ([]coordinate.Attempt, error) {
	params := queryParams{}
	query := buildSelect([]string{
		attemptID,
		workerID,
		workerName,
	}, []string{
		attemptTable,
		workerTable,
	}, []string{
		attemptForUnit(&params, unit.id),
		attemptThisWorker,
	})
	var result []coordinate.Attempt
	err := queryAndScan(unit, query, params, func(rows *sql.Rows) error {
		w := worker{namespace: unit.spec.namespace}
		a := attempt{worker: &w, unit: unit}
		err := rows.Scan(&a.id, &w.id, &w.name)
		if err == nil {
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
	var (
		attempts []coordinate.Attempt
		specs    map[string]*workSpec
		metas    map[string]*coordinate.WorkSpecMeta
		name     string
		err      error
		spec     *workSpec
		meta     *coordinate.WorkSpecMeta
	)

	// Run system-global expiry.
	_ = withTx(w, false, func(tx *sql.Tx) error {
		return expireAttempts(w, tx)
	})

	// Collect the set of candidate work specs and metadata outside
	// the main transaction.  This is pretty expensive to collect
	// and we want to avoid retrying it if possible.
	//
	// There is a possible race condition on a bad day.  It is
	// possible that this returns work specs with positive
	// available units, but while we're deciding what to do,
	// another worker picks those up.  That means the scheduler
	// could pick something but we then fail to get any work from
	// it.
	for {
		err = withTx(w, true, func(tx *sql.Tx) (err error) {
			specs, metas, err = w.namespace.allMetas(tx, true)
			return
		})
		if err != nil {
			return nil, err
		}

		// Now pick something (this is stateless, but see TODO above)
		// (If this picks nothing, we're done)
		metas = coordinate.LimitMetasToNames(metas, req.WorkSpecs)
		metas = coordinate.LimitMetasToRuntimes(metas, req.Runtimes)
		now := w.Coordinate().clock.Now()
		name, err = coordinate.SimplifiedScheduler(metas, now, req.AvailableGb)
		if err == coordinate.ErrNoWork {
			return attempts, nil
		} else if err != nil {
			return nil, err
		}
		spec = specs[name]
		meta = metas[name]

		// Then get some attempts
		attempts, err = w.requestAttemptsForSpec(req, spec, meta)
		if err != nil {
			return nil, err
		}

		// If that returned non-zero attempts, we're done
		if len(attempts) > 0 {
			return attempts, nil
		}
		// Otherwise reloop
	}
}

func (w *worker) requestAttemptsForSpec(req coordinate.AttemptRequest, spec *workSpec, meta *coordinate.WorkSpecMeta) ([]coordinate.Attempt, error) {
	var (
		attempts []coordinate.Attempt
		count    int
		err      error
	)

	// Adjust the work unit count based on what's possible here
	count = req.NumberOfWorkUnits
	if count < 1 {
		count = 1
	}
	if meta.MaxAttemptsReturned > 0 && count > meta.MaxAttemptsReturned {
		count = meta.MaxAttemptsReturned
	}
	if meta.MaxRunning > 0 && count > meta.MaxRunning-meta.PendingCount {
		count = meta.MaxRunning - meta.PendingCount
	}

	// Now choose units and create attempts
	err = withTx(w, false, func(tx *sql.Tx) error {
		now := w.Coordinate().clock.Now()
		units, err := w.chooseWorkUnits(tx, spec, count, now)
		if err != nil {
			return err
		}
		if len(units) == 0 && meta.CanStartContinuous(now) {
			units, err = w.createContinuousUnits(tx, spec, meta, now)
		}
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

// chooseWorkUnits chooses up to a specified number of work units from
// some work spec.
func (w *worker) chooseWorkUnits(tx *sql.Tx, spec *workSpec, numUnits int, now time.Time) ([]*workUnit, error) {
	params := queryParams{}
	query := buildSelect([]string{
		workUnitID,
		workUnitName,
	}, []string{
		workUnitTable,
	}, []string{
		workUnitInSpec(&params, spec.id),
		workUnitHasNoAttempt,
		"NOT " + workUnitTooSoon(&params, now),
	})
	query += " ORDER BY priority DESC, name ASC"
	query += fmt.Sprintf(" LIMIT %v", numUnits)
	query += " FOR UPDATE"
	rows, err := tx.Query(query, params...)
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

// createContinuousUnits tries to create exactly one continuous work
// unit, and returns it.
func (w *worker) createContinuousUnits(tx *sql.Tx, spec *workSpec, meta *coordinate.WorkSpecMeta, now time.Time) ([]*workUnit, error) {
	// We will want to ensure that only one worker is attempting
	// to create this work unit, and we will ultimately want to
	// update the next-continuous time
	params := queryParams{}
	row := tx.QueryRow(buildSelect([]string{workSpecNextContinuous},
		[]string{workSpecTable},
		[]string{isWorkSpec(&params, spec.id)}),
		params...)
	var aTime pq.NullTime
	err := row.Scan(&aTime)
	if err != nil {
		return nil, err
	}

	// Create the work unit
	seconds := now.Unix()
	nano := now.Nanosecond()
	milli := nano / 1000000
	name := fmt.Sprintf("%d.%03d", seconds, milli)
	dataBytes, err := mapToBytes(map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	unit, err := spec.addWorkUnit(tx, name, dataBytes, coordinate.WorkUnitMeta{})
	if err != nil {
		return nil, err
	}

	// Update the next-continuous time for the work spec.
	params = queryParams{}
	fields := fieldList{}
	fields.Add(&params, "next_continuous", now.Add(meta.Interval))
	res, err := tx.Exec(buildUpdate(workSpecTable,
		fields.UpdateChanges(),
		[]string{isWorkSpec(&params, spec.id)}),
		params...)
	if err != nil {
		return nil, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows != 1 {
		return nil, fmt.Errorf("update work spec next continuous changed %v rows (not 1)", rows)
	}

	// We have the single work unit we want to do.
	return []*workUnit{unit}, nil
}

func (w *worker) MakeAttempt(cUnit coordinate.WorkUnit, length time.Duration) (coordinate.Attempt, error) {
	unit, ok := cUnit.(*workUnit)
	if !ok {
		return nil, coordinate.ErrWrongBackend
	}
	var a *attempt
	var err error
	err = withTx(w, false, func(tx *sql.Tx) error {
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

	now := a.Coordinate().clock.Now()
	expiration := now.Add(length)
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "work_unit_id", unit.id)
	fields.Add(&params, "worker_id", w.id)
	fields.Add(&params, "start_time", now)
	fields.Add(&params, "expiration_time", expiration)
	query := fields.InsertStatement(attemptTable) + " RETURNING id"
	row := tx.QueryRow(query, params...)
	err := row.Scan(&a.id)
	if err != nil {
		return nil, err
	}

	params = queryParams{}
	fields = fieldList{}
	fields.Add(&params, "active_attempt_id", a.id)
	query = buildUpdate(workUnitTable, fields.UpdateChanges(), []string{
		isWorkUnit(&params, unit.id),
	})
	_, err = tx.Exec(query, params...)

	return &a, err
}

func (w *worker) ActiveAttempts() ([]coordinate.Attempt, error) {
	qp := queryParams{}
	return w.findAttempts([]string{
		attemptByWorker(&qp, w.id),
		attemptIsActive,
	}, &qp, false)
}

func (w *worker) AllAttempts() ([]coordinate.Attempt, error) {
	qp := queryParams{}
	return w.findAttempts([]string{
		attemptByWorker(&qp, w.id),
	}, &qp, false)
}

func (w *worker) ChildAttempts() ([]coordinate.Attempt, error) {
	qp := queryParams{}
	return w.findAttempts([]string{
		attemptThisWorker,
		attemptIsActive,
		workerHasParent(&qp, w.id),
	}, &qp, true)
}

func (w *worker) findAttempts(conditions []string, qp *queryParams, forOtherWorkers bool) ([]coordinate.Attempt, error) {
	outputs := []string{
		attemptID,
		workUnitID,
		workUnitName,
		workSpecID,
		workSpecName,
	}
	tables := []string{
		attemptTable,
		workUnitTable,
		workSpecTable,
	}
	conditions = append(conditions,
		attemptThisWorkUnit,
		workUnitInThisSpec,
	)
	if forOtherWorkers {
		outputs = append(outputs, workerID, workerName)
		tables = append(tables, workerTable)
		conditions = append(conditions, attemptThisWorker)
	}
	query := buildSelect(outputs, tables, conditions)
	var result []coordinate.Attempt
	err := queryAndScan(w, query, *qp, func(rows *sql.Rows) error {
		spec := workSpec{namespace: w.namespace}
		unit := workUnit{spec: &spec}
		a := attempt{worker: w, unit: &unit}
		theWorker := worker{namespace: w.namespace}
		var err error
		if forOtherWorkers {
			a.worker = &theWorker
			err = rows.Scan(&a.id,
				&unit.id, &unit.name,
				&spec.id, &spec.name,
				&theWorker.id, &theWorker.name)
		} else {
			err = rows.Scan(&a.id,
				&unit.id, &unit.name,
				&spec.id, &spec.name)
		}
		if err == nil {
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

// expireAttempts finds all attempts whose expiration time has passed
// and expires them.  It runs on all attempts for all work units in all
// work specs in all namespaces (which simplifies the query).  Expired
// attempts' statuses become "expired", and those attempts cease to be
// the active attempt for their corresponding work unit.
//
// In general this should be called in its own transaction and its error
// return ignored:
//
//     _ = withTx(self, false, func(tx *sql.Tx) error {
//              return expireAttempts(self, tx)
//     })
//
// Expiry is generally secondary to whatever actual work is going on.
// If a result is different because of expiry, pretend the relevant
// call was made a second earlier or later.  If this fails, then
// either there is a concurrency issue (and since the query is
// system-global, the other expirer will clean up for us) or there is
// an operational error (and the caller will fail afterwards).
func expireAttempts(c coordinable, tx *sql.Tx) error {
	// There are several places this is called with much smaller
	// scope.  For instance, Attempt.Status() needs to invoke
	// expiry but only actually cares about this very specific
	// attempt.  If there are multiple namespaces,
	// Worker.RequestAttempts() only cares about this namespace
	// (though it will run on all work specs).  It may help system
	// performance to try to run this with narrower scope.
	//
	// This is probably also an excellent candidate for a stored
	// procedure.
	var (
		now        time.Time
		cte, query string
		count      int64
		result     sql.Result
		err        error
	)

	now = c.Coordinate().clock.Now()

	// Remove expiring attempts from their work unit
	qp := queryParams{}
	cte = buildSelect([]string{
		attemptID,
	}, []string{
		attemptTable,
	}, []string{
		attemptIsPending,
		attemptIsExpired(&qp, now),
	})
	query = buildUpdate(workUnitTable,
		[]string{"active_attempt_id=NULL"},
		[]string{"active_attempt_id IN (" + cte + ")"})
	result, err = tx.Exec(query, qp...)
	if err != nil {
		return err
	}

	// If this marked nothing as expired, we're done
	count, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

	// Mark attempts as expired
	qp = queryParams{}
	// A slightly exotic setup, since we want to reuse the "now"
	// param
	dollarsNow := qp.Param(now)
	fields := fieldList{}
	fields.AddDirect("expiration_time", dollarsNow)
	fields.AddDirect("status", "'expired'")
	query = buildUpdate(attemptTable, fields.UpdateChanges(), []string{
		attemptIsPending,
		attemptExpirationTime + "<" + dollarsNow,
	})
	_, err = tx.Exec(query, qp...)
	return err
}
