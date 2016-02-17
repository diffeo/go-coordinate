// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/lib/pq"
)

type workSpec struct {
	namespace *namespace
	id        int
	name      string
}

// Namespace based functions:

func (ns *namespace) SetWorkSpec(data map[string]interface{}) (coordinate.WorkSpec, error) {
	name, meta, err := coordinate.ExtractWorkSpecMeta(data)
	if err != nil {
		return nil, err
	}

	spec := workSpec{
		namespace: ns,
		name:      name,
	}
	err = withTx(ns, false, func(tx *sql.Tx) error {
		params := queryParams{}
		query := buildSelect([]string{
			workSpecID,
		}, []string{
			workSpecTable,
		}, []string{
			workSpecInNamespace(&params, ns.id),
			workSpecHasName(&params, name),
		})
		row := tx.QueryRow(query, params...)
		err = row.Scan(&spec.id)
		if err == nil {
			err = spec.setData(tx, data, meta)
		} else if err == sql.ErrNoRows {
			var dataBytes []byte
			dataBytes, err = mapToBytes(data)
			if err != nil {
				return err
			}
			params = queryParams{}
			fields := fieldList{}
			fields.Add(&params, "namespace_id", ns.id)
			fields.Add(&params, "name", name)
			fields.Add(&params, "data", dataBytes)
			fields.Add(&params, "priority", meta.Priority)
			fields.Add(&params, "weight", meta.Weight)
			fields.Add(&params, "paused", meta.Paused)
			fields.Add(&params, "continuous", meta.Continuous)
			fields.Add(&params, "can_be_continuous", meta.CanBeContinuous)
			fields.Add(&params, "min_memory_gb", meta.MinMemoryGb)
			fields.Add(&params, "interval", durationToSQL(meta.Interval))
			fields.Add(&params, "next_continuous", timeToNullTime(meta.NextContinuous))
			fields.Add(&params, "max_running", meta.MaxRunning)
			fields.Add(&params, "max_attempts_returned", meta.MaxAttemptsReturned)
			fields.Add(&params, "next_work_spec_name", meta.NextWorkSpecName)
			fields.AddDirect("next_work_spec_preempts", "FALSE")
			fields.Add(&params, "runtime", meta.Runtime)
			query = fields.InsertStatement(workSpecTable) + "RETURNING id"
			row = tx.QueryRow(query, params...)
			err = row.Scan(&spec.id)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &spec, nil
}

func (ns *namespace) WorkSpec(name string) (coordinate.WorkSpec, error) {
	spec := workSpec{
		namespace: ns,
		name:      name,
	}
	err := withTx(ns, true, func(tx *sql.Tx) error {
		return txWorkSpec(tx, &spec)
	})
	if err != nil {
		return nil, err
	}
	return &spec, nil
}

// txWorkSpec retrieves a work spec within the context of an existing
// transaction, possibly read-only.  The workSpec object must be
// populated with its "namespace" and "name" fields.
func txWorkSpec(tx *sql.Tx, spec *workSpec) error {
	params := queryParams{}
	row := tx.QueryRow(buildSelect([]string{
		workSpecID,
	}, []string{
		workSpecTable,
	}, []string{
		workSpecInNamespace(&params, spec.namespace.id),
		workSpecHasName(&params, spec.name),
	}), params...)
	err := row.Scan(&spec.id)
	if err == sql.ErrNoRows {
		return coordinate.ErrNoSuchWorkSpec{Name: spec.name}
	}
	return err
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	params := queryParams{}
	query := "DELETE FROM " + workSpecTable + " " +
		"WHERE " + workSpecInNamespace(&params, ns.id) + " " +
		"AND " + workSpecHasName(&params, name)
	err := execInTx(ns, query, params, true)
	if err == coordinate.ErrGone {
		err = coordinate.ErrNoSuchWorkSpec{Name: name}
	}
	return err
}

func (ns *namespace) WorkSpecNames() (result []string, err error) {
	params := queryParams{}
	query := buildSelect([]string{
		workSpecName,
	}, []string{
		workSpecTable,
	}, []string{
		workSpecInNamespace(&params, ns.id),
	})
	err = queryAndScan(ns, query, params, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err == nil {
			result = append(result, name)
		}
		return err
	})
	return
}

// WorkSpec functions:

func (spec *workSpec) Name() string {
	return spec.name
}

func (spec *workSpec) Data() (map[string]interface{}, error) {
	var dataBytes []byte
	err := withTx(spec, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT data FROM work_spec WHERE id=$1", spec.id)
		return row.Scan(&dataBytes)
	})
	if err == sql.ErrNoRows {
		return nil, coordinate.ErrGone
	}
	if err != nil {
		return nil, err
	}
	data, err := bytesToMap(dataBytes)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (spec *workSpec) SetData(data map[string]interface{}) error {
	name, meta, err := coordinate.ExtractWorkSpecMeta(data)
	if err != nil {
		return err
	}
	if name != spec.name {
		return coordinate.ErrChangedName
	}
	return withTx(spec, false, func(tx *sql.Tx) error {
		return spec.setData(tx, data, meta)
	})
}

func (spec *workSpec) setData(tx *sql.Tx, data map[string]interface{}, meta coordinate.WorkSpecMeta) error {
	dataBytes, err := mapToBytes(data)
	if err != nil {
		return err
	}

	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "data", dataBytes)
	fields.Add(&params, "priority", meta.Priority)
	fields.Add(&params, "weight", meta.Weight)
	fields.Add(&params, "paused", meta.Paused)
	fields.Add(&params, "continuous", meta.Continuous)
	fields.Add(&params, "can_be_continuous", meta.CanBeContinuous)
	fields.Add(&params, "min_memory_gb", meta.MinMemoryGb)
	fields.Add(&params, "interval", durationToSQL(meta.Interval))
	fields.Add(&params, "next_continuous", timeToNullTime(meta.NextContinuous))
	fields.Add(&params, "max_running", meta.MaxRunning)
	fields.Add(&params, "max_attempts_returned", meta.MaxAttemptsReturned)
	fields.Add(&params, "next_work_spec_name", meta.NextWorkSpecName)
	fields.AddDirect("next_work_spec_preempts", "FALSE")
	fields.Add(&params, "runtime", meta.Runtime)
	query := buildUpdate(workSpecTable, fields.UpdateChanges(), []string{
		isWorkSpec(&params, spec.id),
	})
	return execInTx(spec, query, params, true)
}

func (spec *workSpec) Meta(withCounts bool) (coordinate.WorkSpecMeta, error) {
	// If we need counts, we need to run expiry so that the
	// available/pending counts are rightish
	if withCounts {
		spec.Coordinate().Expiry.Do(spec)
	}
	var meta coordinate.WorkSpecMeta
	err := withTx(spec, true, func(tx *sql.Tx) error {
		var (
			params         queryParams
			query          string
			interval       string
			nextContinuous pq.NullTime
		)
		query = buildSelect([]string{
			workSpecPriority,
			workSpecWeight,
			workSpecPaused,
			workSpecContinuous,
			workSpecCanBeContinuous,
			workSpecMinMemoryGb,
			workSpecInterval,
			workSpecNextContinuous,
			workSpecMaxRunning,
			workSpecMaxAttemptsReturned,
			workSpecNextWorkSpec,
			workSpecRuntime,
		}, []string{
			workSpecTable,
		}, []string{
			isWorkSpec(&params, spec.id),
		})
		row := tx.QueryRow(query, params...)
		err := row.Scan(
			&meta.Priority,
			&meta.Weight,
			&meta.Paused,
			&meta.Continuous,
			&meta.CanBeContinuous,
			&meta.MinMemoryGb,
			&interval,
			&nextContinuous,
			&meta.MaxRunning,
			&meta.MaxAttemptsReturned,
			&meta.NextWorkSpecName,
			&meta.Runtime,
		)
		if err == sql.ErrNoRows {
			return coordinate.ErrGone
		}
		if err != nil {
			return err
		}
		meta.NextContinuous = nullTimeToTime(nextContinuous)
		meta.Interval, err = sqlToDuration(interval)
		if err != nil {
			return err
		}

		// Find counts with a second query, if requested
		if !withCounts {
			return nil
		}
		params = queryParams{}
		query = buildSelect([]string{
			attemptStatus,
			"COUNT(*)",
		}, []string{
			workUnitAttemptJoin,
		}, []string{
			workUnitInSpec(&params, spec.id),
		})
		query += " GROUP BY " + attemptStatus
		rows, err := tx.Query(query, params...)
		if err != nil {
			return err
		}
		return scanRows(rows, func() error {
			var status sql.NullString
			var count int
			err := rows.Scan(&status, &count)
			if err != nil {
				return err
			}
			if !status.Valid {
				meta.AvailableCount += count
			} else {
				switch status.String {
				case "expired":
					meta.AvailableCount += count
				case "retryable":
					meta.AvailableCount += count
				case "pending":
					meta.PendingCount += count
				}
			}
			return nil
		})
	})
	return meta, err
}

// AllMetas retrieves the metadata for all work specs.  This is
// expected to run within a pre-existing transaction.  On success,
// returns maps from work spec name to work spec object and to
// metadata object.
func (ns *namespace) allMetas(tx *sql.Tx, withCounts bool) (map[string]*workSpec, map[string]*coordinate.WorkSpecMeta, error) {
	params := queryParams{}
	query := buildSelect([]string{
		workSpecID,
		workSpecName,
		workSpecPriority,
		workSpecWeight,
		workSpecPaused,
		workSpecContinuous,
		workSpecCanBeContinuous,
		workSpecMinMemoryGb,
		workSpecInterval,
		workSpecNextContinuous,
		workSpecMaxRunning,
		workSpecMaxAttemptsReturned,
		workSpecNextWorkSpec,
		workSpecRuntime,
	}, []string{
		workSpecTable,
	}, []string{
		workSpecInNamespace(&params, ns.id),
	})
	rows, err := tx.Query(query, params...)
	if err != nil {
		return nil, nil, err
	}
	specs := make(map[string]*workSpec)
	metas := make(map[string]*coordinate.WorkSpecMeta)
	err = scanRows(rows, func() error {
		var (
			spec           workSpec
			meta           coordinate.WorkSpecMeta
			interval       string
			nextContinuous pq.NullTime
			err            error
		)
		err = rows.Scan(&spec.id, &spec.name, &meta.Priority,
			&meta.Weight, &meta.Paused, &meta.Continuous,
			&meta.CanBeContinuous, &meta.MinMemoryGb,
			&interval, &nextContinuous, &meta.MaxRunning,
			&meta.MaxAttemptsReturned,
			&meta.NextWorkSpecName, &meta.Runtime)
		if err != nil {
			return err
		}
		spec.namespace = ns
		meta.NextContinuous = nullTimeToTime(nextContinuous)
		meta.Interval, err = sqlToDuration(interval)
		if err != nil {
			return err
		}
		specs[spec.name] = &spec
		metas[spec.name] = &meta
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if withCounts {
		// A single query that selects both "available" and
		// "pending" is hopelessly expensive.  Also, in the
		// only place this is called (in RequestAttempts) we
		// need to know whether or not there are any available
		// attempts, but we don't really care how many there
		// are so long as there are more than zero.
		//
		// Pending:
		params = queryParams{}
		query = buildSelect([]string{workSpecName, "COUNT(*)"},
			[]string{workSpecTable, attemptTable},
			[]string{
				workSpecInNamespace(&params, ns.id),
				attemptInThisSpec,
				attemptIsPending,
			})
		query += " GROUP BY " + workSpecName
		rows, err = tx.Query(query, params...)
		if err != nil {
			return nil, nil, err
		}
		err = scanRows(rows, func() error {
			var name string
			var count int
			err := rows.Scan(&name, &count)
			if err == nil {
				metas[name].PendingCount = count
			}
			return err
		})

		// Available count (0/1):
		now := ns.Coordinate().clock.Now()
		params = queryParams{}
		query = buildSelect([]string{
			workUnitSpec,
		}, []string{
			workUnitTable,
		}, []string{
			workUnitHasNoAttempt,
			"NOT " + workUnitTooSoon(&params, now),
		})
		query = buildSelect([]string{
			workSpecName,
		}, []string{
			workSpecTable,
		}, []string{
			workSpecInNamespace(&params, ns.id),
			workSpecID + " IN (" + query + ")",
		})
		rows, err = tx.Query(query, params...)
		err = scanRows(rows, func() error {
			var name string
			err := rows.Scan(&name)
			if err == nil {
				metas[name].AvailableCount = 1
			}
			return err
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return specs, metas, nil
}

func (spec *workSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	// There are a couple of fields we can't set; in this implementation
	// we can just not update them and be done with it.
	params := queryParams{}
	fields := fieldList{}
	fields.Add(&params, "priority", meta.Priority)
	fields.Add(&params, "weight", meta.Weight)
	fields.Add(&params, "paused", meta.Paused)
	fields.AddDirect("continuous", params.Param(meta.Continuous)+" AND can_be_continuous")
	fields.Add(&params, "min_memory_gb", meta.MinMemoryGb)
	fields.Add(&params, "interval", durationToSQL(meta.Interval))
	fields.Add(&params, "next_continuous", timeToNullTime(meta.NextContinuous))
	fields.Add(&params, "max_running", meta.MaxRunning)
	fields.Add(&params, "max_attempts_returned", meta.MaxAttemptsReturned)
	query := buildUpdate(workSpecTable, fields.UpdateChanges(), []string{
		isWorkSpec(&params, spec.id),
	})
	return execInTx(spec, query, params, true)
}

// coordinable interface:

func (spec *workSpec) Coordinate() *pgCoordinate {
	return spec.namespace.coordinate
}
