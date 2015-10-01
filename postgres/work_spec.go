package postgres

import (
	"database/sql"
	"github.com/dmaze/goordinate/coordinate"
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
	err = withTx(ns, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT work_spec.id FROM work_spec WHERE namespace_id=$1 AND name=$2", ns.id, name)
		err = row.Scan(&spec.id)
		if err == nil {
			err = spec.setData(tx, data, meta)
		} else if err == sql.ErrNoRows {
			var dataGob []byte
			dataGob, err = mapToGob(data)
			if err != nil {
				return err
			}
			interval := durationToSQL(meta.Interval)
			nextContinuous := timeToNullTime(meta.NextContinuous)
			row = tx.QueryRow("INSERT INTO work_spec(namespace_id, name, data, priority, weight, paused, continuous, can_be_continuous, interval, next_continuous, max_running, max_attempts_returned, next_work_spec_name, next_work_spec_preempts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING id", ns.id, name, dataGob, meta.Priority, meta.Weight, meta.Paused, meta.Continuous, meta.CanBeContinuous, interval, nextContinuous, meta.MaxRunning, meta.MaxAttemptsReturned, meta.NextWorkSpecName, meta.NextWorkSpecPreempts)
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
	row := theDB(ns).QueryRow("SELECT work_spec.id FROM work_spec WHERE namespace_id=$1 AND name=$2", ns.id, name)
	err := row.Scan(&spec.id)
	if err == sql.ErrNoRows {
		return nil, coordinate.ErrNoSuchWorkSpec{Name: name}
	}
	if err != nil {
		return nil, err
	}
	return &spec, nil
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	_, err := theDB(ns).Exec("DELETE FROM work_spec WHERE namespace_id=$1 AND name=$2", ns.id, name)
	return err
}

func (ns *namespace) WorkSpecNames() ([]string, error) {
	rows, err := theDB(ns).Query("SELECT name FROM work_spec WHERE namespace_id=$1", ns.id)
	if err != nil {
		return nil, err
	}
	var result []string
	err = scanRows(rows, func() error {
		var name string
		if err := rows.Scan(&name); err == nil {
			result = append(result, name)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// WorkSpec functions:

func (spec *workSpec) Name() string {
	return spec.name
}

func (spec *workSpec) Data() (map[string]interface{}, error) {
	row := theDB(spec).QueryRow("SELECT data FROM work_spec WHERE id=$1", spec.id)
	var dataGob []byte
	err := row.Scan(&dataGob)
	if err != nil {
		return nil, err
	}
	data, err := gobToMap(dataGob)
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
	return withTx(spec, func(tx *sql.Tx) error {
		return spec.setData(tx, data, meta)
	})
}

func (spec *workSpec) setData(tx *sql.Tx, data map[string]interface{}, meta coordinate.WorkSpecMeta) error {
	dataGob, err := mapToGob(data)
	if err != nil {
		return err
	}

	interval := durationToSQL(meta.Interval)
	nextContinuous := timeToNullTime(meta.NextContinuous)
	_, err = tx.Exec("UPDATE work_spec SET data=$2, priority=$3, weight=$4, paused=$5, continuous=$6, can_be_continuous=$7, interval=$8, next_continuous=$9, max_running=$10, max_attempts_returned=$11, next_work_spec_name=$12, next_work_spec_preempts=$13 WHERE id=$1", spec.id, dataGob, meta.Priority, meta.Weight, meta.Paused, meta.Continuous, meta.CanBeContinuous, interval, nextContinuous, meta.MaxRunning, meta.MaxAttemptsReturned, meta.NextWorkSpecName, meta.NextWorkSpecPreempts)
	return err
}

func (spec *workSpec) Meta(withCounts bool) (coordinate.WorkSpecMeta, error) {
	var (
		meta           coordinate.WorkSpecMeta
		interval       string
		nextContinuous pq.NullTime
	)
	row := theDB(spec).QueryRow("SELECT priority, weight, paused, continuous, can_be_continuous, interval, next_continuous, max_running, max_attempts_returned, next_work_spec_name, next_work_spec_preempts FROM work_spec WHERE id=$1", spec.id)
	err := row.Scan(&meta.Priority, &meta.Weight, &meta.Paused, &meta.Continuous, &meta.CanBeContinuous, &interval, &nextContinuous, &meta.MaxRunning, &meta.MaxAttemptsReturned, &meta.NextWorkSpecName, &meta.NextWorkSpecPreempts)
	if err == nil {
		meta.NextContinuous = nullTimeToTime(nextContinuous)
		meta.Interval, err = sqlToDuration(interval)
	}
	// TODO(dmaze): do a second SELECT if withCounts is true
	return meta, err
}

// AllMetas retrieves the metadata for all work specs.  This is
// expected to run within a pre-existing transaction.  On success,
// returns maps from work spec name to work spec object and to
// metadata object.
func (ns *namespace) allMetas(tx *sql.Tx, withCounts bool) (map[string]*workSpec, map[string]*coordinate.WorkSpecMeta, error) {
	query := buildSelect([]string{
		workSpecID,
		workSpecName,
		"priority",
		"weight",
		"paused",
		"continuous",
		"can_be_continuous",
		"interval",
		"next_continuous",
		"max_running",
		"max_attempts_returned",
		"next_work_spec_name",
		"next_work_spec_preempts",
	}, []string{
		workSpecTable,
	}, []string{
		workSpecNamespace + "=$1",
	})
	rows, err := tx.Query(query, ns.id)
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
			&meta.CanBeContinuous, &interval, &nextContinuous,
			&meta.MaxRunning, &meta.MaxAttemptsReturned,
			&meta.NextWorkSpecName, &meta.NextWorkSpecPreempts)
		specs[spec.name] = &spec
		metas[spec.name] = &meta
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if withCounts {
		query = buildSelect([]string{
			workSpecName,
			attemptStatus,
			"COUNT(*)",
		}, []string{
			workSpecTable,
			workUnitAttemptJoin,
		}, []string{
			inThisNamespace, // binds $1
			workUnitInSpec,
			// We ignore finished/failed status; is it
			// worthwhile to ignore those here?
		})
		query += " GROUP BY " + workSpecName + ", " + attemptStatus
		rows, err = tx.Query(query, ns.id)
		if err != nil {
			return nil, nil, err
		}
		err = scanRows(rows, func() error {
			var name string
			var status sql.NullString
			var count int
			err := rows.Scan(&name, &status, &count)
			if err != nil {
				return err
			}
			if !status.Valid {
				metas[name].AvailableCount += count
			} else {
				switch status.String {
				case "expired":
					metas[name].AvailableCount += count
				case "retryable":
					metas[name].AvailableCount += count
				case "pending":
					metas[name].PendingCount += count
				}
			}
			return nil
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
	_, err := theDB(spec).Exec("UPDATE work_spec SET priority=$2, weight=$3, paused=$4, continuous=$5 AND can_be_continuous, interval=$6, next_continuous=$7, max_running=$8, max_attempts_returned=$9 WHERE id=$1", spec.id, meta.Priority, meta.Weight, meta.Paused, meta.Continuous, durationToSQL(meta.Interval), timeToNullTime(meta.NextContinuous), meta.MaxRunning, meta.MaxAttemptsReturned)
	return err
}

// coordinable interface:

func (spec *workSpec) Coordinate() *pgCoordinate {
	return spec.namespace.coordinate
}