// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

type worker struct {
	namespace *namespace
	id        int
	name      string
}

func (ns *namespace) Worker(name string) (coordinate.Worker, error) {
	worker := worker{name: name, namespace: ns}
	err := withTx(ns, false, func(tx *sql.Tx) error {
		params := queryParams{}
		query := buildSelect([]string{
			workerID,
		}, []string{
			workerTable,
		}, []string{
			workerInNamespace(&params, ns.id),
			workerHasName(&params, name),
		})
		err := tx.QueryRow(query, params...).Scan(&worker.id)
		if err == sql.ErrNoRows {
			now := ns.Coordinate().clock.Now()
			expiration := now.Add(time.Duration(15) * time.Minute)
			params = queryParams{}
			fields := fieldList{}
			fields.Add(&params, "namespace_id", ns.id)
			fields.Add(&params, "name", name)
			fields.AddDirect("active", "TRUE")
			fields.AddDirect("mode", "''")
			fields.Add(&params, "data", []byte{})
			fields.Add(&params, "expiration", expiration)
			fields.Add(&params, "last_update", now)
			query = fields.InsertStatement(workerTable) + " RETURNING id"
			err = tx.QueryRow(query, params...).Scan(&worker.id)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &worker, nil
}

func (ns *namespace) Workers() (map[string]coordinate.Worker, error) {
	result := make(map[string]coordinate.Worker)
	params := queryParams{}
	query := buildSelect([]string{
		workerID,
		workerName,
	}, []string{
		workerTable,
	}, []string{
		workerInNamespace(&params, ns.id),
	})
	err := queryAndScan(ns, query, params, func(rows *sql.Rows) error {
		w := worker{namespace: ns}
		err := rows.Scan(&w.id, &w.name)
		if err == nil {
			result[w.name] = &w
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// coordinate.Worker interface

func (w *worker) Name() string {
	return w.name
}

func (w *worker) Parent() (coordinate.Worker, error) {
	parent := worker{namespace: w.namespace}
	params := queryParams{}
	query := buildSelect([]string{
		"parent.id",
		"parent.name",
	}, []string{
		workerTable + " child",
		workerTable + " parent",
	}, []string{
		"parent.id=child.parent",
		"child.id=" + params.Param(w.id),
	})
	err := withTx(w, true, func(tx *sql.Tx) error {
		return tx.QueryRow(query, params...).Scan(&parent.id, &parent.name)
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &parent, nil
}

func (w *worker) SetParent(cParent coordinate.Worker) error {
	parent, ok := cParent.(*worker)
	if !ok {
		return coordinate.ErrWrongBackend
	}
	return withTx(w, false, func(tx *sql.Tx) (err error) {
		if parent == nil {
			_, err = tx.Exec("UPDATE worker SET parent=NULL WHERE id=$1", w.id)
		} else {
			_, err = tx.Exec("UPDATE worker SET parent=$2 WHERE id=$1", w.id, parent.id)
		}
		return
	})
}

func (w *worker) Children() ([]coordinate.Worker, error) {
	params := queryParams{}
	query := buildSelect([]string{
		workerID,
		workerName,
	}, []string{
		workerTable,
	}, []string{
		workerHasParent(&params, w.id),
	})
	var result []coordinate.Worker
	err := queryAndScan(w, query, params, func(rows *sql.Rows) error {
		child := worker{namespace: w.namespace}
		err := rows.Scan(&child.id, &child.name)
		if err == nil {
			result = append(result, &child)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (w *worker) Active() (result bool, err error) {
	err = withTx(w, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT active FROM worker WHERE id=$1", w.id)
		return row.Scan(&result)
	})
	return
}

func (w *worker) Deactivate() error {
	return withTx(w, false, func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE worker SET active=FALSE WHERE id=$1", w.id)
		return err
	})
}

func (w *worker) Mode() (result string, err error) {
	err = withTx(w, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT mode FROM worker WHERE id=$1", w.id)
		return row.Scan(&result)
	})
	return
}

func (w *worker) Data() (map[string]interface{}, error) {
	var dataBytes []byte
	err := withTx(w, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT data FROM worker WHERE id=$1", w.id)
		return row.Scan(&dataBytes)
	})
	if err != nil {
		return nil, err
	}
	if len(dataBytes) == 0 {
		return nil, nil
	}
	result, err := bytesToMap(dataBytes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (w *worker) Expiration() (result time.Time, err error) {
	err = withTx(w, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT expiration FROM worker WHERE id=$1", w.id)
		return row.Scan(&result)
	})
	return
}

func (w *worker) LastUpdate() (result time.Time, err error) {
	err = withTx(w, true, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT last_update FROM worker WHERE id=$1", w.id)
		return row.Scan(&result)
	})
	return
}

func (w *worker) Update(data map[string]interface{}, now, expiration time.Time, mode string) error {
	dataBytes, err := mapToBytes(data)
	if err != nil {
		return err
	}
	params := queryParams{}
	fields := fieldList{}
	fields.AddDirect("active", "TRUE")
	fields.Add(&params, "mode", mode)
	fields.Add(&params, "data", dataBytes)
	fields.Add(&params, "expiration", expiration)
	fields.Add(&params, "last_update", now)
	query := buildUpdate(workerTable, fields.UpdateChanges(), []string{
		isWorker(&params, w.id),
	})
	return execInTx(w, query, params)
}

// coordinable interface

func (w *worker) Coordinate() *pgCoordinate {
	return w.namespace.coordinate
}
