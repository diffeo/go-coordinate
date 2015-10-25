package postgres

import (
	"database/sql"
	"github.com/dmaze/goordinate/coordinate"
	"time"
)

type worker struct {
	namespace *namespace
	id        int
	name      string
}

func (ns *namespace) Worker(name string) (coordinate.Worker, error) {
	worker := worker{name: name, namespace: ns}
	err := withTx(ns, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT id FROM worker WHERE namespace_id=$1 AND name=$2", ns.id, name)
		err := row.Scan(&worker.id)
		if err == sql.ErrNoRows {
			now := ns.Coordinate().clock.Now()
			expiration := now.Add(time.Duration(15) * time.Minute)
			row = tx.QueryRow("INSERT INTO worker(namespace_id, name, active, mode, data, expiration, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id", ns.id, name, true, "", []byte{}, expiration, now)
			err = row.Scan(&worker.id)
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
	rows, err := theDB(ns).Query("SELECT id, name FROM worker WHERE namespace_id=$1", ns.id)
	if err != nil {
		return nil, err
	}
	err = scanRows(rows, func() error {
		w := worker{namespace: ns}
		if err := rows.Scan(&w.id, &w.name); err == nil {
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
	row := theDB(w).QueryRow("SELECT parent.id, parent.name FROM worker child, worker parent WHERE parent.id=child.parent AND child.id=$1", w.id)
	err := row.Scan(&parent.id, &parent.name)
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
	return withTx(w, func(tx *sql.Tx) (err error) {
		if parent == nil {
			_, err = tx.Exec("UPDATE worker SET parent=NULL WHERE id=$1", w.id)
		} else {
			_, err = tx.Exec("UPDATE worker SET parent=$2 WHERE id=$1", w.id, parent.id)
		}
		return
	})
}

func (w *worker) Children() ([]coordinate.Worker, error) {
	rows, err := theDB(w).Query("SELECT id, name FROM worker WHERE parent=$1", w.id)
	if err != nil {
		return nil, err
	}
	var result []coordinate.Worker
	err = scanRows(rows, func() error {
		child := worker{namespace: w.namespace}
		if err := rows.Scan(&child.id, &child.name); err == nil {
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
	row := theDB(w).QueryRow("SELECT active FROM worker WHERE id=$1", w.id)
	err = row.Scan(&result)
	return
}

func (w *worker) Deactivate() error {
	return withTx(w, func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE worker SET active=FALSE WHERE id=$1", w.id)
		return err
	})
}

func (w *worker) Mode() (result string, err error) {
	row := theDB(w).QueryRow("SELECT mode FROM worker WHERE id=$1", w.id)
	err = row.Scan(&result)
	return
}

func (w *worker) Data() (map[string]interface{}, error) {
	var dataBytes []byte
	row := theDB(w).QueryRow("SELECT data FROM worker WHERE id=$1", w.id)
	err := row.Scan(&dataBytes)
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
	row := theDB(w).QueryRow("SELECT expiration FROM worker WHERE id=$1", w.id)
	err = row.Scan(&result)
	return
}

func (w *worker) LastUpdate() (result time.Time, err error) {
	row := theDB(w).QueryRow("SELECT last_update FROM worker WHERE id=$1", w.id)
	err = row.Scan(&result)
	return
}

func (w *worker) Update(data map[string]interface{}, now, expiration time.Time, mode string) error {
	dataBytes, err := mapToBytes(data)
	if err != nil {
		return err
	}
	_, err = theDB(w).Exec("UPDATE worker SET active=TRUE, mode=$2, data=$3, expiration=$4, last_update=$5 WHERE id=$1", w.id, mode, dataBytes, expiration, now)
	return err
}

// coordinable interface

func (w *worker) Coordinate() *pgCoordinate {
	return w.namespace.coordinate
}
