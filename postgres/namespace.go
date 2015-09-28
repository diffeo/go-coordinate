package postgres

import (
	"database/sql"
	"github.com/dmaze/goordinate/coordinate"
)

type namespace struct {
	coordinate *pgCoordinate
	id         int
	name       string
}

// coordinate.Coordinate.Namespace() "constructor":

func (c *pgCoordinate) Namespace(name string) (coordinate.Namespace, error) {
	ns := namespace{
		coordinate: c,
		name:       name,
	}
	err := withTx(c, func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT id FROM namespace WHERE name=$1", name)
		err := row.Scan(&ns.id)
		if err == sql.ErrNoRows {
			// Create the namespace
			row = tx.QueryRow("INSERT INTO namespace(name) VALUES ($1) RETURNING id", name)
			err = row.Scan(&ns.id)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &ns, nil
}

// coordinate.Namespace interface:

func (ns *namespace) Name() string {
	return ns.name
}

func (ns *namespace) Destroy() error {
	_, err := theDB(ns).Exec("DELETE FROM namespace WHERE id=$1", ns.id)
	return err
}

// coordinable interface:

func (ns *namespace) Coordinate() *pgCoordinate {
	return ns.coordinate
}
