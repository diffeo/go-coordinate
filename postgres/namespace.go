// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"github.com/diffeo/go-coordinate/coordinate"
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
	err := withTx(c, false, func(tx *sql.Tx) error {
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

func (c *pgCoordinate) Namespaces() (map[string]coordinate.Namespace, error) {
	result := make(map[string]coordinate.Namespace)
	params := queryParams{}
	query := buildSelect([]string{
		namespaceName,
		namespaceID,
	}, []string{
		namespaceTable,
	}, []string{})
	err := queryAndScan(c, query, params, func(rows *sql.Rows) error {
		ns := namespace{coordinate: c}
		err := rows.Scan(&ns.name, &ns.id)
		if err != nil {
			return err
		}
		result[ns.name] = &ns
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// coordinate.Namespace interface:

func (ns *namespace) Name() string {
	return ns.name
}

func (ns *namespace) Destroy() error {
	params := queryParams{}
	query := "DELETE FROM NAMESPACE WHERE id=" + params.Param(ns.id)
	return execInTx(ns, query, params, false)
}

// coordinable interface:

func (ns *namespace) Coordinate() *pgCoordinate {
	return ns.coordinate
}
