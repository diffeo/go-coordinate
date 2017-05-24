// Statistics generation for everything that needs it.
//
// Copyright 2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"

	"github.com/diffeo/go-coordinate/coordinate"
)

// summarize computes summary statistics for things.  This runs a single
// SQL query over namespaces, work specs, work units, and attempts;
// it selects all active attempts everywhere, further limited by
// whatever is passed as restrictions.
func summarize(
	c coordinable,
	params queryParams,
	restrictions []string,
) (coordinate.Summary, error) {
	var result coordinate.Summary
	outputs := []string{
		namespaceName,
		workSpecName,
		attemptStatus,
		workUnitTooSoon(&params, c.Coordinate().clock.Now()) + " delayed",
		"COUNT(*)",
	}
	tables := []string{
		namespaceTable,
		workSpecTable,
		workUnitAttemptJoin,
	}
	conditions := []string{
		workSpecInThisNamespace,
		workUnitInThisSpec,
	}
	conditions = append(conditions, restrictions...)
	query := buildSelect(outputs, tables, conditions)
	query += (" GROUP BY " + namespaceName + ", " + workSpecName + ", " +
		attemptStatus + ", delayed")
	err := queryAndScan(c, query, params, func(rows *sql.Rows) error {
		var record coordinate.SummaryRecord
		var status sql.NullString
		var delayed bool
		err := rows.Scan(&record.Namespace, &record.WorkSpec, &status,
			&delayed, &record.Count)
		if err != nil {
			return err
		}
		if !status.Valid {
			if delayed {
				record.Status = coordinate.DelayedUnit
			} else {
				record.Status = coordinate.AvailableUnit
			}
		} else {
			switch status.String {
			case "expired":
				record.Status = coordinate.AvailableUnit
			case "retryable":
				record.Status = coordinate.AvailableUnit
			case "pending":
				record.Status = coordinate.PendingUnit
			case "finished":
				record.Status = coordinate.FinishedUnit
			case "failed":
				record.Status = coordinate.FailedUnit
			}
		}
		result = append(result, record)
		return nil
	})
	if err != nil {
		return coordinate.Summary{}, err
	}
	return result, nil
}

func (c *pgCoordinate) Summarize() (coordinate.Summary, error) {
	return summarize(c, nil, nil)
}

func (ns *namespace) Summarize() (coordinate.Summary, error) {
	var params queryParams
	restrictions := []string{
		isNamespace(&params, ns.id),
	}
	return summarize(ns, params, restrictions)
}

func (spec *workSpec) Summarize() (coordinate.Summary, error) {
	var params queryParams
	restrictions := []string{
		isWorkSpec(&params, spec.id),
	}
	return summarize(spec, params, restrictions)
}
