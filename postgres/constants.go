// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

// This file contains Coordinate-specific SQL definitions.  It follows
// the following conventions:
//
// tableTable: constant for the table name "table"
// tableColumn: constant for the column name "table.column"
// isTable: WHERE test for primary key match of table
// tableIsColumny: test for some boolean property of table
// tableHasColumn: equality test for some non-boolean property of table
// tableIsThing: foreign-key test for some other non-joined table
// tableIsThisThing: foreign-key test for some other joined table

import (
	"time"
)

const (
	// SQL table names:
	attemptTable   = "attempt"
	namespaceTable = "namespace"
	workerTable    = "worker"
	workSpecTable  = "work_spec"
	workUnitTable  = "work_unit"

	// SQL column names:
	attemptID                   = attemptTable + ".id"
	attemptWorkUnitID           = attemptTable + ".work_unit_id"
	attemptWorkerID             = attemptTable + ".worker_id"
	attemptStatus               = attemptTable + ".status"
	attemptData                 = attemptTable + ".data"
	attemptStartTime            = attemptTable + ".start_time"
	attemptEndTime              = attemptTable + ".end_time"
	attemptExpirationTime       = attemptTable + ".expiration_time"
	attemptActive               = attemptTable + ".active"
	attemptWorkSpecID           = attemptTable + ".work_spec_id"
	namespaceName               = namespaceTable + ".name"
	namespaceID                 = namespaceTable + ".id"
	workerID                    = workerTable + ".id"
	workerNamespace             = workerTable + ".namespace_id"
	workerName                  = workerTable + ".name"
	workerParent                = workerTable + ".parent"
	workSpecID                  = workSpecTable + ".id"
	workSpecName                = workSpecTable + ".name"
	workSpecNamespace           = workSpecTable + ".namespace_id"
	workSpecData                = workSpecTable + ".data"
	workSpecPriority            = workSpecTable + ".priority"
	workSpecWeight              = workSpecTable + ".weight"
	workSpecPaused              = workSpecTable + ".paused"
	workSpecContinuous          = workSpecTable + ".continuous"
	workSpecCanBeContinuous     = workSpecTable + ".can_be_continuous"
	workSpecMinMemoryGb         = workSpecTable + ".min_memory_gb"
	workSpecInterval            = workSpecTable + ".interval"
	workSpecNextContinuous      = workSpecTable + ".next_continuous"
	workSpecMaxRunning          = workSpecTable + ".max_running"
	workSpecMaxAttemptsReturned = workSpecTable + ".max_attempts_returned"
	workSpecMaxRetries          = workSpecTable + ".max_retries"
	workSpecNextWorkSpec        = workSpecTable + ".next_work_spec_name"
	workSpecRuntime             = workSpecTable + ".runtime"
	workUnitID                  = workUnitTable + ".id"
	workUnitName                = workUnitTable + ".name"
	workUnitData                = workUnitTable + ".data"
	workUnitSpec                = workUnitTable + ".work_spec_id"
	workUnitAttempt             = workUnitTable + ".active_attempt_id"
	workUnitPriority            = workUnitTable + ".priority"
	workUnitNotBefore           = workUnitTable + ".not_before"

	// WHERE clause fragments:
	workSpecInThisNamespace = workSpecNamespace + "=" + namespaceID
	workUnitHasNoAttempt    = workUnitAttempt + " IS NULL"
	workUnitInThisSpec      = workUnitSpec + "=" + workSpecID
	attemptIsActive         = attemptActive + "=TRUE"
	attemptIsPending        = attemptStatus + "='pending'"
	attemptThisWorkUnit     = attemptWorkUnitID + "=" + workUnitID
	attemptThisWorker       = attemptWorkerID + "=" + workerID
	attemptIsTheActive      = attemptID + "=" + workUnitAttempt
	attemptInThisSpec       = attemptWorkSpecID + "=" + workSpecID

	// This join selects all work units and attempts, including
	// work units with no active attempt
	workUnitAttemptJoin = (workUnitTable + " LEFT OUTER JOIN " +
		attemptTable + "  ON " + attemptIsTheActive)
)

// More WHERE clause fragments, that depend on query params:

func isNamespace(params *queryParams, id int) string {
	return namespaceID + "=" + params.Param(id)
}

func isWorkSpec(params *queryParams, id int) string {
	return workSpecID + "=" + params.Param(id)
}

func workSpecInNamespace(params *queryParams, id int) string {
	return workSpecNamespace + "=" + params.Param(id)
}

func workSpecHasName(params *queryParams, name string) string {
	return workSpecName + "=" + params.Param(name)
}

// workSpecNotTooSoon determines whether a work spec can run a new
// continuous work unit, because its next-continuous time has arrived.
func workSpecNotTooSoon(params *queryParams, now time.Time) string {
	return "(" + workSpecNextContinuous + " IS NULL OR " +
		workSpecNextContinuous + "<=" + params.Param(now) + ")"
}

func isWorkUnit(params *queryParams, id int) string {
	return workUnitID + "=" + params.Param(id)
}

func workUnitInSpec(params *queryParams, id int) string {
	return workUnitSpec + "=" + params.Param(id)
}

func workUnitHasName(params *queryParams, name string) string {
	return workUnitName + "=" + params.Param(name)
}

func workUnitHasAttempt(params *queryParams, id int) string {
	return workUnitAttempt + "=" + params.Param(id)
}

// workUnitTooSoon determines whether a work unit cannot run because
// its not_before time has not arrived yet.  If a work unit looks
// available and this predicate returns true, it is actually delayed.
func workUnitTooSoon(params *queryParams, now time.Time) string {
	return "(" + workUnitNotBefore + " IS NOT NULL AND " + params.Param(now) + "<" + workUnitNotBefore + ")"
}

// workUnitAvailable determines whether a work unit is really available.
func workUnitAvailable(params *queryParams, now time.Time) string {
	return "(" + attemptStatus + " IS NULL AND NOT (" + workUnitTooSoon(params, now) + "))"
}

// workUnitDelayed determines whether a work unit is delayed: it has no
// active attempt but it is too soon for it to start.
func workUnitDelayed(params *queryParams, now time.Time) string {
	return "(" + attemptStatus + " IS NULL AND (" + workUnitTooSoon(params, now) + "))"
}

func isAttempt(params *queryParams, id int) string {
	return attemptID + "=" + params.Param(id)
}

func attemptForUnit(params *queryParams, id int) string {
	return attemptWorkUnitID + "=" + params.Param(id)
}

func attemptByWorker(params *queryParams, id int) string {
	return attemptWorkerID + "=" + params.Param(id)
}

func attemptIsExpired(params *queryParams, now time.Time) string {
	return attemptExpirationTime + "<" + params.Param(now)
}

func isWorker(params *queryParams, id int) string {
	return workerID + "=" + params.Param(id)
}

func workerInNamespace(params *queryParams, id int) string {
	return workerNamespace + "=" + params.Param(id)
}

func workerHasName(params *queryParams, name string) string {
	return workerName + "=" + params.Param(name)
}

func workerHasParent(params *queryParams, id int) string {
	return workerParent + "=" + params.Param(id)
}
