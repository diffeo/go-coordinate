// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

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
	namespaceName               = namespaceTable + ".name"
	namespaceID                 = namespaceTable + ".id"
	workerID                    = workerTable + ".id"
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
	workSpecNextWorkSpec        = workSpecTable + ".next_work_spec_name"
	workSpecRuntime             = workSpecTable + ".runtime"
	workUnitID                  = workUnitTable + ".id"
	workUnitName                = workUnitTable + ".name"
	workUnitData                = workUnitTable + ".data"
	workUnitSpec                = workUnitTable + ".work_spec_id"
	workUnitAttempt             = workUnitTable + ".active_attempt_id"
	workUnitPriority            = workUnitTable + ".priority"
	workUnitNotBefore           = workUnitTable + ".not_before"

	// This join selects all work units and attempts, including
	// work units with no active attempt
	workUnitAttemptJoin = (workUnitTable + " LEFT OUTER JOIN " +
		attemptTable + "  ON " + workUnitAttempt + "=" + attemptID)

	// WHERE clause fragments:
	hasNoAttempt        = workUnitAttempt + " IS NULL"
	hasThisParent       = workerParent + "=$1"
	byThisWorker        = attemptWorkerID + "=$1"
	workUnitInSpec      = workUnitSpec + "=" + workSpecID
	attemptIsActive     = attemptActive + "=TRUE"
	attemptIsPending    = attemptStatus + "='pending'"
	attemptIsExpired    = attemptExpirationTime + "<$1"
	attemptThisWorkUnit = attemptWorkUnitID + "=" + workUnitID
	attemptThisWorker   = attemptWorkerID + "=" + workerID
)

func isWorkSpec(params *queryParams, id int) string {
	return workSpecID + "=" + params.Param(id)
}

func workSpecInNamespace(params *queryParams, id int) string {
	return workSpecNamespace + "=" + params.Param(id)
}

func workSpecHasName(params *queryParams, name string) string {
	return workSpecName + "=" + params.Param(name)
}

func isWorkUnit(params *queryParams, id int) string {
	return workUnitID + "=" + params.Param(id)
}

func workUnitInOtherSpec(params *queryParams, id int) string {
	return workUnitSpec + "=" + params.Param(id)
}

func workUnitHasName(params *queryParams, name string) string {
	return workUnitName + "=" + params.Param(name)
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
