package memory

import (
	"github.com/dmaze/goordinate/coordinate"
	"time"
)

// Attempt type:

type attempt struct {
	workUnit       *workUnit
	worker         *worker
	status         coordinate.AttemptStatus
	data           map[string]interface{}
	startTime      time.Time
	endTime        time.Time
	expirationTime time.Time
}

func (attempt *attempt) WorkUnit() coordinate.WorkUnit {
	return attempt.workUnit
}

func (attempt *attempt) Worker() coordinate.Worker {
	return attempt.worker
}

func (attempt *attempt) Status() (coordinate.AttemptStatus, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.status, nil
}

func (attempt *attempt) Data() (map[string]interface{}, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.data, nil
}

func (attempt *attempt) StartTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.startTime, nil
}

func (attempt *attempt) EndTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.endTime, nil
}

func (attempt *attempt) ExpirationTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.expirationTime, nil
}

// isPending checks to see whether an attempt is in "pending" state.
// This counts if the attempt is nominally expired but is still the
// active attempt its work unit.
func (attempt *attempt) isPending() bool {
	return (attempt.status == coordinate.Pending) ||
		((attempt.status == coordinate.Expired) &&
			(attempt.workUnit.activeAttempt == attempt))
}

// finish marks an attempt as finished in some form.  It updates the
// completion time, status, and data, and removes itself as the active
// work unit where possible.  Assumes the global lock.
func (attempt *attempt) finish(status coordinate.AttemptStatus, data map[string]interface{}) {
	attempt.endTime = time.Now()
	attempt.status = status
	if data != nil {
		attempt.data = data
	}
	attempt.worker.completeAttempt(attempt)
	if status == coordinate.Expired || status == coordinate.Retryable {
		attempt.workUnit.resetAttempt()
	}
}

func (attempt *attempt) Renew(extendDuration time.Duration, data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)

	// Check: we must be in a non-terminal status.
	if attempt.status != coordinate.Pending && attempt.status != coordinate.Expired {
		return coordinate.ErrNotPending
	}
	// Check: we must be the active work unit.  If we aren't, we
	// are expired and have lost our lease.
	if attempt.workUnit.activeAttempt != attempt {
		attempt.finish(coordinate.Expired, data)
		return coordinate.ErrLostLease
	}
	// Otherwise, we get to extend our lease.
	attempt.expirationTime = time.Now().Add(extendDuration)
	attempt.status = coordinate.Pending
	if data != nil {
		attempt.data = data
	}
	return nil
}

func (attempt *attempt) Expire(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)

	// No-op if already expired; error if not pending
	if attempt.status == coordinate.Expired {
		return nil
	} else if attempt.status != coordinate.Pending {
		return coordinate.ErrNotPending
	}

	attempt.finish(coordinate.Expired, data)
	return nil
}

func (attempt *attempt) Finish(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if attempt.status != coordinate.Failed && !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Finished, data)

	// Does the work unit data include an "output" key that we
	// understand?
	if attempt.workUnit.activeAttempt != attempt {
		return nil
	}
	if data == nil {
		data = attempt.data
	}
	if data == nil {
		data = attempt.workUnit.data
	}
	var newUnits map[string]coordinate.AddWorkUnitItem
	var nextWorkSpec *workSpec
	output, ok := data["output"]
	if ok {
		newUnits = coordinate.ExtractWorkUnitOutput(output)
	}
	if newUnits != nil {
		then := attempt.workUnit.workSpec.meta.NextWorkSpecName
		if then != "" {
			nextWorkSpec, ok = attempt.workUnit.workSpec.namespace.workSpecs[then]
			nextWorkSpec.addWorkUnits(newUnits)
		}
	}

	return nil
}

func (attempt *attempt) Fail(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Failed, data)
	return nil
}

func (attempt *attempt) Retry(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Retryable, data)
	return nil
}

func (attempt *attempt) Coordinate() *memCoordinate {
	return attempt.workUnit.workSpec.namespace.coordinate
}
