package memory

import "github.com/dmaze/goordinate/coordinate"
import "time"

// Attempt type:

type memAttempt struct {
	workUnit       *memWorkUnit
	worker         *memWorker
	status         coordinate.AttemptStatus
	data           map[string]interface{}
	startTime      time.Time
	endTime        time.Time
	expirationTime time.Time
}

func (attempt *memAttempt) WorkUnit() coordinate.WorkUnit {
	return attempt.workUnit
}

func (attempt *memAttempt) Worker() coordinate.Worker {
	return attempt.worker
}

func (attempt *memAttempt) Status() (coordinate.AttemptStatus, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.status, nil
}

func (attempt *memAttempt) Data() (map[string]interface{}, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.data, nil
}

func (attempt *memAttempt) StartTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.startTime, nil
}

func (attempt *memAttempt) EndTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.endTime, nil
}

func (attempt *memAttempt) ExpirationTime() (time.Time, error) {
	globalLock(attempt)
	defer globalUnlock(attempt)
	return attempt.expirationTime, nil
}

// isPending checks to see whether an attempt is in "pending" state.
// This counts if the attempt is nominally expired but is still the
// active attempt its work unit.
func (attempt *memAttempt) isPending() bool {
	return (attempt.status == coordinate.Pending) ||
		((attempt.status == coordinate.Expired) &&
			(attempt.workUnit.activeAttempt == attempt))
}

// finish marks an attempt as finished in some form.  It updates the
// completion time, status, and data, and removes itself as the active
// work unit where possible.  Assumes the global lock.
func (attempt *memAttempt) finish(status coordinate.AttemptStatus, data map[string]interface{}) {
	attempt.endTime = time.Now()
	attempt.status = status
	if data != nil {
		attempt.data = data
	}
	attempt.worker.completeAttempt(attempt)
	// attempt.workUnit.completeAttempt(attempt)
}

func (attempt *memAttempt) Renew(extendDuration time.Duration, data map[string]interface{}) error {
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

func (attempt *memAttempt) Expire(data map[string]interface{}) error {
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

func (attempt *memAttempt) Finish(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Finished, data)
	return nil
}

func (attempt *memAttempt) Fail(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Failed, data)
	return nil
}

func (attempt *memAttempt) Retry(data map[string]interface{}) error {
	globalLock(attempt)
	defer globalUnlock(attempt)
	if !attempt.isPending() {
		return coordinate.ErrNotPending
	}
	attempt.finish(coordinate.Retryable, data)
	return nil
}

func (attempt *memAttempt) Coordinate() *memCoordinate {
	return attempt.workUnit.workSpec.namespace.coordinate
}
