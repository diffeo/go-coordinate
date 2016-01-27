// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"github.com/diffeo/go-coordinate/coordinate"
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

func (attempt *attempt) do(f func() error) error {
	globalLock(attempt)
	defer globalUnlock(attempt)

	if attempt.workUnit.deleted || attempt.workUnit.workSpec.deleted || attempt.workUnit.workSpec.namespace.deleted {
		return coordinate.ErrGone
	}
	return f()
}

func (attempt *attempt) Status() (status coordinate.AttemptStatus, err error) {
	err = attempt.do(func() error {
		attempt.workUnit.workSpec.expireUnits()
		status = attempt.status
		return nil
	})
	return
}

func (attempt *attempt) Data() (data map[string]interface{}, err error) {
	err = attempt.do(func() error {
		data = attempt.data
		return nil
	})
	return
}

func (attempt *attempt) StartTime() (start time.Time, err error) {
	err = attempt.do(func() error {
		start = attempt.startTime
		return nil
	})
	return
}

func (attempt *attempt) EndTime() (end time.Time, err error) {
	err = attempt.do(func() error {
		attempt.workUnit.workSpec.expireUnits()
		end = attempt.endTime
		return nil
	})
	return
}

func (attempt *attempt) ExpirationTime() (exp time.Time, err error) {
	err = attempt.do(func() error {
		attempt.workUnit.workSpec.expireUnits()
		exp = attempt.expirationTime
		return nil
	})
	return
}

// isPending checks to see whether an attempt is in "pending" state.
// This counts if the attempt is nominally expired but is still the
// active attempt for its work unit.
func (attempt *attempt) isPending() bool {
	return (attempt.status == coordinate.Pending) ||
		((attempt.status == coordinate.Expired) &&
			(attempt.workUnit.activeAttempt == attempt))
}

// finish marks an attempt as finished in some form.  It updates the
// completion time, status, and data, and removes itself as the active
// work unit where possible.  Assumes the global lock.
func (attempt *attempt) finish(status coordinate.AttemptStatus, data map[string]interface{}) {
	attempt.endTime = attempt.Coordinate().clock.Now()
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
	return attempt.do(func() error {
		// Check: we must be in a non-terminal status.
		if attempt.status != coordinate.Pending && attempt.status != coordinate.Expired {
			return coordinate.ErrNotPending
		}
		// Check: we must be the active attempt.  If we
		// aren't, we are expired and have lost our lease.
		// (We do not run expiry; if you can get here after
		// your time runs out but before you've actually been
		// expired, you win!)
		if attempt.workUnit.activeAttempt != attempt {
			attempt.finish(coordinate.Expired, data)
			return coordinate.ErrLostLease
		}
		// Otherwise, we get to extend our lease.
		attempt.expirationTime = attempt.Coordinate().clock.Now().Add(extendDuration)
		attempt.status = coordinate.Pending
		if data != nil {
			attempt.data = data
		}
		return nil
	})
}

func (attempt *attempt) Expire(data map[string]interface{}) error {
	return attempt.do(func() error {
		// No-op if already expired; error if not pending
		if attempt.status == coordinate.Expired {
			return nil
		} else if attempt.status != coordinate.Pending {
			return coordinate.ErrNotPending
		}

		attempt.finish(coordinate.Expired, data)
		return nil
	})
}

func (attempt *attempt) Finish(data map[string]interface{}) error {
	return attempt.do(func() error {
		if attempt.status != coordinate.Failed && !attempt.isPending() {
			return coordinate.ErrNotPending
		}
		attempt.finish(coordinate.Finished, data)

		// Does the work unit data include an "output" key
		// that we understand?
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
			newUnits = coordinate.ExtractWorkUnitOutput(output, attempt.Coordinate().clock.Now())
		}
		if newUnits != nil {
			then := attempt.workUnit.workSpec.meta.NextWorkSpecName
			if then != "" {
				nextWorkSpec, ok = attempt.workUnit.workSpec.namespace.workSpecs[then]
				nextWorkSpec.addWorkUnits(newUnits)
			}
		}

		return nil
	})
}

func (attempt *attempt) Fail(data map[string]interface{}) error {
	return attempt.do(func() error {
		if !attempt.isPending() {
			return coordinate.ErrNotPending
		}
		attempt.finish(coordinate.Failed, data)
		return nil
	})
}

func (attempt *attempt) Retry(data map[string]interface{}, delay time.Duration) error {
	return attempt.do(func() error {
		if !attempt.isPending() {
			return coordinate.ErrNotPending
		}
		attempt.finish(coordinate.Retryable, data)
		attempt.workUnit.meta.NotBefore = attempt.Coordinate().clock.Now().Add(delay)
		return nil
	})
}

func (attempt *attempt) Coordinate() *memCoordinate {
	return attempt.workUnit.workSpec.namespace.coordinate
}
