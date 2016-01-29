// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

type workUnit struct {
	name           string
	data           map[string]interface{}
	meta           coordinate.WorkUnitMeta
	activeAttempt  *attempt
	attempts       []*attempt
	workSpec       *workSpec
	availableIndex int
	deleted        bool
}

// coordinate.WorkUnit interface:

func (unit *workUnit) Name() string {
	return unit.name
}

func (unit *workUnit) Data() (data map[string]interface{}, err error) {
	err = unit.do(func() error {
		data = unit.data
		if unit.activeAttempt != nil && unit.activeAttempt.data != nil {
			data = unit.activeAttempt.data
		}
		return nil
	})
	return
}

func (unit *workUnit) WorkSpec() coordinate.WorkSpec {
	return unit.workSpec
}

func (unit *workUnit) do(f func() error) error {
	globalLock(unit)
	defer globalUnlock(unit)
	if unit.deleted || unit.workSpec.deleted || unit.workSpec.namespace.deleted {
		return coordinate.ErrGone
	}
	return f()
}

func (unit *workUnit) Status() (status coordinate.WorkUnitStatus, err error) {
	err = unit.do(func() error {
		unit.workSpec.expireUnits()
		status = unit.status()
		return nil
	})
	return
}

// status is an internal helper that converts a single unit's attempt
// status to a work unit status.  It assumes the global lock (and that
// the active attempt will not change under it).  It assumes that, if
// expiry is necessary, it has already been run.
func (unit *workUnit) status() coordinate.WorkUnitStatus {
	if unit.activeAttempt == nil {
		now := unit.Coordinate().clock.Now()
		switch {
		case now.Before(unit.meta.NotBefore):
			return coordinate.DelayedUnit
		default:
			return coordinate.AvailableUnit
		}
	}
	switch unit.activeAttempt.status {
	case coordinate.Pending:
		return coordinate.PendingUnit
	case coordinate.Expired:
		return coordinate.AvailableUnit
	case coordinate.Finished:
		return coordinate.FinishedUnit
	case coordinate.Failed:
		return coordinate.FailedUnit
	case coordinate.Retryable:
		return coordinate.AvailableUnit
	default:
		panic("invalid attempt status")
	}
}

func (unit *workUnit) Meta() (meta coordinate.WorkUnitMeta, err error) {
	err = unit.do(func() error {
		meta = unit.meta
		return nil
	})
	return
}

func (unit *workUnit) SetMeta(meta coordinate.WorkUnitMeta) error {
	return unit.do(func() error {
		unit.meta = meta
		unit.workSpec.available.Reprioritize(unit)
		return nil
	})
}

func (unit *workUnit) Priority() (float64, error) {
	meta, err := unit.Meta() // does the lock itself
	return meta.Priority, err
}

func (unit *workUnit) SetPriority(priority float64) error {
	return unit.do(func() error {
		unit.meta.Priority = priority
		unit.workSpec.available.Reprioritize(unit)
		return nil
	})
}

func (unit *workUnit) ActiveAttempt() (attempt coordinate.Attempt, err error) {
	err = unit.do(func() error {
		unit.workSpec.expireUnits()
		// Since this returns an interface type, if we just
		// return unit.activeAttempt, we will get back a nil
		// with a concrete type which is not equal to nil with
		// interface type. Go Go go!
		if unit.activeAttempt != nil {
			attempt = unit.activeAttempt
		}
		return nil
	})
	return
}

// resetAttempt clears the active attempt for a unit and returns it
// to its work spec's available list.  Assumes the global lock.
func (unit *workUnit) resetAttempt() {
	if unit.activeAttempt != nil {
		unit.activeAttempt = nil
		unit.workSpec.available.Add(unit)
	}
}

func (unit *workUnit) ClearActiveAttempt() error {
	return unit.do(func() error {
		unit.resetAttempt()
		return nil
	})
}

func (unit *workUnit) Attempts() (attempts []coordinate.Attempt, err error) {
	err = unit.do(func() error {
		attempts = make([]coordinate.Attempt, len(unit.attempts))
		for i, attempt := range unit.attempts {
			attempts[i] = attempt
		}
		return nil
	})
	return
}

// memory.coordinable interface:

func (unit *workUnit) Coordinate() *memCoordinate {
	return unit.workSpec.namespace.coordinate
}
