// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

type workUnit struct {
	workUnit coordinate.WorkUnit
	workSpec *workSpec
}

func newWorkUnit(upstream coordinate.WorkUnit, workSpec *workSpec) *workUnit {
	return &workUnit{
		workUnit: upstream,
		workSpec: workSpec,
	}
}

// refresh re-fetches the upstream object if possible.  This should be
// called when code strongly expects the cached object is invalid,
// for instance because a method has returned ErrGone.
//
// On success, unit.workUnit points at a newly fetched valid object,
// this object remains the cached work unit for its name in the work
// spec's cache, and returns nil.  On error returns the error from
// trying to fetch the work unit.
func (unit *workUnit) refresh() error {
	name := unit.workUnit.Name()
	var newUnit coordinate.WorkUnit
	err := unit.workSpec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		newUnit, err = workSpec.WorkUnit(name)
		return
	})
	if err == nil {
		unit.workUnit = newUnit
		return nil
	}
	unit.workSpec.invalidateWorkUnit(name)
	return err
}

// withWorkUnit calls some function with the current upstream work
// unit.  If that operation returns ErrGone, tries refreshing this
// object then trying again; it may also refresh the work spec and
// namespace.  Note that if there is an error doing the refresh, that
// error is discarded, and the original ErrGone is returned (which
// will be more meaningful to the caller).
func (unit *workUnit) withWorkUnit(f func(coordinate.WorkUnit) error) error {
	for {
		err := f(unit.workUnit)
		if err != coordinate.ErrGone {
			return err
		}
		err = unit.refresh()
		if err != nil {
			return coordinate.ErrGone
		}
	}
}

func (unit *workUnit) Name() string {
	return unit.workUnit.Name()
}

func (unit *workUnit) Data() (data map[string]interface{}, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		data, err = workUnit.Data()
		return
	})
	return
}

func (unit *workUnit) WorkSpec() coordinate.WorkSpec {
	return unit.workSpec
}

func (unit *workUnit) Status() (status coordinate.WorkUnitStatus, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		status, err = workUnit.Status()
		return
	})
	return
}

func (unit *workUnit) Meta() (meta coordinate.WorkUnitMeta, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		meta, err = workUnit.Meta()
		return
	})
	return
}

func (unit *workUnit) SetMeta(meta coordinate.WorkUnitMeta) error {
	return unit.withWorkUnit(func(workUnit coordinate.WorkUnit) error {
		return workUnit.SetMeta(meta)
	})
}

func (unit *workUnit) Priority() (priority float64, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		priority, err = workUnit.Priority()
		return
	})
	return
}

func (unit *workUnit) SetPriority(priority float64) error {
	return unit.withWorkUnit(func(workUnit coordinate.WorkUnit) error {
		return workUnit.SetPriority(priority)
	})
}

func (unit *workUnit) ActiveAttempt() (attempt coordinate.Attempt, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		attempt, err = workUnit.ActiveAttempt()
		return
	})
	return
}

func (unit *workUnit) ClearActiveAttempt() error {
	return unit.withWorkUnit(func(workUnit coordinate.WorkUnit) error {
		return workUnit.ClearActiveAttempt()
	})
}

func (unit *workUnit) Attempts() (attempts []coordinate.Attempt, err error) {
	err = unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		attempts, err = workUnit.Attempts()
		return
	})
	return
}

func (unit *workUnit) NumAttempts() (int, error) {
	n := 0
	var err error
	unit.withWorkUnit(func(workUnit coordinate.WorkUnit) (err error) {
		n, err = workUnit.NumAttempts()
		return err
	})
	return n, err
}
