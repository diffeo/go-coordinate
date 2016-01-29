// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

type workSpec struct {
	workSpec  coordinate.WorkSpec
	namespace *namespace
	workUnits *lru
}

func newWorkSpec(upstream coordinate.WorkSpec, namespace *namespace) *workSpec {
	return &workSpec{
		workSpec:  upstream,
		namespace: namespace,
		workUnits: newLRU(2048),
	}
}

// refresh re-fetches the upstream object if possible.  This should be
// called when code strongly expects the cached object is invalid,
// for instance because a method has returned ErrGone.
//
// On success, spec.workSpec points at a newly fetched valid object,
// this work spec's caches are cleared, this object remains the cached
// work spec for its name in the namespace cache, and returns nil.  On
// error returns the error from trying to fetch the work spec.
func (spec *workSpec) refresh() error {
	name := spec.workSpec.Name()
	var newSpec coordinate.WorkSpec
	err := spec.namespace.withNamespace(func(namespace coordinate.Namespace) (err error) {
		newSpec, err = namespace.WorkSpec(name)
		return
	})
	if err == nil {
		spec.workSpec = newSpec
		spec.workUnits = newLRU(2048)
		return nil
	}
	spec.namespace.invalidateWorkSpec(name)
	return err
}

// withWorkSpec calls some function with the current upstream work
// spec.  If that operation returns ErrGone, tries refreshing this
// object then trying again; it may also refresh the namespace.  Note
// that if there is an error doing the refresh, that error is
// discarded, and the original ErrGone is returned (which will be more
// meaningful to the caller).
func (spec *workSpec) withWorkSpec(f func(coordinate.WorkSpec) error) error {
	for {
		err := f(spec.workSpec)
		if err != coordinate.ErrGone {
			return err
		}
		// NB: refresh() runs under spec.namespace.withNamespace(),
		// which will refresh the parent namespace if that's gone
		// too.  err could be ErrGone here!  But it may not be.
		err = spec.refresh()
		if err != nil {
			return coordinate.ErrGone
		}
	}
}

func (spec *workSpec) invalidateWorkUnit(name string) {
	spec.workUnits.Remove(name)
}

func (spec *workSpec) Name() string {
	return spec.workSpec.Name()
}

func (spec *workSpec) Data() (data map[string]interface{}, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		data, err = spec.workSpec.Data()
		return
	})
	return
}

func (spec *workSpec) SetData(data map[string]interface{}) error {
	return spec.withWorkSpec(func(workSpec coordinate.WorkSpec) error {
		return workSpec.SetData(data)
	})
}

func (spec *workSpec) Meta(withCounts bool) (meta coordinate.WorkSpecMeta, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		meta, err = workSpec.Meta(withCounts)
		return
	})
	return
}

func (spec *workSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	return spec.withWorkSpec(func(workSpec coordinate.WorkSpec) error {
		return workSpec.SetMeta(meta)
	})
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, meta coordinate.WorkUnitMeta) (workUnit coordinate.WorkUnit, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		workUnit, err = workSpec.AddWorkUnit(name, data, meta)
		if err == nil {
			workUnit = newWorkUnit(workUnit, spec)
			spec.workUnits.Put(workUnit)
		}
		return
	})
	return
}

func (spec *workSpec) WorkUnit(name string) (workUnit coordinate.WorkUnit, err error) {
	unit, err := spec.workUnits.Get(name, func(n string) (unit named, err error) {
		err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) error {
			upstream, err := workSpec.WorkUnit(n)
			if err == nil {
				unit = newWorkUnit(upstream, spec)
			}
			return err
		})
		return
	})
	if err == nil {
		workUnit = unit.(coordinate.WorkUnit)
	}
	return
}

func (spec *workSpec) WorkUnits(q coordinate.WorkUnitQuery) (units map[string]coordinate.WorkUnit, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		units, err = workSpec.WorkUnits(q)
		return
	})
	return
}

func (spec *workSpec) CountWorkUnitStatus() (counts map[coordinate.WorkUnitStatus]int, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		counts, err = workSpec.CountWorkUnitStatus()
		return
	})
	return
}

func (spec *workSpec) SetWorkUnitPriorities(q coordinate.WorkUnitQuery, p float64) error {
	return spec.withWorkSpec(func(workSpec coordinate.WorkSpec) error {
		return workSpec.SetWorkUnitPriorities(q, p)
	})
}

func (spec *workSpec) AdjustWorkUnitPriorities(q coordinate.WorkUnitQuery, p float64) error {
	return spec.withWorkSpec(func(workSpec coordinate.WorkSpec) error {
		return workSpec.AdjustWorkUnitPriorities(q, p)
	})
}

func (spec *workSpec) DeleteWorkUnits(q coordinate.WorkUnitQuery) (count int, err error) {
	err = spec.withWorkSpec(func(workSpec coordinate.WorkSpec) (err error) {
		count, err = workSpec.DeleteWorkUnits(q)
		return
	})
	return
}
