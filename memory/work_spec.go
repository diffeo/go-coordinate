// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"sort"
)

type workSpec struct {
	name      string
	namespace *namespace
	data      map[string]interface{}
	meta      coordinate.WorkSpecMeta
	workUnits map[string]*workUnit
	available availableUnits
	deleted   bool
}

func newWorkSpec(namespace *namespace, name string) *workSpec {
	return &workSpec{
		name:      name,
		namespace: namespace,
		data:      make(map[string]interface{}),
		workUnits: make(map[string]*workUnit),
	}
}

func (spec *workSpec) Name() string {
	return spec.name
}

func (spec *workSpec) do(f func() error) error {
	globalLock(spec)
	defer globalUnlock(spec)

	if spec.deleted || spec.namespace.deleted {
		return coordinate.ErrGone
	}

	return f()
}

func (spec *workSpec) Data() (data map[string]interface{}, err error) {
	err = spec.do(func() error {
		data = spec.data
		return nil
	})
	return
}

func (spec *workSpec) SetData(data map[string]interface{}) error {
	return spec.do(func() error {
		return spec.setData(data)
	})
}

// setData is an internal version of SetData() with the same constraints,
// guarantees, and checking.  It assumes the global lock.
func (spec *workSpec) setData(data map[string]interface{}) error {
	name, meta, err := coordinate.ExtractWorkSpecMeta(data)
	if err == nil {
		if name != spec.name {
			err = coordinate.ErrChangedName
		}
	}
	if err == nil {
		spec.data = data
		spec.meta = meta
	}
	return err
}

func (spec *workSpec) Meta(withCounts bool) (meta coordinate.WorkSpecMeta, err error) {
	err = spec.do(func() error {
		meta = spec.getMeta(withCounts)
		return nil
	})
	return
}

// getMeta gets a copy of this spec's metadata, optionally with counts
// filled in.  It expects to run within the global lock.
func (spec *workSpec) getMeta(withCounts bool) coordinate.WorkSpecMeta {
	result := spec.meta
	result.AvailableCount = 0
	result.PendingCount = 0
	if withCounts {
		spec.expireUnits()
		for _, unit := range spec.workUnits {
			switch unit.status() {
			case coordinate.AvailableUnit:
				result.AvailableCount++
			case coordinate.PendingUnit:
				result.PendingCount++
			}
		}
	}
	return result
}

func (spec *workSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	return spec.do(func() error {
		// Preserve immutable fields (taking advantage of meta pass-by-value)
		meta.CanBeContinuous = spec.meta.CanBeContinuous
		meta.NextWorkSpecName = spec.meta.NextWorkSpecName
		meta.Runtime = spec.meta.Runtime

		// If this cannot be continuous, force-clear that flag
		if !meta.CanBeContinuous {
			meta.Continuous = false
		}

		spec.meta = meta
		return nil
	})
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, meta coordinate.WorkUnitMeta) (unit coordinate.WorkUnit, err error) {
	err = spec.do(func() error {
		now := spec.Coordinate().clock.Now()
		theUnit, exists := spec.workUnits[name]
		if exists {
			theUnit.data = data
			theUnit.meta = meta
			// NB: we do not care if the unit is expired;
			// that would only cause it to transition
			// pending -> available which does not affect
			// this case
			switch theUnit.status() {
			case coordinate.AvailableUnit, coordinate.PendingUnit, coordinate.DelayedUnit:
				// do nothing
			default:
				// drop the existing (completed) attempt and
				// make the work unit be available again
				theUnit.activeAttempt = nil
				if !now.Before(theUnit.meta.NotBefore) {
					spec.available.Add(theUnit)
				}
			}
		} else {
			theUnit = new(workUnit)
			theUnit.name = name
			theUnit.data = data
			theUnit.meta = meta
			theUnit.workSpec = spec
			spec.workUnits[name] = theUnit
			if !now.Before(theUnit.meta.NotBefore) {
				spec.available.Add(theUnit)
			}
		}
		unit = theUnit
		return nil
	})
	return
}

func (spec *workSpec) addWorkUnits(units map[string]coordinate.AddWorkUnitItem) {
	now := spec.Coordinate().clock.Now()
	for name, item := range units {
		unit := workUnit{
			name:     name,
			data:     item.Data,
			meta:     item.Meta,
			workSpec: spec,
		}
		spec.workUnits[name] = &unit
		if !now.Before(unit.meta.NotBefore) {
			spec.available.Add(&unit)
		}
	}
}

func (spec *workSpec) WorkUnit(name string) (unit coordinate.WorkUnit, err error) {
	err = spec.do(func() error {
		var present bool
		unit, present = spec.workUnits[name]
		if !present {
			return coordinate.ErrNoSuchWorkUnit{Name: name}
		}
		return nil
	})
	return
}

// queryWithoutLimit calls a callback function for every work unit that
// a coordinate.WorkUnitQuery selects, ignoring the limit field (which
// requires sorting).
func (spec *workSpec) queryWithoutLimit(query coordinate.WorkUnitQuery, f func(*workUnit)) {
	// Clarity over efficiency: iterate through *all* of the work
	// units and keep the ones that match the query.  If Limit is
	// specified then sort the result after the fact.
	for name, unit := range spec.workUnits {
		if name <= query.PreviousName {
			continue
		}
		if query.Names != nil {
			ok := false
			for _, candidate := range query.Names {
				if name == candidate {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		if query.Statuses != nil {
			ok := false
			status := unit.status()
			for _, candidate := range query.Statuses {
				if status == candidate {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		// If we are here we have passed all filters
		f(unit)
	}

}

// query calls a callback function for every work unit that a
// coordinate.WorkUnitQuery selects, in sorted order if limit is specified.
func (spec *workSpec) query(query coordinate.WorkUnitQuery, f func(*workUnit)) {
	// The query could mention a state, in which case we need to
	// run expiry to distinguish available vs. pending
	spec.expireUnits()
	// No limit?  We know how to do that
	if query.Limit <= 0 {
		spec.queryWithoutLimit(query, f)
		return
	}
	// Otherwise there *is* a limit.  Collect the interesting keys:
	var names []string
	spec.queryWithoutLimit(query, func(unit *workUnit) {
		names = append(names, unit.name)
	})
	// Sort them:
	sort.Strings(names)
	// Apply the limit:
	if len(names) > query.Limit {
		names = names[:query.Limit]
	}
	// Call the callback
	for _, name := range names {
		f(spec.workUnits[name])
	}
}

func (spec *workSpec) WorkUnits(query coordinate.WorkUnitQuery) (result map[string]coordinate.WorkUnit, err error) {
	err = spec.do(func() error {
		result = make(map[string]coordinate.WorkUnit)
		spec.query(query, func(unit *workUnit) {
			result[unit.name] = unit
		})
		return nil
	})
	return
}

func (spec *workSpec) CountWorkUnitStatus() (result map[coordinate.WorkUnitStatus]int, err error) {
	err = spec.do(func() error {
		spec.expireUnits()
		result = make(map[coordinate.WorkUnitStatus]int)
		for _, unit := range spec.workUnits {
			result[unit.status()]++
		}
		return nil
	})
	return
}

func (spec *workSpec) SetWorkUnitPriorities(query coordinate.WorkUnitQuery, priority float64) error {
	return spec.do(func() error {
		spec.query(query, func(unit *workUnit) {
			unit.meta.Priority = priority
			spec.available.Reprioritize(unit)
		})
		return nil
	})
}

func (spec *workSpec) AdjustWorkUnitPriorities(query coordinate.WorkUnitQuery, adjustment float64) error {
	return spec.do(func() error {
		spec.query(query, func(unit *workUnit) {
			unit.meta.Priority += adjustment
			spec.available.Reprioritize(unit)
		})
		return nil
	})
}

func (spec *workSpec) DeleteWorkUnits(query coordinate.WorkUnitQuery) (count int, err error) {
	err = spec.do(func() error {
		// NB: This depends somewhat on Go having good behavior if we
		// modify the keys of the map of work units while iterating
		// through it.
		count = 0
		deleteWorkUnit := func(workUnit *workUnit) {
			for _, attempt := range workUnit.attempts {
				attempt.worker.completeAttempt(attempt)
				attempt.worker.removeAttempt(attempt)
			}
			delete(spec.workUnits, workUnit.name)
			workUnit.deleted = true
			spec.available.Remove(workUnit)
			count++
		}
		spec.query(query, deleteWorkUnit)
		return nil
	})
	return
}

// expireUnits scans all work units in this work spec, and if any have
// an active attempt whose expiration time has passed, marks them as
// expired and clears that active attempt.  It assumes the global
// lock.
func (spec *workSpec) expireUnits() {
	now := spec.Coordinate().clock.Now()
	for _, unit := range spec.workUnits {
		switch unit.status() {
		case coordinate.PendingUnit:
			// If the attempt's expiration time has passed,
			// expire it
			if unit.activeAttempt.expirationTime.Before(now) {
				unit.activeAttempt.finish(coordinate.Expired, nil)
			}
		case coordinate.AvailableUnit:
			// If it is not in the available list (probably
			// because it had previously been delayed) add it
			if unit.availableIndex == 0 {
				spec.available.Add(unit)
			}
		case coordinate.DelayedUnit:
			// If it is in the available list, remove it
			// (which may imply time going backwards)
			if unit.availableIndex > 0 {
				spec.available.Remove(unit)
			}
		}
	}
}

func (spec *workSpec) Coordinate() *memCoordinate {
	return spec.namespace.coordinate
}
