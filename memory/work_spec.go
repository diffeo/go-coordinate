package memory

import (
	"github.com/dmaze/goordinate/coordinate"
	"sort"
)

type workSpec struct {
	name      string
	namespace *namespace
	data      map[string]interface{}
	meta      coordinate.WorkSpecMeta
	workUnits map[string]*workUnit
	available availableUnits
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

func (spec *workSpec) Data() (map[string]interface{}, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	return spec.data, nil
}

func (spec *workSpec) SetData(data map[string]interface{}) error {
	globalLock(spec)
	defer globalUnlock(spec)

	return spec.setData(data)
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

func (spec *workSpec) Meta(withCounts bool) (coordinate.WorkSpecMeta, error) {
	globalLock(spec)
	defer globalUnlock(spec)
	return spec.getMeta(withCounts), nil
}

// getMeta gets a copy of this spec's metadata, optionally with counts
// filled in.  It expects to run within the global lock.
func (spec *workSpec) getMeta(withCounts bool) coordinate.WorkSpecMeta {
	result := spec.meta
	result.AvailableCount = 0
	result.PendingCount = 0
	if withCounts {
		for _, unit := range spec.workUnits {
			if unit.activeAttempt == nil ||
				unit.activeAttempt.status == coordinate.Expired ||
				unit.activeAttempt.status == coordinate.Retryable {
				result.AvailableCount++
			} else if unit.activeAttempt.status == coordinate.Pending {
				result.PendingCount++
			}
		}
	}
	return result
}

func (spec *workSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	globalLock(spec)
	defer globalUnlock(spec)

	// Preserve immutable fields (taking advantage of meta pass-by-value)
	meta.CanBeContinuous = spec.meta.CanBeContinuous
	meta.NextWorkSpecName = spec.meta.NextWorkSpecName
	meta.NextWorkSpecPreempts = spec.meta.NextWorkSpecPreempts

	// If this cannot be continuous, force-clear that flag
	if !meta.CanBeContinuous {
		meta.Continuous = false
	}

	spec.meta = meta
	return nil
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, priority float64) (coordinate.WorkUnit, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	unit := new(workUnit)
	unit.name = name
	unit.data = data
	unit.priority = priority
	unit.workSpec = spec
	spec.workUnits[name] = unit
	spec.available.Add(unit)
	return unit, nil
}

func (spec *workSpec) WorkUnit(name string) (coordinate.WorkUnit, error) {
	globalLock(spec)
	defer globalUnlock(spec)
	workUnit := spec.workUnits[name]
	if workUnit == nil {
		return nil, nil
	}
	return workUnit, nil
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
	globalLock(spec)
	defer globalUnlock(spec)

	result = make(map[string]coordinate.WorkUnit)
	spec.query(query, func(unit *workUnit) {
		result[unit.name] = unit
	})
	return
}

func (spec *workSpec) SetWorkUnitPriorities(query coordinate.WorkUnitQuery, priority float64) error {
	globalLock(spec)
	defer globalUnlock(spec)
	spec.query(query, func(unit *workUnit) {
		unit.priority = priority
		spec.available.Reprioritize(unit)
	})
	return nil
}

func (spec *workSpec) AdjustWorkUnitPriorities(query coordinate.WorkUnitQuery, adjustment float64) error {
	globalLock(spec)
	defer globalUnlock(spec)
	spec.query(query, func(unit *workUnit) {
		unit.priority += adjustment
		spec.available.Reprioritize(unit)
	})
	return nil
}

func (spec *workSpec) DeleteWorkUnits(query coordinate.WorkUnitQuery) (int, error) {
	globalLock(spec)
	defer globalUnlock(spec)
	// NB: This depends somewhat on Go having good behavior if we
	// modify the keys of the map of work units while iterating
	// through it.
	count := 0
	deleteWorkUnit := func(workUnit *workUnit) {
		for _, attempt := range workUnit.attempts {
			attempt.worker.completeAttempt(attempt)
			attempt.worker.removeAttempt(attempt)
		}
		delete(spec.workUnits, workUnit.name)
		spec.available.Remove(workUnit)
		count++
	}

	spec.query(query, deleteWorkUnit)
	return count, nil
}

func (spec *workSpec) Coordinate() *memCoordinate {
	return spec.namespace.coordinate
}
