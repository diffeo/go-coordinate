package memory

import "github.com/dmaze/goordinate/coordinate"
import "sort"

type memWorkSpec struct {
	name      string
	namespace *memNamespace
	data      map[string]interface{}
	meta      coordinate.WorkSpecMeta
	workUnits map[string]*memWorkUnit
}

func newWorkSpec(namespace *memNamespace, name string) *memWorkSpec {
	return &memWorkSpec{
		name:      name,
		namespace: namespace,
		data:      make(map[string]interface{}),
		workUnits: make(map[string]*memWorkUnit),
	}
}

func (spec *memWorkSpec) Name() string {
	return spec.name
}

func (spec *memWorkSpec) Data() (map[string]interface{}, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	return spec.data, nil
}

func (spec *memWorkSpec) SetData(data map[string]interface{}) error {
	globalLock(spec)
	defer globalUnlock(spec)

	return spec.setData(data)
}

// setData is an internal version of SetData() with the same constraints,
// guarantees, and checking.  It assumes the global lock.
func (spec *memWorkSpec) setData(data map[string]interface{}) error {
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

func (spec *memWorkSpec) Meta(withCounts bool) (coordinate.WorkSpecMeta, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	// TODO(dmaze): fill in meta.PendingCount
	return spec.meta, nil
}

func (spec *memWorkSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	globalLock(spec)
	defer globalUnlock(spec)

	// Preserve immutable fields (taking advantage of meta pass-by-value)
	meta.CanBeContinuous = spec.meta.CanBeContinuous
	meta.NextWorkSpecName = spec.meta.NextWorkSpecName
	meta.NextWorkSpecPreempts = spec.meta.NextWorkSpecPreempts

	spec.meta = meta
	return nil
}

func (spec *memWorkSpec) AddWorkUnit(name string, data map[string]interface{}, priority int) (coordinate.WorkUnit, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	unit := new(memWorkUnit)
	unit.name = name
	unit.data = data
	unit.priority = priority
	unit.workSpec = spec
	spec.workUnits[name] = unit
	return unit, nil
}

func (spec *memWorkSpec) WorkUnit(name string) (coordinate.WorkUnit, error) {
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
func (spec *memWorkSpec) queryWithoutLimit(query coordinate.WorkUnitQuery, f func(*memWorkUnit)) {
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
func (spec *memWorkSpec) query(query coordinate.WorkUnitQuery, f func(*memWorkUnit)) {
	// No limit?  We know how to do that
	if query.Limit <= 0 {
		spec.queryWithoutLimit(query, f)
		return
	}
	// Otherwise there *is* a limit.  Collect the interesting keys:
	var names []string
	spec.queryWithoutLimit(query, func(unit *memWorkUnit) {
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

func (spec *memWorkSpec) WorkUnits(query coordinate.WorkUnitQuery) (result map[string]coordinate.WorkUnit, err error) {
	globalLock(spec)
	defer globalUnlock(spec)

	result = make(map[string]coordinate.WorkUnit)
	spec.query(query, func(unit *memWorkUnit) {
		result[unit.name] = unit
	})
	return
}

// deleteWorkUnit does the heavy lifting to delete a work unit.  In
// particular, it deletes the work unit's attempts from the
// corresponding worker objects.  It assumes the global lock.
func (spec *memWorkSpec) deleteWorkUnit(workUnit *memWorkUnit) {
	for _, attempt := range workUnit.attempts {
		attempt.worker.completeAttempt(attempt)
		attempt.worker.removeAttempt(attempt)
	}
	delete(spec.workUnits, workUnit.name)
}

func (spec *memWorkSpec) DeleteWorkUnits(query coordinate.WorkUnitQuery) error {
	globalLock(spec)
	defer globalUnlock(spec)
	// NB: This depends somewhat on Go having good behavior if we
	// modify the keys of the map of work units while iterating
	// through it.
	spec.query(query, spec.deleteWorkUnit)
	return nil
}

func (spec *memWorkSpec) Coordinate() *memCoordinate {
	return spec.namespace.coordinate
}
