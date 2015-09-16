package memory

import "github.com/dmaze/goordinate/coordinate"

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

func (spec *memWorkSpec) WorkUnits(names []string) (map[string]coordinate.WorkUnit, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	result := make(map[string]coordinate.WorkUnit)
	for _, name := range names {
		if unit, present := spec.workUnits[name]; present {
			result[name] = unit
		}
	}
	return result, nil
}

func (spec *memWorkSpec) AllWorkUnits(start uint, limit uint) (map[string]coordinate.WorkUnit, error) {
	globalLock(spec)
	defer globalUnlock(spec)

	result := make(map[string]coordinate.WorkUnit)
	for _, unit := range spec.workUnits {
		if start > 0 {
			start--
		} else if limit > 0 {
			result[unit.Name()] = unit
			limit--
		}
	}
	return result, nil
}

func (spec *memWorkSpec) Coordinate() *memCoordinate {
	return spec.namespace.coordinate
}
