package memory

import "github.com/dmaze/goordinate/coordinate"

type memWorkSpec struct {
	name      string
	namespace *memNamespace
	metadata  map[string]interface{}
	workUnits map[string]*memWorkUnit
}

func newWorkSpec(namespace *memNamespace, name string) *memWorkSpec {
	return &memWorkSpec{
		name: name,
		namespace: namespace,
		metadata: make(map[string]interface{}),
		workUnits: make(map[string]*memWorkUnit),
	}
}

func (spec *memWorkSpec) Name() string {
	return spec.name
}

func (spec *memWorkSpec) Data() (map[string]interface{}, error) {
	return spec.metadata, nil
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
