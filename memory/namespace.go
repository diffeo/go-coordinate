package memory

import (
	"github.com/dmaze/goordinate/coordinate"
)

// namespace is a container type for a coordinate.Namespace.
type namespace struct {
	name       string
	coordinate *memCoordinate
	workSpecs  map[string]*workSpec
	workers    map[string]*worker
}

func newNamespace(coordinate *memCoordinate, name string) *namespace {
	return &namespace{
		name:       name,
		coordinate: coordinate,
		workSpecs:  make(map[string]*workSpec),
		workers:    make(map[string]*worker),
	}
}

// coordinate.Namespace interface:

func (ns *namespace) Name() string {
	return ns.name
}

func (ns *namespace) Destroy() error {
	globalLock(ns)
	defer globalUnlock(ns)

	delete(ns.coordinate.namespaces, ns.name)
	return nil
}

func (ns *namespace) SetWorkSpec(workSpec map[string]interface{}) (coordinate.WorkSpec, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	nameI := workSpec["name"]
	if nameI == nil {
		return nil, coordinate.ErrNoWorkSpecName
	}
	name, ok := nameI.(string)
	if !ok {
		return nil, coordinate.ErrBadWorkSpecName
	}
	spec := ns.workSpecs[name]
	if spec == nil {
		spec = newWorkSpec(ns, name)
		ns.workSpecs[name] = spec
	}
	err := spec.setData(workSpec)
	if err != nil {
		return nil, err
	}
	return spec, nil
}

func (ns *namespace) WorkSpec(name string) (coordinate.WorkSpec, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	workSpec, present := ns.workSpecs[name]
	if !present {
		return nil, coordinate.ErrNoSuchWorkSpec{Name: name}
	}
	return workSpec, nil
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	globalLock(ns)
	defer globalUnlock(ns)

	_, present := ns.workSpecs[name]
	if present {
		delete(ns.workSpecs, name)
		return nil
	}
	return coordinate.ErrNoSuchWorkSpec{Name: name}
}

func (ns *namespace) WorkSpecNames() ([]string, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	result := make([]string, 0, len(ns.workSpecs))
	for name := range ns.workSpecs {
		result = append(result, name)
	}
	return result, nil
}

func (ns *namespace) Worker(name string) (coordinate.Worker, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	worker := ns.workers[name]
	if worker == nil {
		worker = newWorker(ns, name)
		ns.workers[name] = worker
	}
	return worker, nil
}

// memory.coordinable interface:

func (ns *namespace) Coordinate() *memCoordinate {
	return ns.coordinate
}
