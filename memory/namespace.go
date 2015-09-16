package memory

import "github.com/dmaze/goordinate/coordinate"

// memNamespace is a container type for a coordinate.Namespace.
type memNamespace struct {
	name       string
	coordinate *memCoordinate
	workSpecs  map[string]*memWorkSpec
	workers    map[string]*memWorker
}

func newNamespace(coordinate *memCoordinate, name string) *memNamespace {
	return &memNamespace {
		name: name,
		coordinate: coordinate,
		workSpecs: make(map[string]*memWorkSpec),
		workers: make(map[string]*memWorker),
	}
}

// coordinate.Namespace interface:

func (ns *memNamespace) Name() string {
	return ns.name
}

func (ns *memNamespace) Destroy() error {
	globalLock(ns); defer globalUnlock(ns)

	delete(ns.coordinate.namespaces, ns.name)
	return nil
}

func (ns *memNamespace) SetWorkSpec(workSpec map[string]interface{}) (coordinate.WorkSpec, error) {
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

func (ns *memNamespace) WorkSpec(name string) (coordinate.WorkSpec, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	// Remember: missing work spec is not an error, just return nil
	workSpec := ns.workSpecs[name]
	if workSpec == nil {
		return nil, nil
	}
	return workSpec, nil
}

func (ns *memNamespace) DestroyWorkSpec(name string) error {
	globalLock(ns)
	defer globalUnlock(ns)

	_, present := ns.workSpecs[name]
	if present {
		delete(ns.workSpecs, name)
		return nil
	}
	return coordinate.ErrNoSuchWorkSpec{Name: name}
}

func (ns *memNamespace) WorkSpecNames() ([]string, error) {
	globalLock(ns)
	defer globalUnlock(ns)

	result := make([]string, 0, len(ns.workSpecs))
	for name := range ns.workSpecs {
		result = append(result, name)
	}
	return result, nil
}

func (ns *memNamespace) Worker(name string) (coordinate.Worker, error) {
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

func (ns *memNamespace) Coordinate() *memCoordinate {
	return ns.coordinate
}
