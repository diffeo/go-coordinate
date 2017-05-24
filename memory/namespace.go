// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

// namespace is a container type for a coordinate.Namespace.
type namespace struct {
	name       string
	coordinate *memCoordinate
	workSpecs  map[string]*workSpec
	workers    map[string]*worker
	deleted    bool
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
	ns.deleted = true
	return nil
}

func (ns *namespace) do(f func() error) error {
	globalLock(ns)
	defer globalUnlock(ns)

	if ns.deleted {
		return coordinate.ErrGone
	}

	return f()
}

func (ns *namespace) SetWorkSpec(data map[string]interface{}) (spec coordinate.WorkSpec, err error) {
	err = ns.do(func() error {
		nameI := data["name"]
		if nameI == nil {
			return coordinate.ErrNoWorkSpecName
		}
		name, ok := nameI.(string)
		if !ok {
			return coordinate.ErrBadWorkSpecName
		}
		theSpec := ns.workSpecs[name]
		if theSpec == nil {
			theSpec = newWorkSpec(ns, name)
			ns.workSpecs[name] = theSpec
		}
		spec = theSpec
		return theSpec.setData(data)
	})
	return
}

func (ns *namespace) WorkSpec(name string) (spec coordinate.WorkSpec, err error) {
	err = ns.do(func() error {
		var present bool
		spec, present = ns.workSpecs[name]
		if !present {
			return coordinate.ErrNoSuchWorkSpec{Name: name}
		}
		return nil
	})
	return
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	return ns.do(func() error {
		spec, present := ns.workSpecs[name]
		if !present {
			return coordinate.ErrNoSuchWorkSpec{Name: name}
		}
		spec.deleted = true
		delete(ns.workSpecs, name)
		return nil
	})
}

func (ns *namespace) WorkSpecNames() (names []string, err error) {
	err = ns.do(func() error {
		names = make([]string, 0, len(ns.workSpecs))
		for name := range ns.workSpecs {
			names = append(names, name)
		}
		return nil
	})
	return
}

// allMetas retrieves the metadata for all work specs.  This cannot
// fail.  It expects to run within the global lock.
func (ns *namespace) allMetas(withCounts bool) (map[string]*workSpec, map[string]*coordinate.WorkSpecMeta) {
	metas := make(map[string]*coordinate.WorkSpecMeta)
	for name, spec := range ns.workSpecs {
		meta := spec.getMeta(withCounts)
		metas[name] = &meta
	}
	return ns.workSpecs, metas
}

func (ns *namespace) Worker(name string) (worker coordinate.Worker, err error) {
	err = ns.do(func() error {
		var present bool
		worker, present = ns.workers[name]
		if !present {
			ns.workers[name] = newWorker(ns, name)
			worker = ns.workers[name]
		}
		return nil
	})
	return
}

func (ns *namespace) Workers() (workers map[string]coordinate.Worker, err error) {
	// subject to change, see comments in coordinate.go
	err = ns.do(func() error {
		workers = make(map[string]coordinate.Worker)
		for name, worker := range ns.workers {
			workers[name] = worker
		}
		return nil
	})
	return
}

// coordinate.Summarizable interface:

func (ns *namespace) Summarize() (result coordinate.Summary, err error) {
	err = ns.do(func() error {
		result = ns.summarize()
		return nil
	})
	return
}

func (ns *namespace) summarize() coordinate.Summary {
	var result coordinate.Summary
	for _, spec := range ns.workSpecs {
		result = append(result, spec.summarize()...)
	}
	return result
}

// memory.coordinable interface:

func (ns *namespace) Coordinate() *memCoordinate {
	return ns.coordinate
}
