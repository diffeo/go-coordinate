// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

type namespace struct {
	namespace  coordinate.Namespace
	coordinate *cache
	workSpecs  *lru
	workers    *lru
}

func newNamespace(upstream coordinate.Namespace, coordinate *cache) *namespace {
	return &namespace{
		namespace:  upstream,
		coordinate: coordinate,
		workSpecs:  newLRU(64),
		workers:    newLRU(256),
	}
}

// invalidateWorkSpec removes a work spec name from the cache.
func (ns *namespace) invalidateWorkSpec(name string) {
	ns.workSpecs.Remove(name)
}

// wrapWorker returns a cache.worker object for a specific upstream
// Worker.
func (ns *namespace) wrapWorker(upstream coordinate.Worker) *worker {
	// This cannot fail: it can only fail if the embedded function
	// returns an error, and the embedded function never fails
	downstream, _ := ns.workers.Get(upstream.Name(), func(string) (named, error) {
		return newWorker(upstream, ns), nil
	})
	return downstream.(*worker)
}

// invalidateWorker removes a worker name from the cache.
func (ns *namespace) invalidateWorker(name string) {
	ns.workers.Remove(name)
}

// refresh re-fetches the upstream object if possible.  This should be
// called when code strongly expects the cached object is invalid,
// for instance because a method has returned ErrGone.
//
// On success, ns.namespace points at a newly fetched valid object,
// this namespace's caches are cleared, this object remains the cached
// namespace for its name in the root cache, and returns nil.  On
// error returns the error from trying to fetch the namespace.  Note
// that the semantics of fetching a namespace are that it always
// succeeds, potentially creating it, so this should "usually" work.
func (ns *namespace) refresh() error {
	name := ns.namespace.Name()
	newNS, err := ns.coordinate.backend.Namespace(name)
	if err == nil {
		ns.namespace = newNS
		ns.workSpecs = newLRU(64)
		ns.workers = newLRU(256)
		return nil
	}
	ns.coordinate.invalidate(name)
	return err
}

// withNamespace calls some function with the current upstream
// namespace.  If that operation returns ErrGone, tries refreshing
// this object then trying again.  Note that if there is an error
// doing the refresh, that error is discarded, and the original
// ErrGone is returned (which will be more meaningful to the caller).
func (ns *namespace) withNamespace(f func(coordinate.Namespace) error) error {
	for {
		err := f(ns.namespace)
		if err != coordinate.ErrGone {
			return err
		}
		err = ns.refresh()
		if err != nil {
			return coordinate.ErrGone
		}
	}
}

func (ns *namespace) Name() string {
	return ns.namespace.Name()
}

func (ns *namespace) Destroy() error {
	name := ns.namespace.Name()
	err := ns.namespace.Destroy()
	// If that succeeded, we may as well invalidate everything
	if err == nil {
		ns.coordinate.invalidate(name)
		ns.workSpecs = newLRU(64)
		ns.workers = newLRU(256)
	}
	return err
}

func (ns *namespace) SetWorkSpec(data map[string]interface{}) (workSpec coordinate.WorkSpec, err error) {
	err = ns.withNamespace(func(namespace coordinate.Namespace) error {
		var err error
		workSpec, err = namespace.SetWorkSpec(data)
		if err == nil {
			workSpec = newWorkSpec(workSpec, ns)
			ns.workSpecs.Put(workSpec)
		}
		return err
	})
	return
}

func (ns *namespace) WorkSpec(name string) (workSpec coordinate.WorkSpec, err error) {
	var downstream named
	downstream, err = ns.workSpecs.Get(name, func(n string) (named, error) {
		var upstream coordinate.WorkSpec
		err := ns.withNamespace(func(namespace coordinate.Namespace) error {
			var err error
			upstream, err = namespace.WorkSpec(n)
			if err == nil {
				upstream = newWorkSpec(upstream, ns)
			}
			return err
		})
		return upstream, err
	})
	if err == nil {
		workSpec = downstream.(coordinate.WorkSpec)
	}
	return
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	err := ns.withNamespace(func(namespace coordinate.Namespace) error {
		return namespace.DestroyWorkSpec(name)
	})
	if err == nil {
		ns.workSpecs.Remove(name)
	}
	return err
}

func (ns *namespace) WorkSpecNames() (names []string, err error) {
	err = ns.withNamespace(func(namespace coordinate.Namespace) error {
		var err error
		names, err = ns.namespace.WorkSpecNames()
		return err
	})
	return
}

func (ns *namespace) Worker(name string) (coordinate.Worker, error) {
	worker, err := ns.workers.Get(name, func(n string) (named, error) {
		var upstream coordinate.Worker
		var err error
		err = ns.withNamespace(func(namespace coordinate.Namespace) error {
			var err error
			upstream, err = namespace.Worker(n)
			return err
		})
		if err != nil {
			return nil, err
		}
		return newWorker(upstream, ns), nil
	})
	if err != nil {
		return nil, err
	}
	return worker.(coordinate.Worker), err
}

func (ns *namespace) Workers() (workers map[string]coordinate.Worker, err error) {
	err = ns.withNamespace(func(namespace coordinate.Namespace) error {
		var err error
		workers, err = namespace.Workers()
		return err
	})
	if err == nil {
		for name, upstream := range workers {
			workers[name] = ns.wrapWorker(upstream)
		}
	}
	return
}
