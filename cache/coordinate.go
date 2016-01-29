// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package cache provides name-based caching of Coordinate objects.
// The cache wraps some other Coordinate backend.  Most methods on
// most objects simply pass through to their underlying objects, but
// methods that fetch an object by name will generally return a cached
// object, if they have one available.
//
// Object identity
//
// All cached objects wrap specific objects from some other backend.
// However, cached objects generally will use name identity, not any
// other sort of object identity.  A given cached WorkUnit object, for
// instance, will always refer to a work unit in a given name, in a
// work spec with a given name, in a namespace with a given name, even
// if these objects are deleted and recreated.
//
// In some cases this can result in this code succeeding where uncached
// code might return ErrGone:
//
//     workUnit := workSpec.WorkUnit("foo")
//     status, err := workUnit.Status()
//     fmt.Printf("err=%v status=%v\n", err, status)
//
//     workSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
//     workSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
//
//     status, err = workUnit.Status()
//     fmt.Printf("err=%v status=%v\n", err, status)
//
// In most backends, the second call to Status will fail with ErrGone,
// since the work unit was deleted.  This backend will always find the
// recreated work unit.
//
// Caveats
//
// Attempts are neither wrapped nor cached.  There is not a good way to
// find them.  Of the two planned consumers of this package, the CBOR-RPC
// jobserver code always uses the active attempt, and the REST server has
// its own verbose way of finding attempts.
//
// Functions that return slices or maps of other objects will
// generally return unwrapped, uncached objects.  This can mean that
// it is possible to get a mix of objects from two different
// Coordinate backends, which can very occasionally cause problems
// (most notably if you force an attempt for a specific work unit via
// Worker.MakeAttempt).
//
// The WorkSpec functions that operate on generic queries will not
// attempt to reconcile themselves with the local cache.  As a
// variation on the above code:
//
//     workSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
//     workSpec.WorkUnit("foo")
//     workSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
//     workUnit, err := workSpec.WorkUnit("foo")
//
// This will succeed on this backend, since it can returned the cached
// work unit.  Attempts to use the work unit will return ErrGone.
// (Consider slightly less contrived cases where the "delete" call
// happens from another system, perhaps via an administrator action.)
package cache

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

type cache struct {
	backend    coordinate.Coordinate
	namespaces *lru
}

// New creates a new caching backend, wrapping some other backend.
func New(backend coordinate.Coordinate) coordinate.Coordinate {
	return &cache{
		backend:    backend,
		namespaces: newLRU(32),
	}
}

func (cache *cache) Namespace(name string) (coordinate.Namespace, error) {
	ns, err := cache.namespaces.Get(name, func(n string) (named, error) {
		obj, err := cache.backend.Namespace(n)
		return newNamespace(obj, cache), err
	})
	if err != nil {
		return nil, err
	}
	return ns.(coordinate.Namespace), nil
}

func (cache *cache) invalidate(name string) {
	cache.namespaces.Remove(name)
}

func (cache *cache) Namespaces() (map[string]coordinate.Namespace, error) {
	return cache.backend.Namespaces()
}
