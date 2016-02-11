// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache_test

import (
	"github.com/diffeo/go-coordinate/cache"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/stretchr/testify/assert"
	"testing"
)

type CacheAssertions struct {
	*assert.Assertions
	Backend    coordinate.Coordinate
	Coordinate coordinate.Coordinate
}

func NewCacheAssertions(t assert.TestingT) *CacheAssertions {
	backend := memory.New()
	return &CacheAssertions{
		assert.New(t),
		backend,
		cache.New(backend),
	}
}

// Namespace creates a namespace; if it fails, fail the test.
func (a *CacheAssertions) Namespace(name string) coordinate.Namespace {
	ns, err := a.Coordinate.Namespace("")
	if !a.NoError(err, "error fetching namespace") {
		a.FailNow("cannot create namespace")
	}
	return ns
}

// WorkSpec creates a work spec in the named namespace; if it fails,
// fail the test.
func (a *CacheAssertions) WorkSpec(ns coordinate.Namespace, name string) coordinate.WorkSpec {
	workSpec, err := ns.SetWorkSpec(map[string]interface{}{
		"name": name,
	})
	if !a.NoError(err, "error creating work spec") {
		a.FailNow("cannot create work spec")
	}
	return workSpec
}

// WorkUnit creates a work unit in the named work spec; if it fails,
// fail the test.
func (a *CacheAssertions) WorkUnit(workSpec coordinate.WorkSpec, name string) coordinate.WorkUnit {
	workUnit, err := workSpec.AddWorkUnit(name, map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !a.NoError(err, "error creating work unit") {
		a.FailNow("cannot create work unit")
	}
	return workUnit
}

// TestNamespaceDeletion validates cases where a namespace is
// destroyed and recreated.
func TestNamespaceDeletion(t *testing.T) {
	a := NewCacheAssertions(t)
	ns := a.Namespace("")
	a.WorkSpec(ns, "spec")

	err := ns.Destroy()
	if !a.NoError(err, "error destroying namespace") {
		return
	}

	// At this point ns points to a destroyed object, but we can
	// always recreate the namespace.  On the other hand, the
	// Destroy implementation is clever enough to know that it has
	// destroyed all of its work specs.  So what we should get
	// back is an error that says the namespace exists, but it
	// can't find the work spec.  (ErrGone is wrong.)
	_, err = ns.WorkSpec("spec")
	if a.Error(err, "namespace found destroyed work spec") &&
		a.NotEqual(coordinate.ErrGone, err, "namespace failed to refresh itself") {
		a.Equal(coordinate.ErrNoSuchWorkSpec{Name: "spec"}, err)
	}
}

func TestWorkSpecDeletion(t *testing.T) {
	a := NewCacheAssertions(t)
	ns := a.Namespace("")
	spec := a.WorkSpec(ns, "spec")

	// Having this work unit will help us distinguish "old" from "new"
	a.WorkUnit(spec, "unit")

	// Ask the namespace to destroy the work spec, then recreate
	// it; we should see a new, valid, empty object
	err := ns.DestroyWorkSpec("spec")
	if !a.NoError(err, "error destroying work spec") {
		return
	}
	a.WorkSpec(ns, "spec")

	units, err := spec.WorkUnits(coordinate.WorkUnitQuery{})
	if a.NoError(err, "error retrieving work units") {
		a.Len(units, 0, "should get empty unit list back")
	}
}

func TestWorkSpecDeleteNamespace(t *testing.T) {
	a := NewCacheAssertions(t)
	ns := a.Namespace("")
	spec := a.WorkSpec(ns, "spec")

	// Having this work unit will help us distinguish "old" from "new"
	a.WorkUnit(spec, "unit")

	// Blow up the whole namespace, but then let it recreate itself
	err := ns.Destroy()
	if !a.NoError(err, "error destroying namespace") {
		return
	}
	a.WorkSpec(ns, "spec")

	units, err := spec.WorkUnits(coordinate.WorkUnitQuery{})
	if a.NoError(err, "error retrieving work units") {
		a.Len(units, 0, "should get empty unit list back")
	}
}

// TestWorkUnitDeletion tests that, if we delete a work unit and then
// retrieve it, it works correctly.
func TestWorkUnitDeletion(t *testing.T) {
	a := NewCacheAssertions(t)
	ns := a.Namespace("")
	spec := a.WorkSpec(ns, "spec")
	// This will get cached in spec
	a.WorkUnit(spec, "unit")

	// Meanwhile, back on the ranch...
	bns, err := a.Backend.Namespace("")
	if a.NoError(err, "error fetching backend namespace") {
		bspec, err := bns.WorkSpec("spec")
		if a.NoError(err, "error fetching backend work spec") {
			_, err := bspec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
			a.NoError(err, "error deleting work units")
			_, err = bspec.AddWorkUnit("unit", map[string]interface{}{"k": "v"}, coordinate.WorkUnitMeta{})
			a.NoError(err, "error recreating work unit")
		}
	}

	// Now we get to fetch the cached unit
	unit, err := spec.WorkUnit("unit")
	// (this is *probably* the original unit)
	if a.NoError(err, "error fetching work unit") {
		data, err := unit.Data()
		if a.NoError(err, "error fetching work unit data") {
			a.Contains(data, "k", "got back the old work unit data")
		}
	}
}

func TestWorkerChildren(t *testing.T) {
	a := NewCacheAssertions(t)
	ns := a.Namespace("")

	bns, err := a.Backend.Namespace("")
	if !a.NoError(err) {
		return
	}
	bparent, err := bns.Worker("parent")
	if !a.NoError(err) {
		return
	}
	bchild, err := bns.Worker("child")
	if !a.NoError(err) {
		return
	}
	err = bchild.SetParent(bparent)
	if !a.NoError(err) {
		return
	}

	parent, err := ns.Worker("parent")
	if a.NoError(err) {
		a.Equal(bparent.Name(), parent.Name())
		children, err := parent.Children()
		if a.NoError(err) && a.Len(children, 1) {
			a.Equal("child", children[0].Name())
		}
	}
}
