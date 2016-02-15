// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestNamespaceTrivial checks that a namespace's name matches the test name.
func TestNamespaceTrivial(t *testing.T) {
	sts := SimpleTestSetup{NamespaceName: "TestNamespaceTrivial"}
	sts.SetUp(t)
	defer sts.TearDown(t)

	assert.Equal(t, "TestNamespaceTrivial", sts.Namespace.Name())
}

// TestNamespaces does some basic tests on the namespace list call.
// If this is run against a shared server, it may not be possible to
// assert that no namespaces beyond the specific test namespace exist,
// so this only verifies that the requested namespace is present.
func TestNamespaces(t *testing.T) {
	sts := SimpleTestSetup{NamespaceName: "TestNamespaces"}
	sts.SetUp(t)
	defer sts.TearDown(t)

	namespaces, err := Coordinate.Namespaces()
	if assert.NoError(t, err) {
		if assert.Contains(t, namespaces, sts.NamespaceName) {
			assert.Equal(t, sts.NamespaceName, namespaces[sts.NamespaceName].Name())
		}
	}
}

// TestSpecCreateDestroy performs basic work spec lifetime tests.
func TestSpecCreateDestroy(t *testing.T) {
	var (
		dict  map[string]interface{}
		spec  coordinate.WorkSpec
		name  string
		names []string
		err   error
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestSpecCreateDestroy",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)
	name = "spec"

	spec, err = sts.Namespace.WorkSpec(name)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name}, err)

	names, err = sts.Namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 0)
	}

	dict = map[string]interface{}{
		"name":   name,
		"min_gb": 0.1,
	}
	spec, err = sts.Namespace.SetWorkSpec(dict)
	if assert.NoError(t, err) && assert.NotNil(t, spec) {
		assert.Equal(t, name, spec.Name())
	}

	spec, err = sts.Namespace.WorkSpec(name)
	if assert.NoError(t, err) && assert.NotNil(t, spec) {
		assert.Equal(t, name, spec.Name())
	}

	DataMatches(t, spec, dict)

	names, err = sts.Namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Equal(t, []string{name}, names)
	}

	err = sts.Namespace.DestroyWorkSpec(name)
	assert.NoError(t, err)

	spec, err = sts.Namespace.WorkSpec(name)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name}, err)

	names, err = sts.Namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 0)
	}

	err = sts.Namespace.DestroyWorkSpec(name)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name}, err)
}

// TestSpecErrors checks for errors on malformed work specs.
func TestSpecErrors(t *testing.T) {
	namespace, err := Coordinate.Namespace("TestSpecErrors")
	if !assert.NoError(t, err) {
		return
	}
	defer namespace.Destroy()

	_, err = namespace.SetWorkSpec(map[string]interface{}{})
	assert.Exactly(t, coordinate.ErrNoWorkSpecName, err)

	_, err = namespace.SetWorkSpec(map[string]interface{}{"name": 4})
	assert.Exactly(t, coordinate.ErrBadWorkSpecName, err)
}

// TestTwoWorkSpecsBasic ensures that two work specs can be created
// and have independent lifetimes.
func TestTwoWorkSpecsBasic(t *testing.T) {
	var (
		err          error
		dict1, dict2 map[string]interface{}
		name1, name2 string
		names        []string
		spec         coordinate.WorkSpec
	)

	namespace, err := Coordinate.Namespace("TestTwoWorkSpecsBasic")
	if !assert.NoError(t, err) {
		return
	}
	defer namespace.Destroy()

	name1 = "spec1"
	name2 = "spec2"

	dict1 = map[string]interface{}{"name": name1, "min_gb": 1}
	dict2 = map[string]interface{}{"name": name2, "min_gb": 2}

	names, err = namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 0)
	}

	spec, err = namespace.SetWorkSpec(dict1)
	assert.NoError(t, err)

	names, err = namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Equal(t, []string{name1}, names)
	}

	spec, err = namespace.SetWorkSpec(dict2)
	assert.NoError(t, err)

	names, err = namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 2)
		assert.Contains(t, names, name1)
		assert.Contains(t, names, name2)
	}

	spec, err = namespace.WorkSpec(name1)
	if assert.NoError(t, err) && assert.NotNil(t, spec) {
		assert.Equal(t, name1, spec.Name())
	}

	spec, err = namespace.WorkSpec(name2)
	if assert.NoError(t, err) && assert.NotNil(t, spec) {
		assert.Equal(t, name2, spec.Name())
	}

	err = namespace.DestroyWorkSpec(name1)
	assert.NoError(t, err)

	names, err = namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 1)
		assert.Contains(t, names, name2)
	}

	spec, err = namespace.WorkSpec(name1)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name1}, err)

	spec, err = namespace.WorkSpec(name2)
	if assert.NoError(t, err) && assert.NotNil(t, spec) {
		assert.Equal(t, name2, spec.Name())
	}

	err = namespace.DestroyWorkSpec(name2)
	assert.NoError(t, err)

	names, err = namespace.WorkSpecNames()
	if assert.NoError(t, err) {
		assert.Len(t, names, 0)
	}

	spec, err = namespace.WorkSpec(name1)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name1}, err)

	spec, err = namespace.WorkSpec(name2)
	assert.Equal(t, coordinate.ErrNoSuchWorkSpec{Name: name2}, err)
}
