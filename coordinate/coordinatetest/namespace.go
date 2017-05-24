// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
)

// TestNamespaceTrivial checks that a namespace's name matches the test name.
func (s *Suite) TestNamespaceTrivial() {
	sts := SimpleTestSetup{NamespaceName: "TestNamespaceTrivial"}
	sts.SetUp(s)
	defer sts.TearDown(s)

	s.Equal("TestNamespaceTrivial", sts.Namespace.Name())
}

// TestNamespaces does some basic tests on the namespace list call.
// If this is run against a shared server, it may not be possible to
// assert that no namespaces beyond the specific test namespace exist,
// so this only verifies that the requested namespace is present.
func (s *Suite) TestNamespaces() {
	sts := SimpleTestSetup{NamespaceName: "TestNamespaces"}
	sts.SetUp(s)
	defer sts.TearDown(s)

	namespaces, err := s.Coordinate.Namespaces()
	if s.NoError(err) {
		if s.Contains(namespaces, sts.NamespaceName) {
			s.Equal(sts.NamespaceName, namespaces[sts.NamespaceName].Name())
		}
	}
}

// TestSpecCreateDestroy performs basic work spec lifetime tests.
func (s *Suite) TestSpecCreateDestroy() {
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
	sts.SetUp(s)
	defer sts.TearDown(s)
	name = "spec"

	spec, err = sts.Namespace.WorkSpec(name)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name}, err)

	names, err = sts.Namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 0)
	}

	dict = map[string]interface{}{
		"name":   name,
		"min_gb": 0.1,
	}
	spec, err = sts.Namespace.SetWorkSpec(dict)
	if s.NoError(err) && s.NotNil(spec) {
		s.Equal(name, spec.Name())
	}

	spec, err = sts.Namespace.WorkSpec(name)
	if s.NoError(err) && s.NotNil(spec) {
		s.Equal(name, spec.Name())
	}

	s.DataMatches(spec, dict)

	names, err = sts.Namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Equal([]string{name}, names)
	}

	err = sts.Namespace.DestroyWorkSpec(name)
	s.NoError(err)

	spec, err = sts.Namespace.WorkSpec(name)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name}, err)

	names, err = sts.Namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 0)
	}

	err = sts.Namespace.DestroyWorkSpec(name)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name}, err)
}

// TestSpecErrors checks for errors on malformed work specs.
func (s *Suite) TestSpecErrors() {
	namespace, err := s.Coordinate.Namespace("TestSpecErrors")
	if !s.NoError(err) {
		return
	}
	defer namespace.Destroy()

	_, err = namespace.SetWorkSpec(map[string]interface{}{})
	s.Exactly(coordinate.ErrNoWorkSpecName, err)

	_, err = namespace.SetWorkSpec(map[string]interface{}{"name": 4})
	s.Exactly(coordinate.ErrBadWorkSpecName, err)
}

// TestTwoWorkSpecsBasic ensures that two work specs can be created
// and have independent lifetimes.
func (s *Suite) TestTwoWorkSpecsBasic() {
	var (
		err          error
		dict1, dict2 map[string]interface{}
		name1, name2 string
		names        []string
		spec         coordinate.WorkSpec
	)

	namespace, err := s.Coordinate.Namespace("TestTwoWorkSpecsBasic")
	if !s.NoError(err) {
		return
	}
	defer namespace.Destroy()

	name1 = "spec1"
	name2 = "spec2"

	dict1 = map[string]interface{}{"name": name1, "min_gb": 1}
	dict2 = map[string]interface{}{"name": name2, "min_gb": 2}

	names, err = namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 0)
	}

	spec, err = namespace.SetWorkSpec(dict1)
	s.NoError(err)

	names, err = namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Equal([]string{name1}, names)
	}

	spec, err = namespace.SetWorkSpec(dict2)
	s.NoError(err)

	names, err = namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 2)
		s.Contains(names, name1)
		s.Contains(names, name2)
	}

	spec, err = namespace.WorkSpec(name1)
	if s.NoError(err) && s.NotNil(spec) {
		s.Equal(name1, spec.Name())
	}

	spec, err = namespace.WorkSpec(name2)
	if s.NoError(err) && s.NotNil(spec) {
		s.Equal(name2, spec.Name())
	}

	err = namespace.DestroyWorkSpec(name1)
	s.NoError(err)

	names, err = namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 1)
		s.Contains(names, name2)
	}

	spec, err = namespace.WorkSpec(name1)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name1}, err)

	spec, err = namespace.WorkSpec(name2)
	if s.NoError(err) && s.NotNil(spec) {
		s.Equal(name2, spec.Name())
	}

	err = namespace.DestroyWorkSpec(name2)
	s.NoError(err)

	names, err = namespace.WorkSpecNames()
	if s.NoError(err) {
		s.Len(names, 0)
	}

	spec, err = namespace.WorkSpec(name1)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name1}, err)

	spec, err = namespace.WorkSpec(name2)
	s.Equal(coordinate.ErrNoSuchWorkSpec{Name: name2}, err)
}
