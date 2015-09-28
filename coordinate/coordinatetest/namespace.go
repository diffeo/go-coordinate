package coordinatetest

import (
	"github.com/dmaze/goordinate/coordinate"
	"gopkg.in/check.v1"
)

// TestNamespaceTrivial checks that a namespace's name matches the test name.
func (s *Suite) TestNamespaceTrivial(c *check.C) {
	c.Assert(s.Namespace.Name(), check.Equals, c.TestName())
}

// TestSpecCreateDestroy performs basic work spec lifetime tests.
func (s *Suite) TestSpecCreateDestroy(c *check.C) {
	var (
		data  map[string]interface{}
		dict  map[string]interface{}
		spec  coordinate.WorkSpec
		name  string
		names []string
		err   error
	)
	name = "spec"

	spec, err = s.Namespace.WorkSpec(name)
	c.Check(err, check.DeepEquals,
		coordinate.ErrNoSuchWorkSpec{Name: name})

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.HasLen, 0)

	dict = map[string]interface{}{
		"name":   name,
		"min_gb": 0.1,
	}
	spec, err = s.Namespace.SetWorkSpec(dict)
	c.Assert(err, check.IsNil)
	c.Check(spec, check.NotNil)
	c.Check(spec.Name(), check.Equals, name)

	spec, err = s.Namespace.WorkSpec(name)
	c.Assert(err, check.IsNil)
	c.Check(spec, check.NotNil)
	c.Check(spec.Name(), check.Equals, name)

	data, err = spec.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, dict)

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.DeepEquals, []string{name})

	err = s.Namespace.DestroyWorkSpec(name)
	c.Check(err, check.IsNil)

	spec, err = s.Namespace.WorkSpec(name)
	c.Check(err, check.DeepEquals,
		coordinate.ErrNoSuchWorkSpec{Name: name})

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.HasLen, 0)
}

// TestTwoWorkSpecsBasic ensures that two work specs can be created
// and have independent lifetimes.
func (s *Suite) TestTwoWorkSpecsBasic(c *check.C) {
	var (
		err          error
		dict1, dict2 map[string]interface{}
		name1, name2 string
		names        []string
		spec         coordinate.WorkSpec
	)
	name1 = "spec1"
	name2 = "spec2"

	dict1 = map[string]interface{}{"name": name1, "min_gb": 1}
	dict2 = map[string]interface{}{"name": name2, "min_gb": 2}

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.HasLen, 0)

	spec, err = s.Namespace.SetWorkSpec(dict1)
	c.Assert(err, check.IsNil)

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.DeepEquals, []string{name1})

	spec, err = s.Namespace.SetWorkSpec(dict2)
	c.Assert(err, check.IsNil)

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.HasLen, 2)
	if len(names) > 0 {
		if names[0] == name1 {
			c.Check(names, check.DeepEquals, []string{name1, name2})
		} else {
			c.Check(names, check.DeepEquals, []string{name2, name1})
		}
	}

	spec, err = s.Namespace.WorkSpec(name1)
	c.Assert(err, check.IsNil)
	c.Assert(spec, check.NotNil)
	c.Check(spec.Name(), check.Equals, name1)

	spec, err = s.Namespace.WorkSpec(name2)
	c.Assert(err, check.IsNil)
	c.Assert(spec, check.NotNil)
	c.Check(spec.Name(), check.Equals, name2)

	err = s.Namespace.DestroyWorkSpec(name1)
	c.Assert(err, check.IsNil)

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.DeepEquals, []string{name2})

	spec, err = s.Namespace.WorkSpec(name1)
	c.Check(err, check.DeepEquals, coordinate.ErrNoSuchWorkSpec{Name: name1})

	spec, err = s.Namespace.WorkSpec(name2)
	c.Assert(err, check.IsNil)
	c.Assert(spec, check.NotNil)
	c.Check(spec.Name(), check.Equals, name2)

	err = s.Namespace.DestroyWorkSpec(name2)
	c.Assert(err, check.IsNil)

	names, err = s.Namespace.WorkSpecNames()
	c.Assert(err, check.IsNil)
	c.Check(names, check.HasLen, 0)

	spec, err = s.Namespace.WorkSpec(name1)
	c.Check(err, check.DeepEquals, coordinate.ErrNoSuchWorkSpec{Name: name1})

	spec, err = s.Namespace.WorkSpec(name2)
	c.Check(err, check.DeepEquals, coordinate.ErrNoSuchWorkSpec{Name: name2})
}
