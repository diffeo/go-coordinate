// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"gopkg.in/check.v1"
	"time"
)

// HelperSuite encapsulates the helper tests.
type HelperSuite struct {
	Now time.Time
}

func init() {
	check.Suite(&HelperSuite{
		Now: time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC),
	})
}

func (s *HelperSuite) TestOutputStrings(c *check.C) {
	items := ExtractWorkUnitOutput([]interface{}{"first", "second"}, s.Now)
	c.Check(items, check.DeepEquals, map[string]AddWorkUnitItem{
		"first": AddWorkUnitItem{
			Key:  "first",
			Data: map[string]interface{}{},
		},
		"second": AddWorkUnitItem{
			Key:  "second",
			Data: map[string]interface{}{},
		},
	})
}

func (s *HelperSuite) TestOutputMap(c *check.C) {
	items := ExtractWorkUnitOutput(map[string]interface{}{
		"first":  map[string]interface{}{},
		"second": map[string]interface{}{"k": "v"},
	}, s.Now)
	c.Check(items, check.DeepEquals, map[string]AddWorkUnitItem{
		"first": AddWorkUnitItem{
			Key:  "first",
			Data: map[string]interface{}{},
		},
		"second": AddWorkUnitItem{
			Key:  "second",
			Data: map[string]interface{}{"k": "v"},
		},
	})
}

func (s *HelperSuite) TestOutputLists(c *check.C) {
	items := ExtractWorkUnitOutput([]interface{}{
		[]interface{}{"a"},
		[]interface{}{"b", map[string]interface{}{"k": "v"}},
		[]interface{}{"c", map[string]interface{}{}, map[string]interface{}{"priority": 10}},
		[]interface{}{"d", map[string]interface{}{}, map[string]interface{}{"delay": 90}},
		[]interface{}{"e", map[string]interface{}{}, map[string]interface{}{}, 20.0},
	}, s.Now)
	then := s.Now.Add(time.Duration(90) * time.Second)
	c.Check(items, check.DeepEquals, map[string]AddWorkUnitItem{
		"b": AddWorkUnitItem{
			Key:  "b",
			Data: map[string]interface{}{"k": "v"},
		},
		"c": AddWorkUnitItem{
			Key:  "c",
			Data: map[string]interface{}{},
			Meta: WorkUnitMeta{Priority: 10},
		},
		"d": AddWorkUnitItem{
			Key:  "d",
			Data: map[string]interface{}{},
			Meta: WorkUnitMeta{NotBefore: then},
		},
		"e": AddWorkUnitItem{
			Key:  "e",
			Data: map[string]interface{}{},
			Meta: WorkUnitMeta{Priority: 20},
		},
	})
}
