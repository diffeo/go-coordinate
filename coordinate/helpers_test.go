// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// now is a reference datestamp for tests.
var now = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)

func TestOutputStrings(t *testing.T) {
	items := ExtractWorkUnitOutput([]interface{}{"first", "second"}, now)
	assert.Equal(t, map[string]AddWorkUnitItem{
		"first": AddWorkUnitItem{
			Key:  "first",
			Data: map[string]interface{}{},
		},
		"second": AddWorkUnitItem{
			Key:  "second",
			Data: map[string]interface{}{},
		},
	}, items)
}

func TestOutputMap(t *testing.T) {
	items := ExtractWorkUnitOutput(map[string]interface{}{
		"first":  map[string]interface{}{},
		"second": map[string]interface{}{"k": "v"},
	}, now)
	assert.Equal(t, map[string]AddWorkUnitItem{
		"first": AddWorkUnitItem{
			Key:  "first",
			Data: map[string]interface{}{},
		},
		"second": AddWorkUnitItem{
			Key:  "second",
			Data: map[string]interface{}{"k": "v"},
		},
	}, items)
}

func TestOutputLists(t *testing.T) {
	items := ExtractWorkUnitOutput([]interface{}{
		[]interface{}{"a"},
		[]interface{}{"b", map[string]interface{}{"k": "v"}},
		[]interface{}{"c", map[string]interface{}{}, map[string]interface{}{"priority": 10}},
		[]interface{}{"d", map[string]interface{}{}, map[string]interface{}{"delay": 90}},
		[]interface{}{"e", map[string]interface{}{}, map[string]interface{}{}, 20.0},
	}, now)
	then := now.Add(90 * time.Second)
	assert.Equal(t, map[string]AddWorkUnitItem{
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
	}, items)
}
