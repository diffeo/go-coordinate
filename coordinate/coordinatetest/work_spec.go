// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
	"time"
)

// TestChangeSpecData tests WorkSpec.SetData().
func (s *Suite) TestChangeSpecData(c *check.C) {
	var (
		err  error
		data map[string]interface{}
		spec coordinate.WorkSpec
	)

	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)
	c.Check(spec.Name(), check.Equals, "spec")

	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 2,
		"foo":    "bar",
	})
	c.Assert(err, check.IsNil)

	data, err = spec.Data()
	c.Assert(err, check.IsNil)
	c.Check(data["name"], check.Equals, "spec")
	c.Check(data["min_gb"], Like, 2)
	c.Check(data["foo"], check.Equals, "bar")

	err = spec.SetData(map[string]interface{}{})
	c.Assert(err, check.NotNil)
	c.Check(err, check.Equals, coordinate.ErrNoWorkSpecName)

	err = spec.SetData(map[string]interface{}{
		"name":   "name",
		"min_gb": 3,
	})
	c.Assert(err, check.NotNil)
	c.Check(err, check.Equals, coordinate.ErrChangedName)

	data, err = spec.Data()
	c.Assert(err, check.IsNil)
	c.Check(data["name"], check.Equals, "spec")
	c.Check(data["min_gb"], Like, 2)
	c.Check(data["foo"], check.Equals, "bar")
}

// TestDataEmptyList verifies that an empty list gets preserved in the
// work spec data, and not remapped to nil.
func (s *Suite) TestDataEmptyList(c *check.C) {
	emptyList := []string{}
	c.Assert(emptyList, check.NotNil)
	c.Assert(emptyList, check.HasLen, 0)

	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
		"config": map[string]interface{}{
			"empty_list": emptyList,
		},
	})
	c.Assert(err, check.IsNil)

	data, err := spec.Data()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.NotNil)
	c.Assert(data["config"], check.NotNil)
	var found interface{}
	switch config := data["config"].(type) {
	case map[string]interface{}:
		found = config["empty_list"]
	case map[interface{}]interface{}:
		found = config["empty_list"]
	default:
		c.FailNow()
	}
	c.Check(found, check.NotNil)
	c.Check(found, check.HasLen, 0)
}

// TestDefaultMeta tests that WorkSpec.Meta gets the correct defaults,
// which in a couple of cases are not zero values.
func (s *Suite) TestDefaultMeta(c *check.C) {
	var (
		err  error
		spec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)
	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 0)
	c.Check(meta.Weight, check.Equals, 20)
	c.Check(meta.Paused, check.Equals, false)
	c.Check(meta.Continuous, check.Equals, false)
	c.Check(meta.CanBeContinuous, check.Equals, false)
	c.Check(meta.Interval, check.Equals, time.Duration(0))
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 0)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 0)
	c.Check(meta.NextWorkSpecName, check.Equals, "")
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)
}

// TestPrefilledMeta tests that WorkSpec.Meta() fills in correctly from
// "magic" keys in a work spec.
func (s *Suite) TestPrefilledMeta(c *check.C) {
	var (
		err  error
		spec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)
	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":        "spec",
		"min_gb":      1,
		"priority":    10,
		"weight":      100,
		"disabled":    true,
		"continuous":  true,
		"interval":    60,
		"max_running": 10,
		"max_getwork": 1,
		"then":        "spec2",
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 10)
	c.Check(meta.Weight, check.Equals, 100)
	c.Check(meta.Paused, check.Equals, true)
	c.Check(meta.Continuous, check.Equals, true)
	c.Check(meta.CanBeContinuous, check.Equals, true)
	c.Check(meta.Interval, check.Equals, time.Duration(60)*time.Second)
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 10)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 1)
	c.Check(meta.NextWorkSpecName, check.Equals, "spec2")
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)
}

// TestSetDataSetsMeta tests that...yeah
func (s *Suite) TestSetDataSetsMeta(c *check.C) {
	var (
		err  error
		spec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)
	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 0)
	c.Check(meta.Weight, check.Equals, 20)
	c.Check(meta.Paused, check.Equals, false)
	c.Check(meta.Continuous, check.Equals, false)
	c.Check(meta.CanBeContinuous, check.Equals, false)
	c.Check(meta.Interval, check.Equals, time.Duration(0))
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 0)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 0)
	c.Check(meta.NextWorkSpecName, check.Equals, "")
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)

	err = spec.SetData(map[string]interface{}{
		"name":        "spec",
		"min_gb":      1,
		"priority":    10,
		"weight":      100,
		"disabled":    true,
		"continuous":  true,
		"interval":    60,
		"max_running": 10,
		"max_getwork": 1,
		"then":        "spec2",
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 10)
	c.Check(meta.Weight, check.Equals, 100)
	c.Check(meta.Paused, check.Equals, true)
	c.Check(meta.Continuous, check.Equals, true)
	c.Check(meta.CanBeContinuous, check.Equals, true)
	c.Check(meta.Interval, check.Equals, time.Duration(60)*time.Second)
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 10)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 1)
	c.Check(meta.NextWorkSpecName, check.Equals, "spec2")
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)
}

// TestNiceWeight tests the "weight = 20-nice" rule.
func (s *Suite) TestNiceWeight(c *check.C) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   5,
	})
	c.Assert(err, check.IsNil)

	meta, err := spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Weight, check.Equals, 15)

	// Lower bound on weight
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   100,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Weight, check.Equals, 1)

	// No lower bound on niceness
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   -80,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Weight, check.Equals, 100)

	// Weight trumps nice
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"weight": 50,
		"nice":   5,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Weight, check.Equals, 50)

	// Removing either flag resets to default
	// Weight trumps nice
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Weight, check.Equals, 20)
}

// TestSetMeta tests the basic SetMeta() call and a couple of its
// documented oddities.
func (s *Suite) TestSetMeta(c *check.C) {
	var (
		err  error
		spec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)
	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":       "spec",
		"min_gb":     1,
		"continuous": true,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 0)
	c.Check(meta.Weight, check.Equals, 20)
	c.Check(meta.Paused, check.Equals, false)
	c.Check(meta.Continuous, check.Equals, true)
	c.Check(meta.CanBeContinuous, check.Equals, true)
	c.Check(meta.Interval, check.Equals, time.Duration(0))
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 0)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 0)
	c.Check(meta.NextWorkSpecName, check.Equals, "")
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)

	err = spec.SetMeta(coordinate.WorkSpecMeta{
		Priority:            10,
		Weight:              100,
		Paused:              true,
		Continuous:          false,
		CanBeContinuous:     false,
		Interval:            time.Duration(60) * time.Second,
		MaxRunning:          10,
		MaxAttemptsReturned: 1,
		NextWorkSpecName:    "then",
		AvailableCount:      100,
		PendingCount:        50,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Priority, check.Equals, 10)
	c.Check(meta.Weight, check.Equals, 100)
	c.Check(meta.Paused, check.Equals, true)
	c.Check(meta.Continuous, check.Equals, false)
	// Cannot clear "can be continuous" flag
	c.Check(meta.CanBeContinuous, check.Equals, true)
	c.Check(meta.Interval, check.Equals, time.Duration(60)*time.Second)
	c.Check(meta.NextContinuous, SameTime, time.Time{})
	c.Check(meta.MaxRunning, check.Equals, 10)
	c.Check(meta.MaxAttemptsReturned, check.Equals, 1)
	// Cannot change following work spec
	c.Check(meta.NextWorkSpecName, check.Equals, "")
	// Cannot set the counts
	c.Check(meta.AvailableCount, check.Equals, 0)
	c.Check(meta.PendingCount, check.Equals, 0)
}

// TestMetaContinuous specifically checks that you cannot enable the
// "continuous" flag on non-continuous work specs.
func (s *Suite) TestMetaContinuous(c *check.C) {
	var (
		err  error
		spec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)

	// ...also...
	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Continuous, check.Equals, false)
	c.Check(meta.CanBeContinuous, check.Equals, false)

	meta.Continuous = true
	err = spec.SetMeta(meta)
	c.Assert(err, check.IsNil)

	meta, err = spec.Meta(false)
	c.Assert(err, check.IsNil)
	// Cannot set the "continuous" flag
	c.Check(meta.Continuous, check.Equals, false)
	c.Check(meta.CanBeContinuous, check.Equals, false)
}

// TestMetaCounts does basic tests on the "available" and "pending" counts.
func (s *Suite) TestMetaCounts(c *check.C) {
	spec, worker := s.makeWorkSpecAndWorker(c)
	checkCounts := func(available, pending int) {
		meta, err := spec.Meta(true)
		c.Assert(err, check.IsNil)
		c.Check(meta.AvailableCount, check.Equals, available)
		c.Check(meta.PendingCount, check.Equals, pending)
	}
	checkCounts(0, 0)

	// Adding a work unit adds to the available count
	_, err := spec.AddWorkUnit("one", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	checkCounts(1, 0)

	// Starting an attempt makes it pending
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	checkCounts(0, 1)

	// Expiring an attempt makes it available again
	err = attempts[0].Expire(nil)
	c.Assert(err, check.IsNil)
	checkCounts(1, 0)

	// Starting an attempt makes it pending
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	checkCounts(0, 1)

	// Marking an attempt retryable makes it pending again
	err = attempts[0].Retry(nil)
	c.Assert(err, check.IsNil)
	checkCounts(1, 0)

	// Starting an attempt makes it pending
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	checkCounts(0, 1)

	// Finishing an attempt takes it out of the list entirely
	err = attempts[0].Finish(nil)
	c.Assert(err, check.IsNil)
	checkCounts(0, 0)
}
