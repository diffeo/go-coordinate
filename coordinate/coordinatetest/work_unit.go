// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
	"time"
)

// TestTrivialWorkUnitFlow tests work unit creation, deletion, and existence.
func (s *Suite) TestTrivialWorkUnitFlow(c *check.C) {
	var (
		count int
		err   error
		units map[string]coordinate.WorkUnit
	)

	sts := SimpleTestSetup{
		WorkSpecName: "spec",
		WorkUnitName: "unit",
	}
	sts.Do(s, c)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units["unit"], check.NotNil)
	c.Check(units["unit"].Name(), check.Equals, "unit")
	c.Check(units["unit"].WorkSpec().Name(), check.Equals, "spec")

	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)

	_, err = sts.WorkSpec.WorkUnit("unit")
	c.Check(err, check.DeepEquals, coordinate.ErrNoSuchWorkUnit{Name: "unit"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)
}

// TestWorkUnitQueries calls WorkSpec.WorkUnits() with various queries.
func (s *Suite) TestWorkUnitQueries(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	_, err := sts.MakeWorkUnits()
	c.Assert(err, check.IsNil)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 7)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished", "pending", "retryable", "delayed"})

	// Get everything, in two batches, sorted
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Limit: 4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 4)
	c.Check(units, HasKeys, []string{"available", "delayed", "expired", "failed"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "failed",
		Limit:        4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 3)
	c.Check(units, HasKeys, []string{"finished", "pending", "retryable"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "retryable",
		Limit:        4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)

	// Get work units by status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 3)
	c.Check(units, HasKeys, []string{"available", "expired", "retryable"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.PendingUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"pending"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FinishedUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"finished"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FailedUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"failed"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.DelayedUnit,
		},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"delayed"})

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"failed", "finished"})

	// Get work units by name
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"available", "failed", "missing"},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"available", "failed"})

	// Get work units by name and status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names:    []string{"available", "retryable", "finished"},
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"available", "retryable"})
}

// TestDeleteWorkUnits is a smaller set of tests for
// WorkSpec.DeleteWorkUnits(), on the assumption that a fair amount of
// code will typically be shared with GetWorkUnits() and because it is
// intrinsically a mutating operation.
func (s *Suite) TestDeleteWorkUnits(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	_, err := sts.MakeWorkUnits()
	c.Assert(err, check.IsNil)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 7)
	c.Check(units, HasKeys, []string{"available", "delayed", "expired",
		"failed", "finished", "pending", "retryable"})

	// Delete by name
	count, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 6)
	c.Check(units, HasKeys, []string{"available", "delayed", "expired",
		"failed", "finished", "pending"})

	// Delete the same thing again; missing name should be a no-op
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 0)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 6)
	c.Check(units, HasKeys, []string{"available", "delayed", "expired",
		"failed", "finished", "pending"})

	// Delete by status
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 2)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 4)
	c.Check(units, HasKeys, []string{"available", "delayed", "expired", "pending"})

	// Delete everything
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 4)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)
}

// TestCountWorkUnitStatus does simple validation on the
// CountWorkUnitStatus call.
func (s *Suite) TestCountWorkUnitStatus(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)
	_, err := sts.MakeWorkUnits()
	c.Assert(err, check.IsNil)

	counts, err := sts.WorkSpec.CountWorkUnitStatus()
	c.Assert(err, check.IsNil)
	c.Check(counts, check.DeepEquals, map[coordinate.WorkUnitStatus]int{
		coordinate.AvailableUnit: 3,
		coordinate.PendingUnit:   1,
		coordinate.FinishedUnit:  1,
		coordinate.FailedUnit:    1,
		coordinate.DelayedUnit:   1,
	})
}

// TestWorkUnitOrder is a very basic test that work units get returned
// in alphabetic order absent any other constraints.
func (s *Suite) TestWorkUnitOrder(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	for _, name := range []string{"c", "b", "a"} {
		_, err := sts.AddWorkUnit(name)
		c.Assert(err, check.IsNil)
	}

	sts.CheckWorkUnitOrder(s, c, "a", "b", "c")
}

// TestWorkUnitPriorityCtor tests that priorities passed in the work unit
// constructor are honored.
func (s *Suite) TestWorkUnitPriorityCtor(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	var units = []struct {
		string
		float64
	}{
		{"a", 0},
		{"b", 10},
		{"c", 0},
	}

	for _, unit := range units {
		workUnit, err := sts.WorkSpec.AddWorkUnit(unit.string, map[string]interface{}{}, coordinate.WorkUnitMeta{Priority: unit.float64})
		c.Assert(err, check.IsNil)
		pri, err := workUnit.Priority()
		c.Assert(err, check.IsNil)
		c.Check(pri, check.Equals, unit.float64)
	}

	sts.CheckWorkUnitOrder(s, c, "b", "a", "c")
}

// TestWorkUnitPrioritySet tests two different ways of setting work unit
// priority.
func (s *Suite) TestWorkUnitPrioritySet(c *check.C) {
	var (
		err      error
		priority float64
		unit     coordinate.WorkUnit
	)
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	unit, err = sts.WorkSpec.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 0.0)

	unit, err = sts.WorkSpec.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
	err = unit.SetPriority(10.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 10.0)

	unit, err = sts.WorkSpec.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
	err = sts.WorkSpec.SetWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"c"},
	}, 20.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 20.0)

	unit, err = sts.WorkSpec.AddWorkUnit("d", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 20.0)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 20.0)
	c.Assert(err, check.IsNil)
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 10.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 30.0)

	unit, err = sts.WorkSpec.WorkUnit("b")
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 10.0)

	sts.CheckWorkUnitOrder(s, c, "d", "c", "b", "a")
}

// TestWorkUnitData validates that the system can store and update
// data.
func (s *Suite) TestWorkUnitData(c *check.C) {
	var (
		data map[string]interface{}
		unit coordinate.WorkUnit
	)
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	_, err = spec.AddWorkUnit("a", map[string]interface{}{
		"name":  "a",
		"value": 1,
	}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	_, err = spec.AddWorkUnit("b", map[string]interface{}{
		"name":  "b",
		"value": 2,
	}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	unit, err = spec.WorkUnit("a")
	c.Assert(err, check.IsNil)
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 2)
	c.Check(data["name"], check.Equals, "a")
	c.Check(data["value"], Like, 1)

	unit, err = spec.WorkUnit("b")
	c.Assert(err, check.IsNil)
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 2)
	c.Check(data["name"], check.Equals, "b")
	c.Check(data["value"], Like, 2)
}

// TestRecreateWorkUnits checks that creating work units that already
// exist works successfully.
func (s *Suite) TestRecreateWorkUnits(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)
	units, err := sts.MakeWorkUnits()
	c.Assert(err, check.IsNil)

	for name := range units {
		unit, err := sts.AddWorkUnit(name)
		c.Assert(err, check.IsNil,
			check.Commentf("name = %v", name))
		// Unless the unit was previously pending, it should be
		// available now
		status, err := unit.Status()
		c.Assert(err, check.IsNil,
			check.Commentf("name = %v", name))
		expected := coordinate.AvailableUnit
		if name == "pending" {
			expected = coordinate.PendingUnit
		}
		c.Check(status, check.Equals, expected,
			check.Commentf("name = %v", name))
	}
}

// TestContinuous creates a continuous work spec but no work units for it.
// Requesting attempts should create a new work unit for it.
func (s *Suite) TestContinuous(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName: "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
		},
	}
	sts.Do(s, c)

	makeAttempt := func(expected int) {
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		c.Check(attempts, check.HasLen, expected)
		for _, attempt := range attempts {
			err = attempt.Finish(nil)
			c.Assert(err, check.IsNil)
		}
	}

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// If we use SetMeta to turn continuous mode off and on, it
	// should affect whether work units come back
	meta, err := sts.WorkSpec.Meta(false)
	c.Assert(err, check.IsNil)
	meta.Continuous = false
	err = sts.WorkSpec.SetMeta(meta)
	c.Assert(err, check.IsNil)
	makeAttempt(0)

	meta.Continuous = true
	err = sts.WorkSpec.SetMeta(meta)
	c.Assert(err, check.IsNil)
	makeAttempt(1)
}

// TestContinuousInterval verifies the operation of a continuous work spec
// that has a minimum respawn frequency.
func (s *Suite) TestContinuousInterval(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName: "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
			"interval":   60,
		},
	}
	sts.Do(s, c)

	makeAttempt := func(expected int) {
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		c.Check(attempts, check.HasLen, expected)
		for _, attempt := range attempts {
			err = attempt.Finish(nil)
			c.Assert(err, check.IsNil)
		}
	}

	start := s.Clock.Now()
	oneMinute := time.Duration(60) * time.Second

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// The next-attempt time should be the start time plus the
	// interval
	meta, err := sts.WorkSpec.Meta(false)
	c.Assert(err, check.IsNil)
	c.Check(meta.Interval, check.Equals, oneMinute)
	nextTime := start.Add(oneMinute)
	c.Check(meta.NextContinuous, SameTime, nextTime)

	// If we only wait 30 seconds we shouldn't get a job
	s.Clock.Add(time.Duration(30) * time.Second)
	makeAttempt(0)

	// If we wait 30 more we should
	s.Clock.Add(time.Duration(30) * time.Second)
	makeAttempt(1)

	// If we wait 120 more we should only get one
	s.Clock.Add(time.Duration(120) * time.Second)
	makeAttempt(1)
	makeAttempt(0)
}

// TestMaxRunning tests that setting the max_running limit on a work spec
// does result in work coming back.
func (s *Suite) TestMaxRunning(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName: "worker",
		WorkSpecData: map[string]interface{}{
			"name":        "spec",
			"max_running": 1,
		},
	}
	sts.Do(s, c)

	for i := 0; i < 10; i++ {
		_, err := sts.AddWorkUnit(fmt.Sprintf("u%v", i))
		c.Assert(err, check.IsNil)
	}

	// First call, nothing is pending, so we should get one back
	s.Clock.Add(time.Duration(5) * time.Second)
	attempt := sts.RequestOneAttempt(c)

	// While that is still running, do another request; since we
	// have hit max_running we should get nothing back
	s.Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(c)

	// Finish the first batch of attempts
	err := attempt.Finish(nil)
	c.Assert(err, check.IsNil)

	// Now nothing is pending and we can ask for more; even if we
	// ask for 20 we only get one
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		NumberOfWorkUnits: 20,
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
}

// TestRequestSpecificSpec verifies that requesting work units for a
// specific work spec gets the right thing back.
func (s *Suite) TestRequestSpecificSpec(c *check.C) {
	one, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"priority": 20,
	})
	c.Assert(err, check.IsNil)
	_, err = one.AddWorkUnit("u1", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	two, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 10,
	})
	c.Assert(err, check.IsNil)
	_, err = two.AddWorkUnit("u2", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	three, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "three",
		"priority": 0,
	})
	c.Assert(err, check.IsNil)
	_, err = three.AddWorkUnit("u3", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// Plain RequestAttempts should return "one" with the highest
	// priority
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u1")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// If I request only "three" I should get only "three"
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u3")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// Both "two" and "three" should give "two" with higher priority
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three", "two"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u2")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// "four" should just return nothing
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"four"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)
	for _, attempt := range attempts {
		err = attempt.Retry(nil, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// Empty list should query everything and get "one"
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u1")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil, time.Duration(0))
		c.Assert(err, check.IsNil)
	}
}

// TestByRuntime creates two work specs with different runtimes, and
// validates that requests that want a specific runtime get it.
func (s *Suite) TestByRuntime(c *check.C) {
	// The specific thing we'll simulate here is one Python
	// worker, using the jobserver interface, with an empty
	// runtime string, plus one Go worker, using the native API,
	// with a "go" runtime.
	var (
		err          error
		worker       coordinate.Worker
		pSpec, gSpec coordinate.WorkSpec
		pUnit, gUnit coordinate.WorkUnit
		attempts     []coordinate.Attempt
	)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	pSpec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "p",
	})
	c.Assert(err, check.IsNil)
	pUnit, err = pSpec.AddWorkUnit("p", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	gSpec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":    "g",
		"runtime": "go",
	})
	c.Assert(err, check.IsNil)
	gUnit, err = gSpec.AddWorkUnit("g", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	// If we use default settings for RequestAttempts, we should
	// get back both work units
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	for _, attempt := range attempts {
		err = attempt.Finish(map[string]interface{}{})
		c.Assert(err, check.IsNil)
	}
	if len(attempts) == 1 {
		wasP := attempts[0].WorkUnit().Name() == "p"

		// Get more attempts
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		c.Check(attempts, check.HasLen, 1)
		for _, attempt := range attempts {
			err = attempt.Finish(map[string]interface{}{})
			c.Assert(err, check.IsNil)
		}

		if len(attempts) == 1 {
			// Should have gotten the other work spec
			if wasP {
				c.Check(attempts[0].WorkUnit().Name(), check.Equals, "g")
			} else {
				c.Check(attempts[0].WorkUnit().Name(), check.Equals, "p")
			}
		}

		// Now there shouldn't be anything more
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		c.Check(attempts, check.HasLen, 0)
		for _, attempt := range attempts {
			err = attempt.Finish(map[string]interface{}{})
			c.Assert(err, check.IsNil)
		}
	}

	// Reset the world
	err = pUnit.ClearActiveAttempt()
	c.Assert(err, check.IsNil)
	err = gUnit.ClearActiveAttempt()
	c.Assert(err, check.IsNil)

	// What we expect to get from jobserver
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{""},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) == 1 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "p")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(map[string]interface{}{}, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// A more sophisticated Python check
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"python", "python_2", "python_2.7", ""},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) == 1 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "p")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(map[string]interface{}{}, time.Duration(0))
		c.Assert(err, check.IsNil)
	}

	// What we expect to get from Go land
	s.Clock.Add(time.Duration(5) * time.Second)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"go"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) == 1 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "g")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(map[string]interface{}{}, time.Duration(0))
		c.Assert(err, check.IsNil)
	}
}

// TestNotBeforeDelayedStatus verifies that, if a work unit is created
// with a "not before" time, its status is returned as DelayedUnit.
func (s *Suite) TestNotBeforeDelayedStatus(c *check.C) {
	now := s.Clock.Now()
	then := now.Add(time.Duration(5) * time.Second)
	sts := SimpleTestSetup{
		WorkSpecName: "spec",
		WorkUnitName: "unit",
		WorkUnitMeta: coordinate.WorkUnitMeta{
			NotBefore: then,
		},
	}
	sts.Do(s, c)

	status, err := sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.DelayedUnit)

	// If we advance the clock by 10 seconds, the unit should become
	// available
	s.Clock.Add(time.Duration(10) * time.Second)

	status, err = sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.AvailableUnit)
}

// TestNotBeforeAttempt verifies that, if a work unit is created with
// a "not before" time, it is not returned as an attempt.
func (s *Suite) TestNotBeforeAttempt(c *check.C) {
	now := s.Clock.Now()
	then := now.Add(time.Duration(60) * time.Second)
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "unit",
		WorkUnitMeta: coordinate.WorkUnitMeta{
			NotBefore: then,
		},
	}
	sts.Do(s, c)

	// We expect no attempts now, because the "not before" time
	// hasn't arrived yet
	sts.CheckWorkUnitOrder(s, c)

	// If we advance the clock so enough time has passed, we
	// should now see the attempt
	s.Clock.Add(time.Duration(120) * time.Second)
	sts.CheckWorkUnitOrder(s, c, "unit")
}

// TestNotBeforePriority tests the intersection of NotBefore and Priority:
// the lower-priority unit that can execute now should.
func (s *Suite) TestNotBeforePriority(c *check.C) {
	now := s.Clock.Now()
	then := now.Add(time.Duration(60) * time.Second)

	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
	}
	sts.Do(s, c)

	// "first" has default priority and can execute now
	_, err := sts.WorkSpec.AddWorkUnit("first", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	// "second" has higher priority, but can't execute yet
	_, err = sts.WorkSpec.AddWorkUnit("second", map[string]interface{}{}, coordinate.WorkUnitMeta{
		Priority:  10.0,
		NotBefore: then,
	})
	c.Assert(err, check.IsNil)

	// If we do work units now, we should get only "first"
	sts.CheckWorkUnitOrder(s, c, "first")

	// Now advance the clock by a minute; we should get "second"
	s.Clock.Add(time.Duration(60) * time.Second)
	sts.CheckWorkUnitOrder(s, c, "second")
}

// TestDelayedOutput tests that the output of chained work specs can be
// delayed.
func (s *Suite) TestDelayedOutput(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName: "worker",
	}
	sts.Do(s, c)

	one, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)

	two, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	c.Assert(err, check.IsNil)

	_, err = one.AddWorkUnit("unit", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "one")

	err = attempts[0].Finish(map[string]interface{}{
		"output": []interface{}{
			[]interface{}{
				"u2",
				map[string]interface{}{},
				map[string]interface{}{"delay": 90},
			},
		},
	})
	c.Assert(err, check.IsNil)

	// If we get more attempts right now, we should get nothing
	s.Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(c)

	// If we advance far enough, we should get back the unit for "two"
	s.Clock.Add(time.Duration(120) * time.Second)
	sts.WorkSpec = two
	sts.CheckWorkUnitOrder(s, c, "u2")
}
