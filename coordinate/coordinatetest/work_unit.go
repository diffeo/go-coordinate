// Copyright 2015 Diffeo, Inc.
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
		spec  coordinate.WorkSpec
		unit  coordinate.WorkUnit
		units map[string]coordinate.WorkUnit
	)

	spec, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	unit, err = spec.AddWorkUnit("unit", map[string]interface{}{}, 0)
	c.Assert(err, check.IsNil)
	c.Check(unit.Name(), check.Equals, "unit")
	c.Check(unit.WorkSpec().Name(), check.Equals, "spec")

	unit, err = spec.WorkUnit("unit")
	c.Assert(err, check.IsNil)
	c.Check(unit.Name(), check.Equals, "unit")
	c.Check(unit.WorkSpec().Name(), check.Equals, "spec")

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units["unit"], check.NotNil)
	c.Check(units["unit"].Name(), check.Equals, "unit")
	c.Check(units["unit"].WorkSpec().Name(), check.Equals, "spec")

	count, err = spec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)

	unit, err = spec.WorkUnit("unit")
	c.Assert(err, check.IsNil)
	c.Check(unit, check.IsNil)

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)
}

// makeWorkSpecAndWorker does some boring setup to create a default
// work spec named "spec" and a default worker named "worker".  If
// either fails, abort the test.
func (s *Suite) makeWorkSpecAndWorker(c *check.C) (coordinate.WorkSpec, coordinate.Worker) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	return spec, worker
}

// makeWorkUnits creates a handful of work units within a work spec.
// These have keys "available", "pending", "finished", "failed",
// "expired", and "retryable", and wind up in the corresponding
// states.
func makeWorkUnits(spec coordinate.WorkSpec, worker coordinate.Worker) (map[string]coordinate.WorkUnit, error) {
	result := map[string]coordinate.WorkUnit{
		"available": nil,
		"pending":   nil,
		"finished":  nil,
		"failed":    nil,
		"expired":   nil,
		"retryable": nil,
	}
	for key := range result {
		unit, err := spec.AddWorkUnit(key, map[string]interface{}{}, 0)
		if err != nil {
			return nil, err
		}
		result[key] = unit

		// Run the workflow
		if key == "available" {
			continue
		}
		attempt, err := worker.MakeAttempt(unit, time.Duration(0))
		if err != nil {
			return nil, err
		}
		switch key {
		case "pending":
			{
			} // leave it running
		case "finished":
			err = attempt.Finish(nil)
		case "failed":
			err = attempt.Fail(nil)
		case "expired":
			err = attempt.Expire(nil)
		case "retryable":
			err = attempt.Retry(nil)
		}
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// TestWorkUnitQueries calls WorkSpec.WorkUnits() with various queries.
func (s *Suite) TestWorkUnitQueries(c *check.C) {
	spec, worker := s.makeWorkSpecAndWorker(c)

	_, err := makeWorkUnits(spec, worker)
	c.Assert(err, check.IsNil)

	// Get everything
	units, err := spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 6)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished", "pending", "retryable"})

	// Get everything, in two batches, sorted
	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Limit: 4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 4)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "finished",
		Limit:        4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"pending", "retryable"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "retryable",
		Limit:        4,
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)

	// Get work units by status
	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 3)
	c.Check(units, HasKeys, []string{"available", "expired", "retryable"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.PendingUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"pending"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FinishedUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"finished"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FailedUnit},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 1)
	c.Check(units, HasKeys, []string{"failed"})

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"failed", "finished"})

	// Get work units by name
	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"available", "failed", "missing"},
	})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 2)
	c.Check(units, HasKeys, []string{"available", "failed"})

	// Get work units by name and status
	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{
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
	spec, worker := s.makeWorkSpecAndWorker(c)

	_, err := makeWorkUnits(spec, worker)
	c.Assert(err, check.IsNil)

	// Get everything
	units, err := spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 6)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished", "pending", "retryable"})

	// Delete by name
	count, err := spec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 5)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished", "pending"})

	// Delete the same thing again; missing name should be a no-op
	count, err = spec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 0)

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 5)
	c.Check(units, HasKeys, []string{"available", "expired", "failed",
		"finished", "pending"})

	// Delete by status
	count, err = spec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 2)

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 3)
	c.Check(units, HasKeys, []string{"available", "expired", "pending"})

	// Delete everything
	count, err = spec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 3)

	units, err = spec.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)
}

// TestCountWorkUnitStatus does simple validation on the
// CountWorkUnitStatus call.
func (s *Suite) TestCountWorkUnitStatus(c *check.C) {
	spec, worker := s.makeWorkSpecAndWorker(c)
	_, err := makeWorkUnits(spec, worker)
	c.Assert(err, check.IsNil)

	counts, err := spec.CountWorkUnitStatus()
	c.Assert(err, check.IsNil)
	c.Check(counts, check.DeepEquals, map[coordinate.WorkUnitStatus]int{
		coordinate.AvailableUnit: 3,
		coordinate.PendingUnit:   1,
		coordinate.FinishedUnit:  1,
		coordinate.FailedUnit:    1,
	})
}

// checkWorkUnitOrder verifies that getting all of the work possible
// retrieves work units in a specific order.
func checkWorkUnitOrder(
	c *check.C,
	worker coordinate.Worker,
	spec coordinate.WorkSpec,
	unitNames ...string,
) {
	var processedUnits []string
	for {
		attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		if len(attempts) == 0 {
			break
		}
		c.Assert(attempts, check.HasLen, 1)
		attempt := attempts[0]
		c.Check(attempt.WorkUnit().WorkSpec().Name(), check.Equals, spec.Name())
		processedUnits = append(processedUnits, attempt.WorkUnit().Name())
		err = attempt.Finish(nil)
		c.Assert(err, check.IsNil)
	}

	c.Check(processedUnits, check.DeepEquals, unitNames)
}

// TestWorkUnitOrder is a very basic test that work units get returned
// in alphabetic order absent any other constraints.
func (s *Suite) TestWorkUnitOrder(c *check.C) {
	spec, worker := s.makeWorkSpecAndWorker(c)

	for _, name := range []string{"c", "b", "a"} {
		_, err := spec.AddWorkUnit(name, map[string]interface{}{}, 0)
		c.Assert(err, check.IsNil)
	}

	checkWorkUnitOrder(c, worker, spec, "a", "b", "c")
}

// TestWorkUnitPriorityCtor tests that priorities passed in the work unit
// constructor are honored.
func (s *Suite) TestWorkUnitPriorityCtor(c *check.C) {
	spec, worker := s.makeWorkSpecAndWorker(c)

	var units = []struct {
		string
		float64
	}{
		{"a", 0},
		{"b", 10},
		{"c", 0},
	}

	for _, unit := range units {
		workUnit, err := spec.AddWorkUnit(unit.string, map[string]interface{}{}, unit.float64)
		c.Assert(err, check.IsNil)
		pri, err := workUnit.Priority()
		c.Assert(err, check.IsNil)
		c.Check(pri, check.Equals, unit.float64)
	}

	checkWorkUnitOrder(c, worker, spec, "b", "a", "c")
}

// TestWorkUnitPrioritySet tests two different ways of setting work unit
// priority.
func (s *Suite) TestWorkUnitPrioritySet(c *check.C) {
	var (
		err      error
		priority float64
		unit     coordinate.WorkUnit
	)
	spec, worker := s.makeWorkSpecAndWorker(c)

	unit, err = spec.AddWorkUnit("a", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 0.0)

	unit, err = spec.AddWorkUnit("b", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	err = unit.SetPriority(10.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 10.0)

	unit, err = spec.AddWorkUnit("c", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	err = spec.SetWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"c"},
	}, 20.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 20.0)

	unit, err = spec.AddWorkUnit("d", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	err = spec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 20.0)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 20.0)
	c.Assert(err, check.IsNil)
	err = spec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 10.0)
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 30.0)

	unit, err = spec.WorkUnit("b")
	c.Assert(err, check.IsNil)
	priority, err = unit.Priority()
	c.Assert(err, check.IsNil)
	c.Check(priority, check.Equals, 10.0)

	checkWorkUnitOrder(c, worker, spec, "d", "c", "b", "a")
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
	}, 0.0)
	c.Assert(err, check.IsNil)

	_, err = spec.AddWorkUnit("b", map[string]interface{}{
		"name":  "b",
		"value": 2,
	}, 0.0)
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
	spec, worker := s.makeWorkSpecAndWorker(c)
	units, err := makeWorkUnits(spec, worker)
	c.Assert(err, check.IsNil)

	for name := range units {
		unit, err := spec.AddWorkUnit(name, map[string]interface{}{}, 0.0)
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
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":       "spec",
		"continuous": true,
	})
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	makeAttempt := func(expected int) {
		attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
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
	meta, err := spec.Meta(false)
	c.Assert(err, check.IsNil)
	meta.Continuous = false
	err = spec.SetMeta(meta)
	c.Assert(err, check.IsNil)
	makeAttempt(0)

	meta.Continuous = true
	err = spec.SetMeta(meta)
	c.Assert(err, check.IsNil)
	makeAttempt(1)
}

// TestContinuousInterval verifies the operation of a continuous work spec
// that has a minimum respawn frequency.
func (s *Suite) TestContinuousInterval(c *check.C) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":       "spec",
		"continuous": true,
		"interval":   60,
	})
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	makeAttempt := func(expected int) {
		attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
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
	meta, err := spec.Meta(false)
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
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":        "spec",
		"max_running": 1,
	})
	c.Assert(err, check.IsNil)

	for i := 0; i < 10; i++ {
		_, err = spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, 0.0)
		c.Assert(err, check.IsNil)
	}

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// First call, nothing is pending, so we should get one back
	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)

	// While that is still running, do another request; since we
	// have hit max_running we should get nothing back
	a2, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(a2, check.HasLen, 0)

	// Finish the first batch of attempts
	for _, attempt := range attempts {
		err = attempt.Finish(nil)
		c.Assert(err, check.IsNil)
	}

	// Now nothing is pending and we can ask for more; even if we
	// ask for 20 we only get one
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
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
	_, err = one.AddWorkUnit("u1", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)

	two, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 10,
	})
	c.Assert(err, check.IsNil)
	_, err = two.AddWorkUnit("u2", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)

	three, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "three",
		"priority": 0,
	})
	c.Assert(err, check.IsNil)
	_, err = three.AddWorkUnit("u3", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// Plain RequestAttempts should return "one" with the highest
	// priority
	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u1")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil)
		c.Assert(err, check.IsNil)
	}

	// If I request only "three" I should get only "three"
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u3")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil)
		c.Assert(err, check.IsNil)
	}

	// Both "two" and "three" should give "two" with higher priority
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three", "two"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u2")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil)
		c.Assert(err, check.IsNil)
	}

	// "four" should just return nothing
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"four"},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)
	for _, attempt := range attempts {
		err = attempt.Retry(nil)
		c.Assert(err, check.IsNil)
	}

	// Empty list should query everything and get "one"
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{},
	})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "u1")
	}
	for _, attempt := range attempts {
		err = attempt.Retry(nil)
		c.Assert(err, check.IsNil)
	}
}
