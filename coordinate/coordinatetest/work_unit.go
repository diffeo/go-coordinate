// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// TestTrivialWorkUnitFlow tests work unit creation, deletion, and existence.
func (s *Suite) TestTrivialWorkUnitFlow() {
	var (
		count int
		err   error
		units map[string]coordinate.WorkUnit
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestTrivialWorkUnitFlow",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 1)
		if s.Contains(units, "unit") {
			s.Equal("unit", units["unit"].Name())
			s.Equal("spec", units["unit"].WorkSpec().Name())
		}
	}

	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Equal(1, count)
	}

	_, err = sts.WorkSpec.WorkUnit("unit")
	s.Equal(coordinate.ErrNoSuchWorkUnit{Name: "unit"}, err)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Empty(units)
	}
}

// TestWorkUnitQueries calls WorkSpec.WorkUnits() with various queries.
func (s *Suite) TestWorkUnitQueries() {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitQueries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	_, err := sts.MakeWorkUnits()
	s.NoError(err)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 7)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "failed")
		s.Contains(units, "finished")
		s.Contains(units, "pending")
		s.Contains(units, "retryable")
	}

	// Get everything, in two batches, sorted
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Limit: 4,
	})
	if s.NoError(err) {
		s.Len(units, 4)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "failed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "failed",
		Limit:        4,
	})
	if s.NoError(err) {
		s.Len(units, 3)
		s.Contains(units, "finished")
		s.Contains(units, "pending")
		s.Contains(units, "retryable")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "retryable",
		Limit:        4,
	})
	if s.NoError(err) {
		s.Len(units, 0)
	}

	// Get work units by status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	if s.NoError(err) {
		s.Len(units, 3)
		s.Contains(units, "available")
		s.Contains(units, "expired")
		s.Contains(units, "retryable")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.PendingUnit},
	})
	if s.NoError(err) {
		s.Len(units, 1)
		s.Contains(units, "pending")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FinishedUnit},
	})
	if s.NoError(err) {
		s.Len(units, 1)
		s.Contains(units, "finished")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FailedUnit},
	})
	if s.NoError(err) {
		s.Len(units, 1)
		s.Contains(units, "failed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.DelayedUnit,
		},
	})
	if s.NoError(err) {
		s.Len(units, 1)
		s.Contains(units, "delayed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	if s.NoError(err) {
		s.Len(units, 2)
		s.Contains(units, "failed")
		s.Contains(units, "finished")
	}

	// Get work units by name
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"available", "failed", "missing"},
	})
	if s.NoError(err) {
		s.Len(units, 2)
		s.Contains(units, "available")
		s.Contains(units, "failed")
	}

	// Get work units by name and status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names:    []string{"available", "retryable", "finished"},
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	if s.NoError(err) {
		s.Len(units, 2)
		s.Contains(units, "available")
		s.Contains(units, "retryable")
	}
}

// TestDeleteWorkUnits is a smaller set of tests for
// WorkSpec.DeleteWorkUnits(), on the assumption that a fair amount of
// code will typically be shared with GetWorkUnits() and because it is
// intrinsically a mutating operation.
func (s *Suite) TestDeleteWorkUnits() {
	sts := SimpleTestSetup{
		NamespaceName: "TestDeleteWorkUnits",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	_, err := sts.MakeWorkUnits()
	s.NoError(err)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 7)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "failed")
		s.Contains(units, "finished")
		s.Contains(units, "pending")
		s.Contains(units, "retryable")
	}

	// Delete by name
	count, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	if s.NoError(err) {
		s.Equal(1, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 6)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "failed")
		s.Contains(units, "finished")
		s.Contains(units, "pending")
	}

	// Delete the same thing again; missing name should be a no-op
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	if s.NoError(err) {
		s.Equal(0, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 6)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "failed")
		s.Contains(units, "finished")
		s.Contains(units, "pending")
	}

	// Delete by status
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	if s.NoError(err) {
		s.Equal(2, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 4)
		s.Contains(units, "available")
		s.Contains(units, "delayed")
		s.Contains(units, "expired")
		s.Contains(units, "pending")
	}

	// Delete everything
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Equal(4, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 0)
	}
}

// TestCountWorkUnitStatus does simple validation on the
// CountWorkUnitStatus call.
func (s *Suite) TestCountWorkUnitStatus() {
	sts := SimpleTestSetup{
		NamespaceName: "TestCountWorkUnitStatus",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	_, err := sts.MakeWorkUnits()
	s.NoError(err)

	counts, err := sts.WorkSpec.CountWorkUnitStatus()
	if s.NoError(err) {
		s.Equal(map[coordinate.WorkUnitStatus]int{
			coordinate.AvailableUnit: 3,
			coordinate.PendingUnit:   1,
			coordinate.FinishedUnit:  1,
			coordinate.FailedUnit:    1,
			coordinate.DelayedUnit:   1,
		}, counts)
	}
}

// TestWorkUnitOrder is a very basic test that work units get returned
// in alphabetic order absent any other constraints.
func (s *Suite) TestWorkUnitOrder() {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitOrder",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	for _, name := range []string{"c", "b", "a"} {
		_, err := sts.AddWorkUnit(name)
		s.NoError(err)
	}

	sts.CheckWorkUnitOrder(s, "a", "b", "c")
}

// TestWorkUnitPriorityCtor tests that priorities passed in the work unit
// constructor are honored.
func (s *Suite) TestWorkUnitPriorityCtor() {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitPriorityCtor",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

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
		if s.NoError(err) {
			s.UnitHasPriority(workUnit, unit.float64)
		}
	}

	sts.CheckWorkUnitOrder(s, "b", "a", "c")
}

// TestWorkUnitPrioritySet tests two different ways of setting work unit
// priority.
func (s *Suite) TestWorkUnitPrioritySet() {
	var (
		err  error
		unit coordinate.WorkUnit
	)
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitPrioritySet",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	unit, err = sts.WorkSpec.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if s.NoError(err) {
		s.UnitHasPriority(unit, 0.0)
	}

	unit, err = sts.WorkSpec.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if s.NoError(err) {
		err = unit.SetPriority(10.0)
		if s.NoError(err) {
			s.UnitHasPriority(unit, 10.0)
		}
	}

	unit, err = sts.WorkSpec.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)
	err = sts.WorkSpec.SetWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"c"},
	}, 20.0)
	if s.NoError(err) {
		s.UnitHasPriority(unit, 20.0)
	}

	unit, err = sts.WorkSpec.AddWorkUnit("d", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 20.0)
	if s.NoError(err) {
		s.UnitHasPriority(unit, 20.0)
	}
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 10.0)
	if s.NoError(err) {
		s.UnitHasPriority(unit, 30.0)
	}

	unit, err = sts.WorkSpec.WorkUnit("b")
	if s.NoError(err) {
		s.UnitHasPriority(unit, 10.0)
	}

	sts.CheckWorkUnitOrder(s, "d", "c", "b", "a")
}

// TestWorkUnitData validates that the system can store and update
// data.
func (s *Suite) TestWorkUnitData() {
	var (
		unit coordinate.WorkUnit
		err  error
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitData",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	_, err = sts.WorkSpec.AddWorkUnit("a", map[string]interface{}{
		"name":  "a",
		"value": 1,
	}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	_, err = sts.WorkSpec.AddWorkUnit("b", map[string]interface{}{
		"name":  "b",
		"value": 2,
	}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	unit, err = sts.WorkSpec.WorkUnit("a")
	if s.NoError(err) {
		s.DataMatches(unit, map[string]interface{}{
			"name":  "a",
			"value": 1,
		})
	}

	unit, err = sts.WorkSpec.WorkUnit("b")
	if s.NoError(err) {
		s.DataMatches(unit, map[string]interface{}{
			"name":  "b",
			"value": 2,
		})
	}
}

// TestAddWorkUnitBleedover validates a bug in the postgres backend
// where adding a duplicate work unit in one work spec would modify
// similarly-named work units' data in all work specs.
func (s *Suite) TestAddWorkUnitBleedover() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAddWorkUnitBleedover",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	specA, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "a",
	})
	if !s.NoError(err) {
		return
	}
	specB, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "b",
	})
	if !s.NoError(err) {
		return
	}
	unitA, err := specA.AddWorkUnit("unit", map[string]interface{}{
		"unit": "a",
	}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}
	unitB, err := specB.AddWorkUnit("unit", map[string]interface{}{
		"unit": "b",
	}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	// Pre-check:
	s.DataMatches(unitA, map[string]interface{}{"unit": "a"})
	s.DataMatches(unitB, map[string]interface{}{"unit": "b"})

	// Now re-add unit B
	unitB2, err := specB.AddWorkUnit("unit", map[string]interface{}{
		"unit": "c",
	}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	// The bug was that unitA's data would have changed
	s.DataMatches(unitA, map[string]interface{}{"unit": "a"})
	s.DataMatches(unitB, map[string]interface{}{"unit": "c"})
	s.DataMatches(unitB2, map[string]interface{}{"unit": "c"})
}

// TestRecreateWorkUnits checks that creating work units that already
// exist works successfully.
func (s *Suite) TestRecreateWorkUnits() {
	sts := SimpleTestSetup{
		NamespaceName: "TestRecreateWorkUnits",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	units, err := sts.MakeWorkUnits()
	if !s.NoError(err) {
		return
	}

	for name := range units {
		unit, err := sts.AddWorkUnit(name)
		if !s.NoError(err, "name = %v", name) {
			continue
		}
		// Unless the unit was previously pending, it should be
		// available now
		status, err := unit.Status()
		if s.NoError(err, "name = %v", name) {
			expected := coordinate.AvailableUnit
			if name == "pending" {
				expected = coordinate.PendingUnit
			}
			s.Equal(expected, status, "name = %v", name)
		}
	}
}

// TestContinuous creates a continuous work spec but no work units for it.
// Requesting attempts should create a new work unit for it.
func (s *Suite) TestContinuous() {
	sts := SimpleTestSetup{
		WorkerName: "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	makeAttempt := func(expected int) {
		s.Clock.Add(5 * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if s.NoError(err) {
			s.Len(attempts, expected)
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				s.NoError(err)
			}
		}
	}

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// If we use SetMeta to turn continuous mode off and on, it
	// should affect whether work units come back
	meta, err := sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		meta.Continuous = false
		err = sts.WorkSpec.SetMeta(meta)
		if s.NoError(err) {
			makeAttempt(0)
		}

		meta.Continuous = true
		err = sts.WorkSpec.SetMeta(meta)
		if s.NoError(err) {
			makeAttempt(1)
		}
	}
}

// TestContinuousInterval verifies the operation of a continuous work spec
// that has a minimum respawn frequency.
func (s *Suite) TestContinuousInterval() {
	sts := SimpleTestSetup{
		NamespaceName: "TestContinuousInterval",
		WorkerName:    "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
			"interval":   60,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	makeAttempt := func(expected int) {
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if s.NoError(err) {
			s.Len(attempts, expected)
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				s.NoError(err)
			}
		}
	}

	start := s.Clock.Now()

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// The next-attempt time should be the start time plus the
	// interval
	meta, err := sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		s.Equal(1*time.Minute, meta.Interval)
		nextTime := start.Add(1 * time.Minute)
		s.WithinDuration(nextTime, meta.NextContinuous, 1*time.Millisecond)
	}

	// If we only wait 30 seconds we shouldn't get a job
	s.Clock.Add(30 * time.Second)
	makeAttempt(0)

	// If we wait 30 more we should
	s.Clock.Add(30 * time.Second)
	makeAttempt(1)

	// If we wait 120 more we should only get one
	s.Clock.Add(120 * time.Second)
	makeAttempt(1)
	makeAttempt(0)
}

// TestMaxRunning tests that setting the max_running limit on a work spec
// does result in work coming back.
func (s *Suite) TestMaxRunning() {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRunning",
		WorkerName:    "worker",
		WorkSpecData: map[string]interface{}{
			"name":        "spec",
			"max_running": 1,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	for i := 0; i < 10; i++ {
		_, err := sts.AddWorkUnit(fmt.Sprintf("u%v", i))
		s.NoError(err)
	}

	// First call, nothing is pending, so we should get one back
	s.Clock.Add(time.Duration(5) * time.Second)
	attempt := sts.RequestOneAttempt(s)

	// While that is still running, do another request; since we
	// have hit max_running we should get nothing back
	s.Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(s)

	// Finish the first batch of attempts
	err := attempt.Finish(nil)
	s.NoError(err)

	// Now nothing is pending and we can ask for more; even if we
	// ask for 20 we only get one
	s.Clock.Add(5 * time.Second)
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		NumberOfWorkUnits: 20,
	})
	if s.NoError(err) {
		s.Len(attempts, 1)
	}
}

// TestRequestSpecificSpec verifies that requesting work units for a
// specific work spec gets the right thing back.
func (s *Suite) TestRequestSpecificSpec() {
	sts := SimpleTestSetup{
		NamespaceName: "TestRequestSpecificSpec",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"priority": 20,
	})
	if !s.NoError(err) {
		return
	}
	_, err = one.AddWorkUnit("u1", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	two, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 10,
	})
	if !s.NoError(err) {
		return
	}
	_, err = two.AddWorkUnit("u2", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	three, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "three",
		"priority": 0,
	})
	if !s.NoError(err) {
		return
	}
	_, err = three.AddWorkUnit("u3", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	// Plain RequestAttempts should return "one" with the highest
	// priority
	s.Clock.Add(5 * time.Second)
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("u1", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		s.NoError(err)
	}

	// If I request only "three" I should get only "three"
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three"},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("u3", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		s.NoError(err)
	}

	// Both "two" and "three" should give "two" with higher priority
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three", "two"},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("u2", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		s.NoError(err)
	}

	// "four" should just return nothing
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"four"},
	})
	s.NoError(err)
	s.Empty(attempts)

	// Empty list should query everything and get "one"
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("u1", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		s.NoError(err)
	}
}

// TestByRuntime creates two work specs with different runtimes, and
// validates that requests that want a specific runtime get it.
func (s *Suite) TestByRuntime() {
	// The specific thing we'll simulate here is one Python
	// worker, using the jobserver interface, with an empty
	// runtime string, plus one Go worker, using the native API,
	// with a "go" runtime.
	var (
		err          error
		pSpec, gSpec coordinate.WorkSpec
		pUnit, gUnit coordinate.WorkUnit
		attempts     []coordinate.Attempt
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestByRuntime",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	pSpec, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "p",
	})
	if !s.NoError(err) {
		return
	}
	pUnit, err = pSpec.AddWorkUnit("p", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	gSpec, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":    "g",
		"runtime": "go",
	})
	if !s.NoError(err) {
		return
	}
	gUnit, err = gSpec.AddWorkUnit("g", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	// If we use default settings for RequestAttempts, we should
	// get back both work units
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if s.NoError(err) && s.Len(attempts, 1) {
		err = attempts[0].Finish(map[string]interface{}{})
		s.NoError(err)

		wasP := attempts[0].WorkUnit().Name() == "p"

		// Get more attempts
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if s.NoError(err) && s.Len(attempts, 1) {
			err = attempts[0].Finish(map[string]interface{}{})
			s.NoError(err)

			// Should have gotten the other work spec
			if wasP {
				s.Equal("g", attempts[0].WorkUnit().Name())
			} else {
				s.Equal("p", attempts[0].WorkUnit().Name())
			}
		}

		// Now there shouldn't be anything more
		s.Clock.Add(5 * time.Second)
		sts.RequestNoAttempts(s)
	}

	// Reset the world
	err = pUnit.ClearActiveAttempt()
	s.NoError(err)
	err = gUnit.ClearActiveAttempt()
	s.NoError(err)

	// What we expect to get from jobserver
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{""},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("p", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		s.NoError(err)
	}

	// A more sophisticated Python check
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"python", "python_2", "python_2.7", ""},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("p", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		s.NoError(err)
	}

	// What we expect to get from Go land
	s.Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"go"},
	})
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("g", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		s.NoError(err)
	}
}

// TestNotBeforeDelayedStatus verifies that, if a work unit is created
// with a "not before" time, its status is returned as DelayedUnit.
func (s *Suite) TestNotBeforeDelayedStatus() {
	now := s.Clock.Now()
	then := now.Add(time.Duration(5) * time.Second)
	sts := SimpleTestSetup{
		NamespaceName: "TestNotBeforeDelayedStatus",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
		WorkUnitMeta: coordinate.WorkUnitMeta{
			NotBefore: then,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	sts.CheckUnitStatus(s, coordinate.DelayedUnit)

	// If we advance the clock by 10 seconds, the unit should become
	// available
	s.Clock.Add(10 * time.Second)
	sts.CheckUnitStatus(s, coordinate.AvailableUnit)
}

// TestNotBeforeAttempt verifies that, if a work unit is created with
// a "not before" time, it is not returned as an attempt.
func (s *Suite) TestNotBeforeAttempt() {
	now := s.Clock.Now()
	then := now.Add(time.Duration(60) * time.Second)
	sts := SimpleTestSetup{
		NamespaceName: "TestNotBeforeAttempt",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
		WorkUnitMeta: coordinate.WorkUnitMeta{
			NotBefore: then,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// We expect no attempts now, because the "not before" time
	// hasn't arrived yet
	sts.CheckWorkUnitOrder(s)

	// If we advance the clock so enough time has passed, we
	// should now see the attempt
	s.Clock.Add(120 * time.Second)
	sts.CheckWorkUnitOrder(s, "unit")
}

// TestNotBeforePriority tests the intersection of NotBefore and Priority:
// the lower-priority unit that can execute now should.
func (s *Suite) TestNotBeforePriority() {
	now := s.Clock.Now()
	then := now.Add(60 * time.Second)

	sts := SimpleTestSetup{
		NamespaceName: "TestNotBeforePriority",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// "first" has default priority and can execute now
	_, err := sts.WorkSpec.AddWorkUnit("first", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	// "second" has higher priority, but can't execute yet
	_, err = sts.WorkSpec.AddWorkUnit("second", map[string]interface{}{}, coordinate.WorkUnitMeta{
		Priority:  10.0,
		NotBefore: then,
	})
	s.NoError(err)

	// If we do work units now, we should get only "first"
	sts.CheckWorkUnitOrder(s, "first")

	// Now advance the clock by a minute; we should get "second"
	s.Clock.Add(60 * time.Second)
	sts.CheckWorkUnitOrder(s, "second")
}

// TestDelayedOutput tests that the output of chained work specs can be
// delayed.
func (s *Suite) TestDelayedOutput() {
	sts := SimpleTestSetup{
		NamespaceName: "TestDelayedOutput",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !s.NoError(err) {
		return
	}

	two, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !s.NoError(err) {
		return
	}

	_, err = one.AddWorkUnit("unit", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	attempt := sts.RequestOneAttempt(s)
	s.Equal("one", attempt.WorkUnit().WorkSpec().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []interface{}{
			[]interface{}{
				"u2",
				map[string]interface{}{},
				map[string]interface{}{"delay": 90},
			},
		},
	})
	s.NoError(err)

	// If we get more attempts right now, we should get nothing
	s.Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(s)

	// If we advance far enough, we should get back the unit for "two"
	s.Clock.Add(time.Duration(120) * time.Second)
	sts.WorkSpec = two
	sts.CheckWorkUnitOrder(s, "u2")
}

// TestUnitDeletedGone validates that deleting a work unit causes
// operations on it to return ErrGone.
func (s *Suite) TestUnitDeletedGone() {
	sts := SimpleTestSetup{
		NamespaceName: "TestUnitDeletedGone",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// Delete all the work units
	_, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	s.NoError(err)

	_, err = sts.WorkUnit.Status()
	// If the backend does name-based lookup (restclient) we will get
	// ErrNoSuchWorkUnit; if it does object-based lookup (memory,
	// postgres) we will get ErrGone
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		s.Equal(sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("deleted work spec produced unexpected error",
			"%+v", err)
	}
}

// TestUnitSpecDeletedGone validates that deleting a work unit's work
// spec causes operations on the unit to return ErrGone.
func (s *Suite) TestUnitSpecDeletedGone() {
	sts := SimpleTestSetup{
		NamespaceName: "TestUnitSpecDeletedGone",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// Delete the work spec
	err := sts.Namespace.DestroyWorkSpec(sts.WorkSpecName)
	s.NoError(err)

	_, err = sts.WorkUnit.Status()
	// If the backend does name-based lookup (restclient) we will get
	// ErrNoSuchWorkUnit; if it does object-based lookup (memory,
	// postgres) we will get ErrGone
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		s.Equal(sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("deleted work spec produced unexpected error",
			"%+v", err)
	}
}
