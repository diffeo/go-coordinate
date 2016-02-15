// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestTrivialWorkUnitFlow tests work unit creation, deletion, and existence.
func TestTrivialWorkUnitFlow(t *testing.T) {
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
	sts.SetUp(t)
	defer sts.TearDown(t)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		if assert.Contains(t, units, "unit") {
			assert.Equal(t, "unit", units["unit"].Name())
			assert.Equal(t, "spec", units["unit"].WorkSpec().Name())
		}
	}

	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Equal(t, 1, count)
	}

	_, err = sts.WorkSpec.WorkUnit("unit")
	assert.Equal(t, coordinate.ErrNoSuchWorkUnit{Name: "unit"}, err)

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Empty(t, units)
	}
}

// TestWorkUnitQueries calls WorkSpec.WorkUnits() with various queries.
func TestWorkUnitQueries(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitQueries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	_, err := sts.MakeWorkUnits()
	assert.NoError(t, err)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 7)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "failed")
		assert.Contains(t, units, "finished")
		assert.Contains(t, units, "pending")
		assert.Contains(t, units, "retryable")
	}

	// Get everything, in two batches, sorted
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Limit: 4,
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 4)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "failed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "failed",
		Limit:        4,
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 3)
		assert.Contains(t, units, "finished")
		assert.Contains(t, units, "pending")
		assert.Contains(t, units, "retryable")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		PreviousName: "retryable",
		Limit:        4,
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 0)
	}

	// Get work units by status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 3)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "retryable")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.PendingUnit},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		assert.Contains(t, units, "pending")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FinishedUnit},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		assert.Contains(t, units, "finished")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{coordinate.FailedUnit},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		assert.Contains(t, units, "failed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.DelayedUnit,
		},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		assert.Contains(t, units, "delayed")
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 2)
		assert.Contains(t, units, "failed")
		assert.Contains(t, units, "finished")
	}

	// Get work units by name
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"available", "failed", "missing"},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 2)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "failed")
	}

	// Get work units by name and status
	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{
		Names:    []string{"available", "retryable", "finished"},
		Statuses: []coordinate.WorkUnitStatus{coordinate.AvailableUnit},
	})
	if assert.NoError(t, err) {
		assert.Len(t, units, 2)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "retryable")
	}
}

// TestDeleteWorkUnits is a smaller set of tests for
// WorkSpec.DeleteWorkUnits(), on the assumption that a fair amount of
// code will typically be shared with GetWorkUnits() and because it is
// intrinsically a mutating operation.
func TestDeleteWorkUnits(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestDeleteWorkUnits",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	_, err := sts.MakeWorkUnits()
	assert.NoError(t, err)

	// Get everything
	units, err := sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 7)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "failed")
		assert.Contains(t, units, "finished")
		assert.Contains(t, units, "pending")
		assert.Contains(t, units, "retryable")
	}

	// Delete by name
	count, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	if assert.NoError(t, err) {
		assert.Equal(t, 1, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 6)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "failed")
		assert.Contains(t, units, "finished")
		assert.Contains(t, units, "pending")
	}

	// Delete the same thing again; missing name should be a no-op
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Names: []string{"retryable"},
	})
	if assert.NoError(t, err) {
		assert.Equal(t, 0, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 6)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "failed")
		assert.Contains(t, units, "finished")
		assert.Contains(t, units, "pending")
	}

	// Delete by status
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{
		Statuses: []coordinate.WorkUnitStatus{
			coordinate.FailedUnit,
			coordinate.FinishedUnit,
		},
	})
	if assert.NoError(t, err) {
		assert.Equal(t, 2, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 4)
		assert.Contains(t, units, "available")
		assert.Contains(t, units, "delayed")
		assert.Contains(t, units, "expired")
		assert.Contains(t, units, "pending")
	}

	// Delete everything
	count, err = sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Equal(t, 4, count)
	}

	units, err = sts.WorkSpec.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 0)
	}
}

// TestCountWorkUnitStatus does simple validation on the
// CountWorkUnitStatus call.
func TestCountWorkUnitStatus(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestCountWorkUnitStatus",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	_, err := sts.MakeWorkUnits()
	assert.NoError(t, err)

	counts, err := sts.WorkSpec.CountWorkUnitStatus()
	if assert.NoError(t, err) {
		assert.Equal(t, map[coordinate.WorkUnitStatus]int{
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
func TestWorkUnitOrder(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitOrder",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	for _, name := range []string{"c", "b", "a"} {
		_, err := sts.AddWorkUnit(name)
		assert.NoError(t, err)
	}

	sts.CheckWorkUnitOrder(t, "a", "b", "c")
}

// TestWorkUnitPriorityCtor tests that priorities passed in the work unit
// constructor are honored.
func TestWorkUnitPriorityCtor(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitPriorityCtor",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

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
		if assert.NoError(t, err) {
			UnitHasPriority(t, workUnit, unit.float64)
		}
	}

	sts.CheckWorkUnitOrder(t, "b", "a", "c")
}

// TestWorkUnitPrioritySet tests two different ways of setting work unit
// priority.
func TestWorkUnitPrioritySet(t *testing.T) {
	var (
		err  error
		unit coordinate.WorkUnit
	)
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitPrioritySet",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	unit, err = sts.WorkSpec.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if assert.NoError(t, err) {
		UnitHasPriority(t, unit, 0.0)
	}

	unit, err = sts.WorkSpec.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if assert.NoError(t, err) {
		err = unit.SetPriority(10.0)
		if assert.NoError(t, err) {
			UnitHasPriority(t, unit, 10.0)
		}
	}

	unit, err = sts.WorkSpec.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)
	err = sts.WorkSpec.SetWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"c"},
	}, 20.0)
	if assert.NoError(t, err) {
		UnitHasPriority(t, unit, 20.0)
	}

	unit, err = sts.WorkSpec.AddWorkUnit("d", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 20.0)
	if assert.NoError(t, err) {
		UnitHasPriority(t, unit, 20.0)
	}
	err = sts.WorkSpec.AdjustWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{"d"},
	}, 10.0)
	if assert.NoError(t, err) {
		UnitHasPriority(t, unit, 30.0)
	}

	unit, err = sts.WorkSpec.WorkUnit("b")
	if assert.NoError(t, err) {
		UnitHasPriority(t, unit, 10.0)
	}

	sts.CheckWorkUnitOrder(t, "d", "c", "b", "a")
}

// TestWorkUnitData validates that the system can store and update
// data.
func TestWorkUnitData(t *testing.T) {
	var (
		unit coordinate.WorkUnit
		err  error
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitData",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	_, err = sts.WorkSpec.AddWorkUnit("a", map[string]interface{}{
		"name":  "a",
		"value": 1,
	}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	_, err = sts.WorkSpec.AddWorkUnit("b", map[string]interface{}{
		"name":  "b",
		"value": 2,
	}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	unit, err = sts.WorkSpec.WorkUnit("a")
	if assert.NoError(t, err) {
		DataMatches(t, unit, map[string]interface{}{
			"name":  "a",
			"value": 1,
		})
	}

	unit, err = sts.WorkSpec.WorkUnit("b")
	if assert.NoError(t, err) {
		DataMatches(t, unit, map[string]interface{}{
			"name":  "b",
			"value": 2,
		})
	}
}

// TestRecreateWorkUnits checks that creating work units that already
// exist works successfully.
func TestRecreateWorkUnits(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestRecreateWorkUnits",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	units, err := sts.MakeWorkUnits()
	if !assert.NoError(t, err) {
		return
	}

	for name := range units {
		unit, err := sts.AddWorkUnit(name)
		if !assert.NoError(t, err, "name = %v", name) {
			continue
		}
		// Unless the unit was previously pending, it should be
		// available now
		status, err := unit.Status()
		if assert.NoError(t, err, "name = %v", name) {
			expected := coordinate.AvailableUnit
			if name == "pending" {
				expected = coordinate.PendingUnit
			}
			assert.Equal(t, expected, status, "name = %v", name)
		}
	}
}

// TestContinuous creates a continuous work spec but no work units for it.
// Requesting attempts should create a new work unit for it.
func TestContinuous(t *testing.T) {
	sts := SimpleTestSetup{
		WorkerName: "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	makeAttempt := func(expected int) {
		Clock.Add(5 * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if assert.NoError(t, err) {
			assert.Len(t, attempts, expected)
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				assert.NoError(t, err)
			}
		}
	}

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// If we use SetMeta to turn continuous mode off and on, it
	// should affect whether work units come back
	meta, err := sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		meta.Continuous = false
		err = sts.WorkSpec.SetMeta(meta)
		if assert.NoError(t, err) {
			makeAttempt(0)
		}

		meta.Continuous = true
		err = sts.WorkSpec.SetMeta(meta)
		if assert.NoError(t, err) {
			makeAttempt(1)
		}
	}
}

// TestContinuousInterval verifies the operation of a continuous work spec
// that has a minimum respawn frequency.
func TestContinuousInterval(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestContinuousInterval",
		WorkerName:    "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"continuous": true,
			"interval":   60,
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	makeAttempt := func(expected int) {
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if assert.NoError(t, err) {
			assert.Len(t, attempts, expected)
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				assert.NoError(t, err)
			}
		}
	}

	start := Clock.Now()

	// While we haven't added any work units yet, since the work
	// spec is continuous, we should have something
	makeAttempt(1)

	// The next-attempt time should be the start time plus the
	// interval
	meta, err := sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 1*time.Minute, meta.Interval)
		nextTime := start.Add(1 * time.Minute)
		assert.WithinDuration(t, nextTime, meta.NextContinuous, 1*time.Millisecond)
	}

	// If we only wait 30 seconds we shouldn't get a job
	Clock.Add(30 * time.Second)
	makeAttempt(0)

	// If we wait 30 more we should
	Clock.Add(30 * time.Second)
	makeAttempt(1)

	// If we wait 120 more we should only get one
	Clock.Add(120 * time.Second)
	makeAttempt(1)
	makeAttempt(0)
}

// TestMaxRunning tests that setting the max_running limit on a work spec
// does result in work coming back.
func TestMaxRunning(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRunning",
		WorkerName:    "worker",
		WorkSpecData: map[string]interface{}{
			"name":        "spec",
			"max_running": 1,
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	for i := 0; i < 10; i++ {
		_, err := sts.AddWorkUnit(fmt.Sprintf("u%v", i))
		assert.NoError(t, err)
	}

	// First call, nothing is pending, so we should get one back
	Clock.Add(time.Duration(5) * time.Second)
	attempt := sts.RequestOneAttempt(t)

	// While that is still running, do another request; since we
	// have hit max_running we should get nothing back
	Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(t)

	// Finish the first batch of attempts
	err := attempt.Finish(nil)
	assert.NoError(t, err)

	// Now nothing is pending and we can ask for more; even if we
	// ask for 20 we only get one
	Clock.Add(5 * time.Second)
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		NumberOfWorkUnits: 20,
	})
	if assert.NoError(t, err) {
		assert.Len(t, attempts, 1)
	}
}

// TestRequestSpecificSpec verifies that requesting work units for a
// specific work spec gets the right thing back.
func TestRequestSpecificSpec(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestRequestSpecificSpec",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"priority": 20,
	})
	if !assert.NoError(t, err) {
		return
	}
	_, err = one.AddWorkUnit("u1", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	two, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 10,
	})
	if !assert.NoError(t, err) {
		return
	}
	_, err = two.AddWorkUnit("u2", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	three, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "three",
		"priority": 0,
	})
	if !assert.NoError(t, err) {
		return
	}
	_, err = three.AddWorkUnit("u3", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	// Plain RequestAttempts should return "one" with the highest
	// priority
	Clock.Add(5 * time.Second)
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "u1", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		assert.NoError(t, err)
	}

	// If I request only "three" I should get only "three"
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three"},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "u3", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		assert.NoError(t, err)
	}

	// Both "two" and "three" should give "two" with higher priority
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"three", "two"},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "u2", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		assert.NoError(t, err)
	}

	// "four" should just return nothing
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{"four"},
	})
	assert.NoError(t, err)
	assert.Empty(t, attempts)

	// Empty list should query everything and get "one"
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		WorkSpecs: []string{},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "u1", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(nil, time.Duration(0))
		assert.NoError(t, err)
	}
}

// TestByRuntime creates two work specs with different runtimes, and
// validates that requests that want a specific runtime get it.
func TestByRuntime(t *testing.T) {
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
	sts.SetUp(t)
	defer sts.TearDown(t)

	pSpec, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "p",
	})
	if !assert.NoError(t, err) {
		return
	}
	pUnit, err = pSpec.AddWorkUnit("p", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !assert.NoError(t, err) {
		return
	}

	gSpec, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":    "g",
		"runtime": "go",
	})
	if !assert.NoError(t, err) {
		return
	}
	gUnit, err = gSpec.AddWorkUnit("g", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !assert.NoError(t, err) {
		return
	}

	// If we use default settings for RequestAttempts, we should
	// get back both work units
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		err = attempts[0].Finish(map[string]interface{}{})
		assert.NoError(t, err)

		wasP := attempts[0].WorkUnit().Name() == "p"

		// Get more attempts
		Clock.Add(time.Duration(5) * time.Second)
		attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
			err = attempts[0].Finish(map[string]interface{}{})
			assert.NoError(t, err)

			// Should have gotten the other work spec
			if wasP {
				assert.Equal(t, "g", attempts[0].WorkUnit().Name())
			} else {
				assert.Equal(t, "p", attempts[0].WorkUnit().Name())
			}
		}

		// Now there shouldn't be anything more
		Clock.Add(5 * time.Second)
		sts.RequestNoAttempts(t)
	}

	// Reset the world
	err = pUnit.ClearActiveAttempt()
	assert.NoError(t, err)
	err = gUnit.ClearActiveAttempt()
	assert.NoError(t, err)

	// What we expect to get from jobserver
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{""},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "p", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		assert.NoError(t, err)
	}

	// A more sophisticated Python check
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"python", "python_2", "python_2.7", ""},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "p", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		assert.NoError(t, err)
	}

	// What we expect to get from Go land
	Clock.Add(5 * time.Second)
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes: []string{"go"},
	})
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "g", attempts[0].WorkUnit().Name())
		err = attempts[0].Retry(map[string]interface{}{}, time.Duration(0))
		assert.NoError(t, err)
	}
}

// TestNotBeforeDelayedStatus verifies that, if a work unit is created
// with a "not before" time, its status is returned as DelayedUnit.
func TestNotBeforeDelayedStatus(t *testing.T) {
	now := Clock.Now()
	then := now.Add(time.Duration(5) * time.Second)
	sts := SimpleTestSetup{
		NamespaceName: "TestNotBeforeDelayedStatus",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
		WorkUnitMeta: coordinate.WorkUnitMeta{
			NotBefore: then,
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	sts.CheckUnitStatus(t, coordinate.DelayedUnit)

	// If we advance the clock by 10 seconds, the unit should become
	// available
	Clock.Add(10 * time.Second)
	sts.CheckUnitStatus(t, coordinate.AvailableUnit)
}

// TestNotBeforeAttempt verifies that, if a work unit is created with
// a "not before" time, it is not returned as an attempt.
func TestNotBeforeAttempt(t *testing.T) {
	now := Clock.Now()
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
	sts.SetUp(t)
	defer sts.TearDown(t)

	// We expect no attempts now, because the "not before" time
	// hasn't arrived yet
	sts.CheckWorkUnitOrder(t)

	// If we advance the clock so enough time has passed, we
	// should now see the attempt
	Clock.Add(120 * time.Second)
	sts.CheckWorkUnitOrder(t, "unit")
}

// TestNotBeforePriority tests the intersection of NotBefore and Priority:
// the lower-priority unit that can execute now should.
func TestNotBeforePriority(t *testing.T) {
	now := Clock.Now()
	then := now.Add(60 * time.Second)

	sts := SimpleTestSetup{
		NamespaceName: "TestNotBeforePriority",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// "first" has default priority and can execute now
	_, err := sts.WorkSpec.AddWorkUnit("first", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	// "second" has higher priority, but can't execute yet
	_, err = sts.WorkSpec.AddWorkUnit("second", map[string]interface{}{}, coordinate.WorkUnitMeta{
		Priority:  10.0,
		NotBefore: then,
	})
	assert.NoError(t, err)

	// If we do work units now, we should get only "first"
	sts.CheckWorkUnitOrder(t, "first")

	// Now advance the clock by a minute; we should get "second"
	Clock.Add(60 * time.Second)
	sts.CheckWorkUnitOrder(t, "second")
}

// TestDelayedOutput tests that the output of chained work specs can be
// delayed.
func TestDelayedOutput(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestDelayedOutput",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	two, err := sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	_, err = one.AddWorkUnit("unit", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !assert.NoError(t, err) {
		return
	}

	attempt := sts.RequestOneAttempt(t)
	assert.Equal(t, "one", attempt.WorkUnit().WorkSpec().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []interface{}{
			[]interface{}{
				"u2",
				map[string]interface{}{},
				map[string]interface{}{"delay": 90},
			},
		},
	})
	assert.NoError(t, err)

	// If we get more attempts right now, we should get nothing
	Clock.Add(time.Duration(5) * time.Second)
	sts.RequestNoAttempts(t)

	// If we advance far enough, we should get back the unit for "two"
	Clock.Add(time.Duration(120) * time.Second)
	sts.WorkSpec = two
	sts.CheckWorkUnitOrder(t, "u2")
}

// TestUnitDeletedGone validates that deleting a work unit causes
// operations on it to return ErrGone.
func TestUnitDeletedGone(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestUnitDeletedGone",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// Delete all the work units
	_, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	assert.NoError(t, err)

	_, err = sts.WorkUnit.Status()
	// If the backend does name-based lookup (restclient) we will get
	// ErrNoSuchWorkUnit; if it does object-based lookup (memory,
	// postgres) we will get ErrGone
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		assert.Equal(t, sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "deleted work spec produced unexpected error",
			"%+v", err)
	}
}

// TestUnitSpecDeletedGone validates that deleting a work unit's work
// spec causes operations on the unit to return ErrGone.
func TestUnitSpecDeletedGone(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestUnitSpecDeletedGone",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// Delete the work spec
	err := sts.Namespace.DestroyWorkSpec(sts.WorkSpecName)
	assert.NoError(t, err)

	_, err = sts.WorkUnit.Status()
	// If the backend does name-based lookup (restclient) we will get
	// ErrNoSuchWorkUnit; if it does object-based lookup (memory,
	// postgres) we will get ErrGone
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		assert.Equal(t, sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "deleted work spec produced unexpected error",
			"%+v", err)
	}
}
