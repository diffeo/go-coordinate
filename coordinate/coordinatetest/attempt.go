// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

// TestAttemptLifetime validates a basic attempt lifetime.
func TestAttemptLifetime(t *testing.T) {
	var (
		err               error
		data              map[string]interface{}
		attempt, attempt2 coordinate.Attempt
	)
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptLifetime",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "a",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// The work unit should be "available"
	sts.CheckUnitStatus(t, coordinate.AvailableUnit)

	// The work unit data should be defined but empty
	DataEmpty(t, sts.WorkUnit)

	// Get an attempt for it
	attempt = sts.RequestOneAttempt(t)

	// The work unit and attempt should both be "pending"
	sts.CheckUnitStatus(t, coordinate.PendingUnit)
	AttemptStatus(t, coordinate.Pending, attempt)

	// The active attempt for the unit should match this
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	if assert.NoError(t, err) {
		AttemptMatches(t, attempt, attempt2)
	}

	// There should be one active attempt for the worker and it should
	// also match
	attempts, err := sts.Worker.ActiveAttempts()
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 1) {
			AttemptMatches(t, attempt, attempts[0])
		}
	}

	// The work unit data should (still) be defined but empty
	DataEmpty(t, sts.WorkUnit)

	// Now finish the attempt with some updated data
	err = attempt.Finish(map[string]interface{}{
		"outputs": []string{"yes"},
	})
	assert.NoError(t, err)

	// The unit and should report "finished"
	sts.CheckUnitStatus(t, coordinate.FinishedUnit)
	AttemptStatus(t, coordinate.Finished, attempt)

	// The attempt should still be the active attempt for the unit
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	if assert.NoError(t, err) {
		AttemptMatches(t, attempt, attempt2)
	}

	// The attempt should not be in the active attempt list for the worker
	attempts, err = sts.Worker.ActiveAttempts()
	if assert.NoError(t, err) {
		assert.Empty(t, attempts)
	}

	// Both the unit and the worker should have one archived attempt
	attempts, err = sts.WorkUnit.Attempts()
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 1) {
			AttemptMatches(t, attempt, attempts[0])
		}
	}

	attempts, err = sts.Worker.AllAttempts()
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 1) {
			AttemptMatches(t, attempt, attempts[0])
		}
	}

	// This should have updated the visible work unit data too
	data, err = sts.WorkUnit.Data()
	if assert.NoError(t, err) {
		assert.Len(t, data, 1)
		if assert.Contains(t, data, "outputs") {
			if assert.Len(t, data["outputs"], 1) {
				assert.Equal(t, "yes", reflect.ValueOf(data["outputs"]).Index(0).Interface())
			}
		}
	}

	// For bonus points, force-clear the active attempt
	err = sts.WorkUnit.ClearActiveAttempt()
	assert.NoError(t, err)

	// This should have pushed the unit back to available
	sts.CheckUnitStatus(t, coordinate.AvailableUnit)

	// This also should have reset the work unit data
	DataEmpty(t, sts.WorkUnit)

	// But, this should not have reset the historical attempts
	attempts, err = sts.WorkUnit.Attempts()
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 1) {
			AttemptMatches(t, attempt, attempts[0])
		}
	}

	attempts, err = sts.Worker.AllAttempts()
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 1) {
			AttemptMatches(t, attempt, attempts[0])
		}
	}
}

// TestAttemptMetadata validates the various bits of data associated
// with a single attempt.
func TestAttemptMetadata(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptMetadata",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "a",
		WorkUnitData:  map[string]interface{}{"from": "wu"},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)
	attempt := sts.RequestOneAttempt(t)

	// Start checking things
	start := Clock.Now()
	startTime, err := attempt.StartTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, start, startTime, 1*time.Millisecond)
	}

	DataMatches(t, attempt, map[string]interface{}{"from": "wu"})

	endTime, err := attempt.EndTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, time.Time{}, endTime, 1*time.Millisecond)
	}

	expirationTime, err := attempt.ExpirationTime()
	if assert.NoError(t, err) {
		assert.Equal(t, 15*time.Minute, expirationTime.Sub(startTime))
	}

	// Renew the lease, giving new work unit data
	Clock.Add(10 * time.Second)
	renewTime := Clock.Now()
	renewDuration := 5 * time.Minute
	err = attempt.Renew(renewDuration,
		map[string]interface{}{"from": "renew"})
	assert.NoError(t, err)

	startTime, err = attempt.StartTime()
	if assert.NoError(t, err) {
		// no change from above
		assert.WithinDuration(t, start, startTime, 1*time.Millisecond)
	}

	DataMatches(t, attempt, map[string]interface{}{"from": "renew"})

	endTime, err = attempt.EndTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, time.Time{}, endTime, 1*time.Millisecond)
	}

	expectedExpiration := renewTime.Add(renewDuration)
	expirationTime, err = attempt.ExpirationTime()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, expectedExpiration, expirationTime, 1*time.Millisecond)
	}

	// Finish the attempt
	err = attempt.Finish(map[string]interface{}{"from": "finish"})
	assert.NoError(t, err)

	startTime, err = attempt.StartTime()
	if assert.NoError(t, err) {
		// no change from above
		assert.WithinDuration(t, start, startTime, 1*time.Millisecond)
	}

	DataMatches(t, attempt, map[string]interface{}{"from": "finish"})

	endTime, err = attempt.EndTime()
	if assert.NoError(t, err) {
		assert.True(t, endTime.After(startTime),
			"start time %+v end time %+v", startTime, endTime)
	}

	// don't check expiration time here
}

// TestWorkUnitChaining tests that completing work units in one work spec
// will cause work units to appear in another, if so configured.
func TestWorkUnitChaining(t *testing.T) {
	var (
		err      error
		one, two coordinate.WorkSpec
		units    map[string]coordinate.WorkUnit
		attempt  coordinate.Attempt
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestWorkUnitChaining",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !assert.NoError(t, err) {
		return
	}
	// RequestAttempts always returns this
	sts.WorkSpec = one

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	if !assert.NoError(t, err) {
		return
	}

	// Create and perform a work unit, with no output
	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(t)
	err = attempt.Finish(nil)
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Empty(t, units)
	}

	// Create and perform a work unit, with a map output
	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	attempt = sts.RequestOneAttempt(t)
	err = attempt.Finish(map[string]interface{}{
		"output": map[string]interface{}{
			"two_b": map[string]interface{}{"k": "v"},
		},
	})
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 1)
		if assert.Contains(t, units, "two_b") {
			DataMatches(t, units["two_b"], map[string]interface{}{"k": "v"})
		}
	}

	// Create and perform a work unit, with a slice output
	_, err = one.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	attempt = sts.RequestOneAttempt(t)
	err = attempt.Finish(map[string]interface{}{
		"output": []string{"two_c", "two_cc"},
	})
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 3)
		assert.Contains(t, units, "two_b")
		assert.Contains(t, units, "two_cc")
		if assert.Contains(t, units, "two_c") {
			DataEmpty(t, units["two_c"])
		}
	}

	// Put the output in the original work unit data
	_, err = one.AddWorkUnit("d", map[string]interface{}{
		"output": []string{"two_d"},
	}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)
	attempt = sts.RequestOneAttempt(t)
	err = attempt.Finish(nil)
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Len(t, units, 4)
		assert.Contains(t, units, "two_b")
		assert.Contains(t, units, "two_c")
		assert.Contains(t, units, "two_cc")
		assert.Contains(t, units, "two_d")
	}
}

// TestChainingMixed uses a combination of strings and tuples in its
// "output" data.
func TestChainingMixed(t *testing.T) {
	var (
		one, two coordinate.WorkSpec
		attempt  coordinate.Attempt
		units    map[string]coordinate.WorkUnit
		err      error
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingMixed",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(t)
	err = attempt.Finish(map[string]interface{}{
		"output": []interface{}{
			"key",
			cborrpc.PythonTuple{Items: []interface{}{
				"key",
				map[string]interface{}{
					"data": "x",
				},
				map[string]interface{}{
					"priority": 10.0,
				},
			}},
		},
	})
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		if assert.Contains(t, units, "key") {
			DataMatches(t, units["key"], map[string]interface{}{"data": "x"})
			UnitHasPriority(t, units["key"], 10.0)
		}
	}
}

// TestChainingTwoStep separately renews an attempt to insert an output
// key, then finishes the work unit; it should still chain.
func TestChainingTwoStep(t *testing.T) {
	var (
		one, two coordinate.WorkSpec
		attempt  coordinate.Attempt
		units    map[string]coordinate.WorkUnit
		unit     coordinate.WorkUnit
		err      error
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingTwoStep",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !assert.NoError(t, err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(t)
	err = attempt.Renew(900*time.Second,
		map[string]interface{}{
			"output": []interface{}{
				[]byte{1, 2, 3, 4},
				cborrpc.PythonTuple{Items: []interface{}{
					[]byte{1, 2, 3, 4},
					map[interface{}]interface{}{},
					map[interface{}]interface{}{
						"priority": 0,
					},
				}},
			},
		})
	assert.NoError(t, err)

	err = attempt.Finish(nil)
	assert.NoError(t, err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		if assert.Contains(t, units, "\x01\x02\x03\x04") {
			unit = units["\x01\x02\x03\x04"]
			DataEmpty(t, unit)
			UnitHasPriority(t, unit, 0.0)
		}
	}
}

// TestChainingExpiry tests that, if an attempt finishes but is no
// longer the active attempt, then its successor work units will not
// be created.
func TestChainingExpiry(t *testing.T) {
	var (
		one, two coordinate.WorkSpec
		err      error
		unit     coordinate.WorkUnit
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingExpiry",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !assert.NoError(t, err) {
		return
	}
	sts.WorkSpec = one

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	if !assert.NoError(t, err) {
		return
	}

	// Create and perform a work unit, with no output
	unit, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !assert.NoError(t, err) {
		return
	}

	attempt := sts.RequestOneAttempt(t)

	// But wait!  We got preempted
	err = unit.ClearActiveAttempt()
	assert.NoError(t, err)
	sts.RequestOneAttempt(t)

	// Now, let the original attempt finish, trying to generate
	// more outputs
	err = attempt.Finish(map[string]interface{}{
		"output": []string{"unit"},
	})
	assert.NoError(t, err)

	// Since attempt is no longer active, this shouldn't generate
	// new outputs
	units, err := two.WorkUnits(coordinate.WorkUnitQuery{})
	if assert.NoError(t, err) {
		assert.Empty(t, units)
	}
}

// TestChainingDuplicate tests that work unit chaining still works
// even when the same output work unit is generated twice (it should
// get retried).
func TestChainingDuplicate(t *testing.T) {
	var (
		err      error
		one, two coordinate.WorkSpec
		attempt  coordinate.Attempt
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingDuplicate",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"then":     "two",
		"priority": 1,
	})
	if !assert.NoError(t, err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 2,
	})
	if !assert.NoError(t, err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	assert.NoError(t, err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(t)
	assert.Equal(t, "a", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	assert.NoError(t, err)

	sts.WorkSpec = two
	attempt = sts.RequestOneAttempt(t)
	assert.Equal(t, "z", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{})
	assert.NoError(t, err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(t)
	assert.Equal(t, "b", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	assert.NoError(t, err)

	sts.WorkSpec = two
	attempt = sts.RequestOneAttempt(t)
	assert.Equal(t, "z", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{})
	assert.NoError(t, err)

	sts.RequestNoAttempts(t)
}

// TestAttemptExpiration validates that an attempt's status will switch
// (on its own) to "expired" after a timeout.
func TestAttemptExpiration(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptExpiration",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "a",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if !(assert.NoError(t, err) && assert.Len(t, attempts, 1)) {
		return
	}
	attempt := attempts[0]

	status, err := attempt.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, coordinate.Pending, status)
	}

	sts.RequestNoAttempts(t)

	// There is a default expiration of 15 minutes (checked elsewhere)
	// So if we wait for, say, 20 minutes we should become expired
	Clock.Add(time.Duration(20) * time.Minute)
	status, err = attempt.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, coordinate.Expired, status)
	}

	// The work unit should be "available" for all purposes
	meta, err := sts.WorkSpec.Meta(true)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
	}
	sts.CheckUnitStatus(t, coordinate.AvailableUnit)

	// If we request more attempts we should get back the expired
	// unit again
	attempt = sts.RequestOneAttempt(t)
	AttemptStatus(t, coordinate.Pending, attempt)
}

// TestRetryDelay verifies that the delay option on the Retry() call works.
func TestRetryDelay(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestRetryDelay",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	sts.CheckUnitStatus(t, coordinate.AvailableUnit)

	attempt := sts.RequestOneAttempt(t)
	err := attempt.Retry(nil, 90*time.Second)
	assert.NoError(t, err)

	Clock.Add(60 * time.Second)
	sts.CheckUnitStatus(t, coordinate.DelayedUnit)
	sts.RequestNoAttempts(t)

	Clock.Add(60 * time.Second)
	sts.CheckUnitStatus(t, coordinate.AvailableUnit)
	sts.CheckWorkUnitOrder(t, "unit")
}

// TestAttemptFractionalStart verifies that an attempt that starts at
// a non-integral time (as most of them are) can find itself.  This is
// a regression test for a specific issue in restclient.
func TestAttemptFractionalStart(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptFractionalStart",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	Clock.Add(500 * time.Millisecond)

	attempt := sts.RequestOneAttempt(t)
	// Since restclient uses the start time as one of its keys to
	// look up attempts, but the code uses two different ways to
	// format it, having a fractional second means that this call
	// will fail when restserver can't find the attempt.  Actually
	// the call above that adds half a second to the mock clock ruins
	// a lot of tests without the bug fix since *none* of them can
	// find their own attempts.
	err := attempt.Finish(nil)
	assert.NoError(t, err)
}

// TestAttemptGone verifies that, if a work unit is deleted, its
// attempts return ErrGone for things.
func TestAttemptGone(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptGone",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	attempt := sts.RequestOneAttempt(t)

	_, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	assert.NoError(t, err)

	_, err = attempt.Status()
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		assert.Equal(t, sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "unexpected error deleting work spec",
			"%+v", err)
	}
}

// TestMaxRetries is a simple test for the max_retries work spec
// option.
func TestMaxRetries(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRetries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkSpecData: map[string]interface{}{
			"max_retries": 2,
		},
		WorkUnitName: "unit",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	sts.RequestOneAttempt(t)

	// Force the clock far enough ahead that the attempt has expired.
	// We should get an attempt for the same work unit again.
	Clock.Add(1 * time.Hour)
	sts.RequestOneAttempt(t)

	// Force the clock ahead again, and get an attempt.  Since
	// we exceed max_retries we should not get an attempt.
	Clock.Add(1 * time.Hour)
	sts.RequestNoAttempts(t)
	sts.CheckUnitStatus(t, coordinate.FailedUnit)
	data, err := sts.WorkUnit.Data()
	if assert.NoError(t, err) {
		assert.Equal(t, "too many retries", data["traceback"])
	}
}

// TestMaxRetriesMulti tests both setting max_retries and max_getwork.
func TestMaxRetriesMulti(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRetries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkSpecData: map[string]interface{}{
			"max_getwork": 2,
			"max_retries": 1,
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := sts.AddWorkUnit(name)
		if !assert.NoError(t, err) {
			return
		}
	}

	// Now we should be able to request work and get both a and b
	req := coordinate.AttemptRequest{
		NumberOfWorkUnits: 10,
	}
	attempts, err := sts.Worker.RequestAttempts(req)
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 2) {
			assert.Equal(t, "a", attempts[0].WorkUnit().Name())
			assert.Equal(t, "b", attempts[1].WorkUnit().Name())
		}
	}

	// Let the first work unit finish and the second time out
	Clock.Add(1 * time.Minute)
	err = attempts[0].Finish(nil)
	assert.NoError(t, err)

	Clock.Add(1 * time.Hour)

	// Now get two more work units.  We expect the system to find
	// b and c, notice that b is expired, and keep looking to return
	// c and d.
	attempts, err = sts.Worker.RequestAttempts(req)
	if assert.NoError(t, err) {
		if assert.Len(t, attempts, 2) {
			assert.Equal(t, "c", attempts[0].WorkUnit().Name())
			assert.Equal(t, "d", attempts[1].WorkUnit().Name())
		}
	}
}
