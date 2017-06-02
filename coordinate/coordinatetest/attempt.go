// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"reflect"
	"time"
)

// TestAttemptLifetime validates a basic attempt lifetime.
func (s *Suite) TestAttemptLifetime() {
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
	sts.SetUp(s)
	defer sts.TearDown(s)

	// The work unit should be "available"
	sts.CheckUnitStatus(s, coordinate.AvailableUnit)

	// The work unit data should be defined but empty
	s.DataEmpty(sts.WorkUnit)

	// Get an attempt for it
	attempt = sts.RequestOneAttempt(s)

	// The work unit and attempt should both be "pending"
	sts.CheckUnitStatus(s, coordinate.PendingUnit)
	s.AttemptStatus(coordinate.Pending, attempt)

	// The active attempt for the unit should match this
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	if s.NoError(err) {
		s.AttemptMatches(attempt, attempt2)
	}

	// There should be one active attempt for the worker and it should
	// also match
	attempts, err := sts.Worker.ActiveAttempts()
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.AttemptMatches(attempt, attempts[0])
		}
	}

	// The work unit data should (still) be defined but empty
	s.DataEmpty(sts.WorkUnit)

	// Now finish the attempt with some updated data
	err = attempt.Finish(map[string]interface{}{
		"outputs": []string{"yes"},
	})
	s.NoError(err)

	// The unit and should report "finished"
	sts.CheckUnitStatus(s, coordinate.FinishedUnit)
	s.AttemptStatus(coordinate.Finished, attempt)

	// The attempt should still be the active attempt for the unit
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	if s.NoError(err) {
		s.AttemptMatches(attempt, attempt2)
	}

	// The attempt should not be in the active attempt list for the worker
	attempts, err = sts.Worker.ActiveAttempts()
	if s.NoError(err) {
		s.Empty(attempts)
	}

	// Both the unit and the worker should have one archived attempt
	attempts, err = sts.WorkUnit.Attempts()
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.AttemptMatches(attempt, attempts[0])
		}
	}

	attempts, err = sts.Worker.AllAttempts()
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.AttemptMatches(attempt, attempts[0])
		}
	}

	// This should have updated the visible work unit data too
	data, err = sts.WorkUnit.Data()
	if s.NoError(err) {
		s.Len(data, 1)
		if s.Contains(data, "outputs") {
			if s.Len(data["outputs"], 1) {
				s.Equal("yes", reflect.ValueOf(data["outputs"]).Index(0).Interface())
			}
		}
	}

	// For bonus points, force-clear the active attempt
	err = sts.WorkUnit.ClearActiveAttempt()
	s.NoError(err)

	// This should have pushed the unit back to available
	sts.CheckUnitStatus(s, coordinate.AvailableUnit)

	// This also should have reset the work unit data
	s.DataEmpty(sts.WorkUnit)

	// But, this should not have reset the historical attempts
	attempts, err = sts.WorkUnit.Attempts()
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.AttemptMatches(attempt, attempts[0])
		}
	}

	attempts, err = sts.Worker.AllAttempts()
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.AttemptMatches(attempt, attempts[0])
		}
	}
}

// TestAttemptMetadata validates the various bits of data associated
// with a single attempt.
func (s *Suite) TestAttemptMetadata() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptMetadata",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "a",
		WorkUnitData:  map[string]interface{}{"from": "wu"},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)
	attempt := sts.RequestOneAttempt(s)

	// Start checking things
	start := s.Clock.Now()
	startTime, err := attempt.StartTime()
	if s.NoError(err) {
		s.WithinDuration(start, startTime, 1*time.Millisecond)
	}

	s.DataMatches(attempt, map[string]interface{}{"from": "wu"})

	endTime, err := attempt.EndTime()
	if s.NoError(err) {
		s.WithinDuration(time.Time{}, endTime, 1*time.Millisecond)
	}

	expirationTime, err := attempt.ExpirationTime()
	if s.NoError(err) {
		s.Equal(15*time.Minute, expirationTime.Sub(startTime))
	}

	// Renew the lease, giving new work unit data
	s.Clock.Add(10 * time.Second)
	renewTime := s.Clock.Now()
	renewDuration := 5 * time.Minute
	err = attempt.Renew(renewDuration,
		map[string]interface{}{"from": "renew"})
	s.NoError(err)

	startTime, err = attempt.StartTime()
	if s.NoError(err) {
		// no change from above
		s.WithinDuration(start, startTime, 1*time.Millisecond)
	}

	s.DataMatches(attempt, map[string]interface{}{"from": "renew"})

	endTime, err = attempt.EndTime()
	if s.NoError(err) {
		s.WithinDuration(time.Time{}, endTime, 1*time.Millisecond)
	}

	expectedExpiration := renewTime.Add(renewDuration)
	expirationTime, err = attempt.ExpirationTime()
	if s.NoError(err) {
		s.WithinDuration(expectedExpiration, expirationTime, 1*time.Millisecond)
	}

	// Finish the attempt
	err = attempt.Finish(map[string]interface{}{"from": "finish"})
	s.NoError(err)

	startTime, err = attempt.StartTime()
	if s.NoError(err) {
		// no change from above
		s.WithinDuration(start, startTime, 1*time.Millisecond)
	}

	s.DataMatches(attempt, map[string]interface{}{"from": "finish"})

	endTime, err = attempt.EndTime()
	if s.NoError(err) {
		s.True(endTime.After(startTime),
			"start time %+v end time %+v", startTime, endTime)
	}

	// don't check expiration time here
}

// TestWorkUnitChaining tests that completing work units in one work spec
// will cause work units to appear in another, if so configured.
func (s *Suite) TestWorkUnitChaining() {
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
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !s.NoError(err) {
		return
	}
	// RequestAttempts always returns this
	sts.WorkSpec = one

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	if !s.NoError(err) {
		return
	}

	// Create and perform a work unit, with no output
	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(s)
	err = attempt.Finish(nil)
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Empty(units)
	}

	// Create and perform a work unit, with a map output
	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	attempt = sts.RequestOneAttempt(s)
	err = attempt.Finish(map[string]interface{}{
		"output": map[string]interface{}{
			"two_b": map[string]interface{}{"k": "v"},
		},
	})
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 1)
		if s.Contains(units, "two_b") {
			s.DataMatches(units["two_b"], map[string]interface{}{"k": "v"})
		}
	}

	// Create and perform a work unit, with a slice output
	_, err = one.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	attempt = sts.RequestOneAttempt(s)
	err = attempt.Finish(map[string]interface{}{
		"output": []string{"two_c", "two_cc"},
	})
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 3)
		s.Contains(units, "two_b")
		s.Contains(units, "two_cc")
		if s.Contains(units, "two_c") {
			s.DataEmpty(units["two_c"])
		}
	}

	// Put the output in the original work unit data
	_, err = one.AddWorkUnit("d", map[string]interface{}{
		"output": []string{"two_d"},
	}, coordinate.WorkUnitMeta{})
	s.NoError(err)
	attempt = sts.RequestOneAttempt(s)
	err = attempt.Finish(nil)
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Len(units, 4)
		s.Contains(units, "two_b")
		s.Contains(units, "two_c")
		s.Contains(units, "two_cc")
		s.Contains(units, "two_d")
	}
}

// TestChainingMixed uses a combination of strings and tuples in its
// "output" data.
func (s *Suite) TestChainingMixed() {
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
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !s.NoError(err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !s.NoError(err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(s)
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
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		if s.Contains(units, "key") {
			s.DataMatches(units["key"], map[string]interface{}{"data": "x"})
			s.UnitHasPriority(units["key"], 10.0)
		}
	}
}

// TestChainingTwoStep separately renews an attempt to insert an output
// key, then finishes the work unit; it should still chain.
func (s *Suite) TestChainingTwoStep() {
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
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !s.NoError(err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if !s.NoError(err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(s)
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
	s.NoError(err)

	err = attempt.Finish(nil)
	s.NoError(err)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		if s.Contains(units, "\x01\x02\x03\x04") {
			unit = units["\x01\x02\x03\x04"]
			s.DataEmpty(unit)
			s.UnitHasPriority(unit, 0.0)
		}
	}
}

// TestChainingExpiry tests that, if an attempt finishes but is no
// longer the active attempt, then its successor work units will not
// be created.
func (s *Suite) TestChainingExpiry() {
	var (
		one, two coordinate.WorkSpec
		err      error
		unit     coordinate.WorkUnit
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingExpiry",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if !s.NoError(err) {
		return
	}
	sts.WorkSpec = one

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	if !s.NoError(err) {
		return
	}

	// Create and perform a work unit, with no output
	unit, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !s.NoError(err) {
		return
	}

	attempt := sts.RequestOneAttempt(s)

	// But wait!  We got preempted
	err = unit.ClearActiveAttempt()
	s.NoError(err)
	sts.RequestOneAttempt(s)

	// Now, let the original attempt finish, trying to generate
	// more outputs
	err = attempt.Finish(map[string]interface{}{
		"output": []string{"unit"},
	})
	s.NoError(err)

	// Since attempt is no longer active, this shouldn't generate
	// new outputs
	units, err := two.WorkUnits(coordinate.WorkUnitQuery{})
	if s.NoError(err) {
		s.Empty(units)
	}
}

// TestChainingDuplicate tests that work unit chaining still works
// even when the same output work unit is generated twice (it should
// get retried).
func (s *Suite) TestChainingDuplicate() {
	var (
		err      error
		one, two coordinate.WorkSpec
		attempt  coordinate.Attempt
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestChainingDuplicate",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	one, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"then":     "two",
		"priority": 1,
	})
	if !s.NoError(err) {
		return
	}

	two, err = sts.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 2,
	})
	if !s.NoError(err) {
		return
	}

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	s.NoError(err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(s)
	s.Equal("a", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	s.NoError(err)

	sts.WorkSpec = two
	attempt = sts.RequestOneAttempt(s)
	s.Equal("z", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{})
	s.NoError(err)

	sts.WorkSpec = one
	attempt = sts.RequestOneAttempt(s)
	s.Equal("b", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	s.NoError(err)

	sts.WorkSpec = two
	attempt = sts.RequestOneAttempt(s)
	s.Equal("z", attempt.WorkUnit().Name())

	err = attempt.Finish(map[string]interface{}{})
	s.NoError(err)

	sts.RequestNoAttempts(s)
}

// TestAttemptExpiration validates that an attempt's status will switch
// (on its own) to "expired" after a timeout.
func (s *Suite) TestAttemptExpiration() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptExpiration",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "a",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if !(s.NoError(err) && s.Len(attempts, 1)) {
		return
	}
	attempt := attempts[0]

	status, err := attempt.Status()
	if s.NoError(err) {
		s.Equal(coordinate.Pending, status)
	}

	sts.RequestNoAttempts(s)

	// There is a default expiration of 15 minutes (checked elsewhere)
	// So if we wait for, say, 20 minutes we should become expired
	s.Clock.Add(time.Duration(20) * time.Minute)
	status, err = attempt.Status()
	if s.NoError(err) {
		s.Equal(coordinate.Expired, status)
	}

	// The work unit should be "available" for all purposes
	meta, err := sts.WorkSpec.Meta(true)
	if s.NoError(err) {
		s.Equal(1, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
	}
	sts.CheckUnitStatus(s, coordinate.AvailableUnit)

	// If we request more attempts we should get back the expired
	// unit again
	attempt = sts.RequestOneAttempt(s)
	s.AttemptStatus(coordinate.Pending, attempt)
}

// TestRetryDelay verifies that the delay option on the Retry() call works.
func (s *Suite) TestRetryDelay() {
	sts := SimpleTestSetup{
		NamespaceName: "TestRetryDelay",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	sts.CheckUnitStatus(s, coordinate.AvailableUnit)

	attempt := sts.RequestOneAttempt(s)
	err := attempt.Retry(nil, 90*time.Second)
	s.NoError(err)

	s.Clock.Add(60 * time.Second)
	sts.CheckUnitStatus(s, coordinate.DelayedUnit)
	sts.RequestNoAttempts(s)

	s.Clock.Add(60 * time.Second)
	sts.CheckUnitStatus(s, coordinate.AvailableUnit)
	sts.CheckWorkUnitOrder(s, "unit")
}

// TestAttemptFractionalStart verifies that an attempt that starts at
// a non-integral time (as most of them are) can find itself.  This is
// a regression test for a specific issue in restclient.
func (s *Suite) TestAttemptFractionalStart() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptFractionalStart",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	s.Clock.Add(500 * time.Millisecond)

	attempt := sts.RequestOneAttempt(s)
	// Since restclient uses the start time as one of its keys to
	// look up attempts, but the code uses two different ways to
	// format it, having a fractional second means that this call
	// will fail when restserver can't find the attempt.  Actually
	// the call above that adds half a second to the mock clock ruins
	// a lot of tests without the bug fix since *none* of them can
	// find their own attempts.
	err := attempt.Finish(nil)
	s.NoError(err)
}

// TestAttemptGone verifies that, if a work unit is deleted, its
// attempts return ErrGone for things.
func (s *Suite) TestAttemptGone() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAttemptGone",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkUnitName:  "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	attempt := sts.RequestOneAttempt(s)

	_, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	s.NoError(err)

	_, err = attempt.Status()
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		s.Equal(sts.WorkUnitName, nswu.Name)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("unexpected error deleting work spec",
			"%+v", err)
	}
}

// TestMaxRetries is a simple test for the max_retries work spec
// option.
func (s *Suite) TestMaxRetries() {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRetries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkSpecData: map[string]interface{}{
			"max_retries": 2,
		},
		WorkUnitName: "unit",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	sts.RequestOneAttempt(s)

	// Force the clock far enough ahead that the attempt has expired.
	// We should get an attempt for the same work unit again.
	s.Clock.Add(1 * time.Hour)
	sts.RequestOneAttempt(s)

	// Force the clock ahead again, and get an attempt.  Since
	// we exceed max_retries we should not get an attempt.
	s.Clock.Add(1 * time.Hour)
	sts.RequestNoAttempts(s)
	sts.CheckUnitStatus(s, coordinate.FailedUnit)
	data, err := sts.WorkUnit.Data()
	if s.NoError(err) {
		s.Equal("too many retries", data["traceback"])
	}
}

// TestMaxRetriesMulti tests both setting max_retries and max_getwork.
func (s *Suite) TestMaxRetriesMulti() {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRetries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkSpecData: map[string]interface{}{
			"max_getwork": 2,
			"max_retries": 1,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := sts.AddWorkUnit(name)
		if !s.NoError(err) {
			return
		}
	}

	// Now we should be able to request work and get both a and b
	req := coordinate.AttemptRequest{
		NumberOfWorkUnits: 10,
	}
	attempts, err := sts.Worker.RequestAttempts(req)
	if s.NoError(err) {
		if s.Len(attempts, 2) {
			s.Equal("a", attempts[0].WorkUnit().Name())
			s.Equal("b", attempts[1].WorkUnit().Name())
		}
	}

	// Let the first work unit finish and the second time out
	s.Clock.Add(1 * time.Minute)
	err = attempts[0].Finish(nil)
	s.NoError(err)

	s.Clock.Add(1 * time.Hour)

	// Now get two more work units.  We expect the system to find
	// b and c, notice that b is expired, and just return c.
	attempts, err = sts.Worker.RequestAttempts(req)
	if s.NoError(err) {
		if s.Len(attempts, 1) {
			s.Equal("c", attempts[0].WorkUnit().Name())
		}
	}
}

// TestMaxRetriesMultiBatch is like TestMaxRetriesMulti, but has an
// entire batch go over the retry limit.
func (s *Suite) TestMaxRetriesMultiBatch() {
	sts := SimpleTestSetup{
		NamespaceName: "TestMaxRetries",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
		WorkSpecData: map[string]interface{}{
			"max_getwork": 2,
			"max_retries": 1,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := sts.AddWorkUnit(name)
		if !s.NoError(err) {
			return
		}
	}

	// Now we should be able to request work and get both a and b
	req := coordinate.AttemptRequest{
		NumberOfWorkUnits: 10,
	}
	attempts, err := sts.Worker.RequestAttempts(req)
	if s.NoError(err) {
		if s.Len(attempts, 2) {
			s.Equal("a", attempts[0].WorkUnit().Name())
			s.Equal("b", attempts[1].WorkUnit().Name())
		}
	}

	// Let both work units time out
	s.Clock.Add(1 * time.Hour)

	// Now get two more work units.  We expect the system to find
	// a and b, see both are expired, find a new batch, and get
	// both c and d
	attempts, err = sts.Worker.RequestAttempts(req)
	if s.NoError(err) {
		if s.Len(attempts, 2) {
			s.Equal("c", attempts[0].WorkUnit().Name())
			s.Equal("d", attempts[1].WorkUnit().Name())
		}
	}
}
