// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
	"reflect"
	"time"
)

// TestAttemptLifetime validates a basic attempt lifetime.
func (s *Suite) TestAttemptLifetime(c *check.C) {
	var (
		err               error
		data              map[string]interface{}
		attempt, attempt2 coordinate.Attempt
		aStatus           coordinate.AttemptStatus
		uStatus           coordinate.WorkUnitStatus
	)
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "a",
	}
	sts.Do(s, c)

	// The work unit should be "available"
	uStatus, err = sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.AvailableUnit)

	// The work unit data should be defined but empty
	data, err = sts.WorkUnit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// Get an attempt for it
	attempt = sts.RequestOneAttempt(c)

	// The work unit should be "pending"
	uStatus, err = sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.PendingUnit)

	// The attempt should be "pending" too
	aStatus, err = attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(aStatus, check.Equals, coordinate.Pending)

	// The active attempt for the unit should match this
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	c.Assert(err, check.IsNil)
	c.Check(attempt2, AttemptMatches, attempt)

	// There should be one active attempt for the worker and it should
	// also match
	attempts, err := sts.Worker.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	// The work unit data should (still) be defined but empty
	data, err = sts.WorkUnit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// Now finish the attempt with some updated data
	err = attempt.Finish(map[string]interface{}{
		"outputs": []string{"yes"},
	})
	c.Assert(err, check.IsNil)

	// The unit should report "finished"
	uStatus, err = sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.FinishedUnit)

	// The attempt should report "finished"
	aStatus, err = attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(aStatus, check.Equals, coordinate.Finished)

	// The attempt should still be the active attempt for the unit
	attempt2, err = sts.WorkUnit.ActiveAttempt()
	c.Assert(err, check.IsNil)
	c.Check(attempt2, AttemptMatches, attempt)

	// The attempt should not be in the active attempt list for the worker
	attempts, err = sts.Worker.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	// Both the unit and the worker should have one archived attempt
	attempts, err = sts.WorkUnit.Attempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	attempts, err = sts.Worker.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	// This should have updated the visible work unit data too
	data, err = sts.WorkUnit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 1)
	c.Check(data["outputs"], check.HasLen, 1)
	c.Check(reflect.ValueOf(data["outputs"]).Index(0).Interface(),
		check.Equals, "yes")

	// For bonus points, force-clear the active attempt
	err = sts.WorkUnit.ClearActiveAttempt()
	c.Assert(err, check.IsNil)

	// This should have pushed the unit back to available
	uStatus, err = sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.AvailableUnit)

	// This also should have reset the work unit data
	data, err = sts.WorkUnit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// But, this should not have reset the historical attempts
	attempts, err = sts.WorkUnit.Attempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	attempts, err = sts.Worker.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}
}

// TestAttemptMetadata validates the various bits of data associated
// with a single attempt.
func (s *Suite) TestAttemptMetadata(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "a",
		WorkUnitData: map[string]interface{}{"from": "wu"},
	}
	sts.Do(s, c)
	attempt := sts.RequestOneAttempt(c)

	// Start checking things
	start := s.Clock.Now()
	startTime, err := attempt.StartTime()
	c.Assert(err, check.IsNil)
	c.Check(startTime, SameTime, start)

	data, err := attempt.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, map[string]interface{}{"from": "wu"})

	endTime, err := attempt.EndTime()
	c.Assert(err, check.IsNil)
	c.Check(endTime, SameTime, time.Time{})

	expirationTime, err := attempt.ExpirationTime()
	c.Assert(err, check.IsNil)
	c.Check(expirationTime.Sub(startTime), check.Equals, time.Duration(15)*time.Minute)

	// Renew the lease, giving new work unit data
	s.Clock.Add(time.Duration(10) * time.Second)
	renewTime := s.Clock.Now()
	renewDuration := time.Duration(5) * time.Minute
	err = attempt.Renew(renewDuration,
		map[string]interface{}{"from": "renew"})
	c.Assert(err, check.IsNil)

	startTime, err = attempt.StartTime()
	c.Assert(err, check.IsNil)
	c.Check(startTime, SameTime, start) // no change from above

	data, err = attempt.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, map[string]interface{}{"from": "renew"})

	endTime, err = attempt.EndTime()
	c.Assert(err, check.IsNil)
	c.Check(endTime, SameTime, time.Time{})

	expirationTime, err = attempt.ExpirationTime()
	c.Assert(err, check.IsNil)
	expectedExpiration := renewTime.Add(renewDuration)
	c.Check(expirationTime, SameTime, expectedExpiration)

	// Finish the attempt
	err = attempt.Finish(map[string]interface{}{"from": "finish"})
	c.Assert(err, check.IsNil)

	startTime, err = attempt.StartTime()
	c.Assert(err, check.IsNil)
	c.Check(startTime, SameTime, start) // no change from above

	data, err = attempt.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, map[string]interface{}{"from": "finish"})

	endTime, err = attempt.EndTime()
	c.Assert(err, check.IsNil)
	c.Check(endTime.After(startTime), check.Equals, true,
		check.Commentf("start time %+v end time %+v", startTime, endTime))

	// don't check expiration time here
}

// TestWorkUnitChaining tests that completing work units in one work spec
// will cause work units to appear in another, if so configured.
func (s *Suite) TestWorkUnitChaining(c *check.C) {
	var (
		err      error
		worker   coordinate.Worker
		one, two coordinate.WorkSpec
		units    map[string]coordinate.WorkUnit
		attempts []coordinate.Attempt
		data     map[string]interface{}
		ok       bool
	)

	one, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)

	two, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	c.Assert(err, check.IsNil)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// Create and perform a work unit, with no output
	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Finish(nil)
	c.Assert(err, check.IsNil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{})

	// Create and perform a work unit, with a map output
	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Finish(map[string]interface{}{
		"output": map[string]interface{}{
			"two_b": map[string]interface{}{"k": "v"},
		},
	})
	c.Assert(err, check.IsNil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{"two_b"})

	if _, ok = units["two_b"]; ok {
		data, err = units["two_b"].Data()
		c.Assert(err, check.IsNil)
		c.Check(data, check.DeepEquals, map[string]interface{}{"k": "v"})
	}

	// Create and perform a work unit, with a slice output
	_, err = one.AddWorkUnit("c", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Finish(map[string]interface{}{
		"output": []string{"two_c", "two_cc"},
	})
	c.Assert(err, check.IsNil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{"two_b", "two_c", "two_cc"})

	if _, ok = units["two_c"]; ok {
		data, err = units["two_c"].Data()
		c.Assert(err, check.IsNil)
		c.Check(data, check.HasLen, 0)
	}

	// Put the output in the original work unit data
	_, err = one.AddWorkUnit("d", map[string]interface{}{
		"output": []string{"two_d"},
	}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Finish(nil)
	c.Assert(err, check.IsNil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{"two_b", "two_c", "two_cc", "two_d"})
}

// TestChainingMixed uses a combination of strings and tuples in its
// "output" data.
func (s *Suite) TestChainingMixed(c *check.C) {
	var (
		one, two coordinate.WorkSpec
		worker   coordinate.Worker
		attempts []coordinate.Attempt
		units    map[string]coordinate.WorkUnit
		unit     coordinate.WorkUnit
		data     map[string]interface{}
		priority float64
		ok       bool
		err      error
	)

	one, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)

	two, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	c.Assert(err, check.IsNil)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Finish(map[string]interface{}{
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
	c.Assert(err, check.IsNil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{"key"})
	if unit, ok = units["key"]; ok {
		data, err = unit.Data()
		c.Assert(err, check.IsNil)
		c.Check(data, check.DeepEquals, map[string]interface{}{"data": "x"})

		priority, err = unit.Priority()
		c.Assert(err, check.IsNil)
		c.Check(priority, check.Equals, 10.0)
	}
}

// TestChainingTwoStep separately renews an attempt to insert an output
// key, then finishes the work unit; it should still chain.
func (s *Suite) TestChainingTwoStep(c *check.C) {
	var (
		one, two coordinate.WorkSpec
		worker   coordinate.Worker
		attempts []coordinate.Attempt
		units    map[string]coordinate.WorkUnit
		unit     coordinate.WorkUnit
		data     map[string]interface{}
		priority float64
		ok       bool
		err      error
	)

	one, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)

	two, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	c.Assert(err, check.IsNil)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	err = attempts[0].Renew(time.Duration(900)*time.Second,
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
	c.Assert(err, check.IsNil)

	err = attempts[0].Finish(nil)

	units, err = two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, HasKeys, []string{"\x01\x02\x03\x04"})
	if unit, ok = units["\x01\x02\x03\x04"]; ok {
		data, err = unit.Data()
		c.Assert(err, check.IsNil)
		c.Check(data, check.HasLen, 0)

		priority, err = unit.Priority()
		c.Assert(err, check.IsNil)
		c.Check(priority, check.Equals, 0.0)
	}
}

// TestChainingExpiry tests that, if an attempt finishes but is no
// longer the active attempt, then its successor work units will not
// be created.
func (s *Suite) TestChainingExpiry(c *check.C) {
	var (
		one, two coordinate.WorkSpec
		err      error
		worker   coordinate.Worker
		unit     coordinate.WorkUnit
		attempts []coordinate.Attempt
	)

	one, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)

	two, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"disabled": true,
	})
	c.Assert(err, check.IsNil)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// Create and perform a work unit, with no output
	unit, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	attempt := attempts[0]

	// But wait!  We got preempted
	err = unit.ClearActiveAttempt()
	c.Assert(err, check.IsNil)
	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	// Now, let the original attempt finish, trying to generate
	// more outputs
	err = attempt.Finish(map[string]interface{}{
		"output": []string{"unit"},
	})
	c.Assert(err, check.IsNil)

	// Since attempt is no longer active, this shouldn't generate
	// new outputs
	units, err := two.WorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)
	c.Check(units, check.HasLen, 0)
}

// TestChainingDuplicate tests that work unit chaining still works
// even when the same output work unit is generated twice (it should
// get retried).
func (s *Suite) TestChainingDuplicate(c *check.C) {
	var (
		err      error
		one      coordinate.WorkSpec
		worker   coordinate.Worker
		attempts []coordinate.Attempt
	)
	one, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "one",
		"then":     "two",
		"priority": 1,
	})
	c.Assert(err, check.IsNil)

	_, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":     "two",
		"priority": 2,
	})
	c.Assert(err, check.IsNil)

	worker, err = s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	_, err = one.AddWorkUnit("a", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	_, err = one.AddWorkUnit("b", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "one")
	c.Assert(attempts[0].WorkUnit().Name(), check.Equals, "a")

	err = attempts[0].Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "two")
	c.Assert(attempts[0].WorkUnit().Name(), check.Equals, "z")

	err = attempts[0].Finish(map[string]interface{}{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "one")
	c.Assert(attempts[0].WorkUnit().Name(), check.Equals, "b")

	err = attempts[0].Finish(map[string]interface{}{
		"output": []string{"z"},
	})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "two")
	c.Assert(attempts[0].WorkUnit().Name(), check.Equals, "z")

	err = attempts[0].Finish(map[string]interface{}{})
	c.Assert(err, check.IsNil)

	attempts, err = worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 0)
}

// TestAttemptExpiration validates that an attempt's status will switch
// (on its own) to "expired" after a timeout.
func (s *Suite) TestAttemptExpiration(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "a",
	}
	sts.Do(s, c)

	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	attempt := attempts[0]

	status, err := attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.Pending)

	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	// There is a default expiration of 15 minutes (checked elsewhere)
	// So if we wait for, say, 20 minutes we should become expired
	s.Clock.Add(time.Duration(20) * time.Minute)
	status, err = attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.Expired)

	// The work unit should be "available" for all purposes
	meta, err := sts.WorkSpec.Meta(true)
	c.Assert(err, check.IsNil)
	c.Check(meta.AvailableCount, check.Equals, 1)
	c.Check(meta.PendingCount, check.Equals, 0)

	uStatus, err := sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.AvailableUnit)

	// If we request more attempts we should get back the expired
	// unit again
	attempts, err = sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)

	status, err = attempts[0].Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.Pending)
}

// TestRetryDelay verifies that the delay option on the Retry() call works.
func (s *Suite) TestRetryDelay(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "unit",
	}
	sts.Do(s, c)

	sts.CheckUnitStatus(c, coordinate.AvailableUnit)

	attempt := sts.RequestOneAttempt(c)
	err := attempt.Retry(nil, time.Duration(90)*time.Second)
	c.Assert(err, check.IsNil)

	s.Clock.Add(time.Duration(60) * time.Second)
	sts.CheckUnitStatus(c, coordinate.DelayedUnit)
	sts.RequestNoAttempts(c)

	s.Clock.Add(time.Duration(60) * time.Second)
	sts.CheckUnitStatus(c, coordinate.AvailableUnit)
	sts.CheckWorkUnitOrder(s, c, "unit")
}

// TestAttemptFractionalStart verifies that an attempt that starts at
// a non-integral time (as most of them are) can find itself.  This is
// a regression test for a specific issue in restclient.
func (s *Suite) TestAttemptFractionalStart(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "unit",
	}
	sts.Do(s, c)

	s.Clock.Add(500 * time.Millisecond)

	attempt := sts.RequestOneAttempt(c)
	// Since restclient uses the start time as one of its keys to
	// look up attempts, but the code uses two different ways to
	// format it, having a fractional second means that this call
	// will fail when restserver can't find the attempt.  Actually
	// the call above that adds half a second to the mock clock ruins
	// a lot of tests without the bug fix since *none* of them can
	// find their own attempts.
	err := attempt.Finish(nil)
	c.Assert(err, check.IsNil)
}

// TestAttemptGone verifies that, if a work unit is deleted, its
// attempts return ErrGone for things.
func (s *Suite) TestAttemptGone(c *check.C) {
	sts := SimpleTestSetup{
		WorkerName:   "worker",
		WorkSpecName: "spec",
		WorkUnitName: "unit",
	}
	sts.Do(s, c)

	attempt := sts.RequestOneAttempt(c)

	_, err := sts.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	c.Assert(err, check.IsNil)

	_, err = attempt.Status()
	if err == coordinate.ErrGone {
		// okay
	} else if nswu, ok := err.(coordinate.ErrNoSuchWorkUnit); ok {
		c.Check(nswu.Name, check.Equals, sts.WorkUnitName)
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		c.Check(nsws.Name, check.Equals, sts.WorkSpecName)
	} else {
		c.Errorf("deleted work spec produced error %+v", err)
	}
}
