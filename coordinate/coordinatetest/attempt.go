package coordinatetest

import (
	"github.com/dmaze/goordinate/coordinate"
	"gopkg.in/check.v1"
)

// TestAttemptLifetime validates a basic attempt lifetime.
func (s *Suite) TestAttemptLifetime(c *check.C) {
	var (
		err               error
		data              map[string]interface{}
		attempt, attempt2 coordinate.Attempt
		aStatus           coordinate.AttemptStatus
		spec              coordinate.WorkSpec
		unit              coordinate.WorkUnit
		worker            coordinate.Worker
		uStatus           coordinate.WorkUnitStatus
	)
	spec, worker = s.makeWorkSpecAndWorker(c)

	// Create a work unit
	unit, err = spec.AddWorkUnit("a", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)

	// The work unit should be "available"
	uStatus, err = unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.AvailableUnit)

	// The work unit data should be defined but empty
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// Get an attempt for it
	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	attempt = attempts[0]

	// The work unit should be "pending"
	uStatus, err = unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.PendingUnit)

	// The attempt should be "pending" too
	aStatus, err = attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(aStatus, check.Equals, coordinate.Pending)

	// The active attempt for the unit should match this
	attempt2, err = unit.ActiveAttempt()
	c.Assert(err, check.IsNil)
	c.Check(attempt2, AttemptMatches, attempt)

	// There should be one active attempt for the worker and it should
	// also match
	attempts, err = worker.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	// The work unit data should (still) be defined but empty
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// Now finish the attempt with some updated data
	err = attempt.Finish(map[string]interface{}{
		"outputs": []string{"yes"},
	})
	c.Assert(err, check.IsNil)

	// The unit should report "finished"
	uStatus, err = unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.FinishedUnit)

	// The attempt should report "finished"
	aStatus, err = attempt.Status()
	c.Assert(err, check.IsNil)
	c.Check(aStatus, check.Equals, coordinate.Finished)

	// The attempt should still be the active attempt for the unit
	attempt2, err = unit.ActiveAttempt()
	c.Assert(err, check.IsNil)
	c.Check(attempt2, AttemptMatches, attempt)

	// The attempt should not be in the active attempt list for the worker
	attempts, err = worker.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	// Both the unit and the worker should have one archived attempt
	attempts, err = unit.Attempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	attempts, err = worker.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	// This should have updated the visible work unit data too
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 1)
	c.Check(data["outputs"], check.DeepEquals, []string{"yes"})

	// For bonus points, force-clear the active attempt
	err = unit.ClearActiveAttempt()
	c.Assert(err, check.IsNil)

	// This should have pushed the unit back to available
	uStatus, err = unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(uStatus, check.Equals, coordinate.AvailableUnit)

	// This also should have reset the work unit data
	data, err = unit.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.HasLen, 0)

	// But, this should not have reset the historical attempts
	attempts, err = unit.Attempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}

	attempts, err = worker.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0], AttemptMatches, attempt)
	}
}
