package coordinatetest

import (
	"github.com/dmaze/goordinate/coordinate"
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
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	c.Assert(err, check.IsNil)

	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	_, err = makeWorkUnits(spec, worker)
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
