package coordinatetest

import (
	"github.com/dmaze/goordinate/coordinate"
	"gopkg.in/check.v1"
	"time"
)

// TestWorkerAncestry does basic tests on worker parents and children.
func (s *Suite) TestWorkerAncestry(c *check.C) {
	var (
		err                   error
		parent, child, worker coordinate.Worker
		kids                  []coordinate.Worker
	)

	// start in the middle
	parent, err = s.Namespace.Worker("parent")
	c.Assert(err, check.IsNil)

	worker, err = parent.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.IsNil)
	kids, err = parent.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 0)

	// Create a child
	child, err = s.Namespace.Worker("child")
	c.Assert(err, check.IsNil)
	err = child.SetParent(parent)
	c.Assert(err, check.IsNil)

	// this should update the parent metadata
	worker, err = parent.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.IsNil)
	kids, err = parent.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 1)
	if len(kids) > 0 {
		c.Check(kids[0].Name(), check.Equals, "child")
	}

	// and also the child metadata
	worker, err = child.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.NotNil)
	if worker != nil {
		c.Check(worker.Name(), check.Equals, "parent")
	}
	kids, err = child.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 0)
}

// TestWorkerAdoption hands a child worker to a new parent.
func (s *Suite) TestWorkerAdoption(c *check.C) {
	var (
		err                                 error
		child, oldParent, newParent, worker coordinate.Worker
		kids                                []coordinate.Worker
	)

	// Create the worker objects
	child, err = s.Namespace.Worker("child")
	c.Assert(err, check.IsNil)
	oldParent, err = s.Namespace.Worker("old")
	c.Assert(err, check.IsNil)
	newParent, err = s.Namespace.Worker("new")
	c.Assert(err, check.IsNil)

	// Set up the original ancestry
	err = child.SetParent(oldParent)
	c.Assert(err, check.IsNil)

	// Move it to the new parent
	err = child.SetParent(newParent)
	c.Assert(err, check.IsNil)

	// Checks
	worker, err = child.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.NotNil)
	if worker != nil {
		c.Check(worker.Name(), check.Equals, "new")
	}
	kids, err = child.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 0)

	worker, err = oldParent.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.IsNil)
	kids, err = oldParent.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 0)

	worker, err = newParent.Parent()
	c.Assert(err, check.IsNil)
	c.Check(worker, check.IsNil)
	kids, err = newParent.Children()
	c.Assert(err, check.IsNil)
	c.Check(kids, check.HasLen, 1)
	if len(kids) > 0 {
		c.Check(kids[0].Name(), check.Equals, "child")
	}
}

// TestWorkerMetadata tests the various metadata fields.
func (s *Suite) TestWorkerMetadata(c *check.C) {
	worker, err := s.Namespace.Worker("worker")
	c.Assert(err, check.IsNil)

	// With no explicit setup, we should get these defaults
	active, err := worker.Active()
	c.Assert(err, check.IsNil)
	c.Check(active, check.Equals, true)

	mode, err := worker.Mode()
	c.Assert(err, check.IsNil)
	c.Check(mode, check.Equals, "")

	data, err := worker.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.IsNil)

	lastUpdate, err := worker.LastUpdate()
	c.Assert(err, check.IsNil)
	// Should default to "now"

	expiration, err := worker.Expiration()
	c.Assert(err, check.IsNil)
	c.Check(expiration.Sub(lastUpdate), check.Equals, time.Duration(15)*time.Minute)

	// Run an update
	whenIsNow := time.Now()
	whenIsThen := whenIsNow.Add(time.Duration(15) * time.Minute)
	// ("We passed 'then', sir."  "When?"  "Just now.")
	theData := map[string]interface{}{"key": "value"}
	theMode := "run"

	err = worker.Update(theData, whenIsNow, whenIsThen, theMode)
	c.Assert(err, check.IsNil)

	active, err = worker.Active()
	c.Assert(err, check.IsNil)
	c.Check(active, check.Equals, true)

	mode, err = worker.Mode()
	c.Assert(err, check.IsNil)
	c.Check(mode, check.Equals, theMode)

	data, err = worker.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, theData)

	expiration, err = worker.Expiration()
	c.Assert(err, check.IsNil)
	c.Check(expiration, SameTime, whenIsThen)

	lastUpdate, err = worker.LastUpdate()
	c.Assert(err, check.IsNil)
	c.Check(lastUpdate, SameTime, whenIsNow)
	c.Check(expiration.Sub(lastUpdate), check.Equals, time.Duration(15)*time.Minute)

	// Deactivate ourselves
	err = worker.Deactivate()
	c.Assert(err, check.IsNil)

	active, err = worker.Active()
	c.Assert(err, check.IsNil)
	c.Check(active, check.Equals, false)

	// Re-update, which should reactivate
	err = worker.Update(theData, whenIsNow, whenIsThen, theMode)
	c.Assert(err, check.IsNil)

	active, err = worker.Active()
	c.Assert(err, check.IsNil)
	c.Check(active, check.Equals, true)
}

// TestWorkerAttempts checks the association between attempts and workers.
func (s *Suite) TestWorkerAttempts(c *check.C) {
	// Manually set up two workers and a work spec
	parent, err := s.Namespace.Worker("parent")
	c.Assert(err, check.IsNil)
	child, err := s.Namespace.Worker("child")
	c.Assert(err, check.IsNil)
	err = child.SetParent(parent)
	c.Assert(err, check.IsNil)
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)

	// Create and perform one work unit
	_, err = spec.AddWorkUnit("one", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	attempts, err := child.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Check(attempts[0].Worker().Name(), check.Equals, "child")
	c.Check(attempts[0].WorkUnit().Name(), check.Equals, "one")
	c.Check(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
	err = attempts[0].Finish(nil)
	c.Assert(err, check.IsNil)

	// Create and start (but don't finish) a second one
	_, err = spec.AddWorkUnit("two", map[string]interface{}{}, 0.0)
	c.Assert(err, check.IsNil)
	attempts, err = child.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	c.Check(attempts[0].Worker().Name(), check.Equals, "child")
	c.Check(attempts[0].WorkUnit().Name(), check.Equals, "two")
	c.Check(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")

	// Validate the child worker's attempts
	attempts, err = child.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].Worker().Name(), check.Equals, "child")
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "two")
		c.Check(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
	}

	attempts, err = child.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 2)
	if len(attempts) >= 2 {
		c.Check(attempts[0].Worker().Name(), check.Equals, "child")
		c.Check(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
		c.Check(attempts[1].Worker().Name(), check.Equals, "child")
		c.Check(attempts[1].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
		if attempts[0].WorkUnit().Name() == "one" {
			c.Check(attempts[0].WorkUnit().Name(), check.Equals, "one")
			c.Check(attempts[1].WorkUnit().Name(), check.Equals, "two")
		} else {
			c.Check(attempts[0].WorkUnit().Name(), check.Equals, "two")
			c.Check(attempts[1].WorkUnit().Name(), check.Equals, "one")
		}
	}

	attempts, err = child.ChildAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	// Check the parent's attempt lists
	attempts, err = parent.ActiveAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	attempts, err = parent.AllAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 0)

	attempts, err = parent.ChildAttempts()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.HasLen, 1)
	if len(attempts) > 0 {
		c.Check(attempts[0].Worker().Name(), check.Equals, "child")
		c.Check(attempts[0].WorkUnit().Name(), check.Equals, "two")
		c.Check(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
	}
}
