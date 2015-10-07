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
	c.Check(mode, check.Equals, 0)

	data, err := worker.Data()
	c.Assert(err, check.IsNil)
	c.Check(data, check.IsNil)

	lastUpdate, err := worker.LastUpdate()
	c.Assert(err, check.IsNil)
	// Should default to "now"

	expiration, err := worker.Expiration()
	c.Assert(err, check.IsNil)
	// c.Check(expiration.Sub(lastUpdate), check.Equals, time.Duration(15)*time.Minute)

	// Run an update
	whenIsNow := time.Now()
	whenIsThen := whenIsNow.Add(time.Duration(15) * time.Minute)
	// ("We passed 'then', sir."  "When?"  "Just now.")
	theData := map[string]interface{}{"key": "value"}
	theMode := 17

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
