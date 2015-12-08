// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

// This file has miscellaneous work attempt tests.

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/jobserver"
	"gopkg.in/check.v1"
)

// TestUpdateAvailable tries to transition a work unit from "available"
// to "failed" state.
func (s *PythonSuite) TestUpdateAvailable(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "unit", map[string]interface{}{})

	ok, msg, err := s.JobServer.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Failed,
		"worker_id": "child",
	})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(ok, check.Equals, true)

	s.checkWorkUnitStatus(c, workSpecName, "unit", jobserver.Failed)
}

// TestUpdateAvailableFull verifies a specific race condition that can
// happen in the Python worker.  Say the parent asks coordinated for a
// list of its childrens' pending work units.  Even though it tries to
// kill them off 15 seconds before they expire, on a bad day
// coordinated will still manage to hit the expiry first, so the work
// unit transitions back to "available".
//
// This test validates this specific sequence of things.
func (s *PythonSuite) TestUpdateAvailableFull(c *check.C) {
	empty := map[string]interface{}{}
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "unit", empty)

	ok, msg, err := s.JobServer.WorkerHeartbeat("parent", "RUN", 900, empty, "")
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)

	ok, msg, err = s.JobServer.WorkerHeartbeat("child", "RUN", 900, empty, "parent")
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)

	work, msg, err := s.JobServer.GetWork("child", map[string]interface{}{"available_gb": 1})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Assert(work, check.NotNil)
	tuple, ok := work.(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Assert(tuple.Items, check.HasLen, 3)
	c.Assert(tuple.Items[0], check.Equals, workSpecName)
	c.Assert(tuple.Items[1], check.DeepEquals, []byte("unit"))
	s.checkWorkUnitStatus(c, workSpecName, "unit", jobserver.Pending)

	// Force the work unit back to "available" to simulate expiry
	ok, msg, err = s.JobServer.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Available,
		"worker_id": "child",
	})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(ok, check.Equals, true)
	s.checkWorkUnitStatus(c, workSpecName, "unit", jobserver.Available)

	// Now kill it from the parent
	ok, msg, err = s.JobServer.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Failed,
		"worker_id": "parent",
	})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(ok, check.Equals, true)
	s.checkWorkUnitStatus(c, workSpecName, "unit", jobserver.Failed)
}
