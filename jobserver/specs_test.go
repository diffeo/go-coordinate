// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

// This file has miscellaneous work spec tests.

import (
	"gopkg.in/check.v1"
)

// (Glomming on to the python_test check suite is a little weird but saves
// effort.)

func (s *PythonSuite) TestSpecByteification(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)

	data, err := s.JobServer.GetWorkSpec(workSpecName)
	c.Assert(err, check.IsNil)
	c.Check(data, check.DeepEquals, map[string]interface{}{
		"name":         "test_job_client",
		"min_gb":       1,
		"module":       []byte("coordinate.tests.test_job_client"),
		"run_function": []byte("run_function"),
	})
}
