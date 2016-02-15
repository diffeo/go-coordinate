// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

// This file has miscellaneous work spec tests.

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSpecByteification(t *testing.T) {
	j := setUpTest(t, "TestSpecByteification")
	defer tearDownTest(t, j)
	workSpecName := setWorkSpec(t, j, WorkSpecData)

	data, err := j.GetWorkSpec(workSpecName)
	if assert.NoError(t, err) {
		assert.Equal(t, map[string]interface{}{
			"name":         "test_job_client",
			"min_gb":       1,
			"module":       []byte("coordinate.tests.test_job_client"),
			"run_function": []byte("run_function"),
		}, data)
	}
}
