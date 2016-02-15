// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

// This file has miscellaneous work attempt tests.

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/jobserver"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestUpdateAvailable tries to transition a work unit from "available"
// to "failed" state.
func TestUpdateAvailable(t *testing.T) {
	j := setUpTest(t, "TestUpdateAvailable")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "unit", map[string]interface{}{})

	ok, msg, err := j.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Failed,
		"worker_id": "child",
	})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Failed)
}

// TestUpdateAvailableFull verifies a specific race condition that can
// happen in the Python worker.  Say the parent asks coordinated for a
// list of its childrens' pending work units.  Even though it tries to
// kill them off 15 seconds before they expire, on a bad day
// coordinated will still manage to hit the expiry first, so the work
// unit transitions back to "available".
//
// This test validates this specific sequence of things.
func TestUpdateAvailableFull(t *testing.T) {
	j := setUpTest(t, "TestUpdateAvailableFull")
	defer tearDownTest(t, j)

	empty := map[string]interface{}{}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "unit", empty)

	ok, msg, err := j.WorkerHeartbeat("parent", "RUN", 900, empty, "")
	if assert.NoError(t, err) {
		assert.True(t, ok)
	}

	ok, msg, err = j.WorkerHeartbeat("child", "RUN", 900, empty, "parent")
	if assert.NoError(t, err) {
		assert.True(t, ok)
	}

	work, msg, err := j.GetWork("child", map[string]interface{}{"available_gb": 1})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)
		if assert.NotNil(t, work) && assert.IsType(t, cborrpc.PythonTuple{}, work) {
			tuple := work.(cborrpc.PythonTuple)
			if assert.Len(t, tuple.Items, 3) {
				assert.Equal(t, workSpecName, tuple.Items[0])
				assert.Equal(t, []byte("unit"), tuple.Items[1])
			}
		}
	}
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Pending)

	// Force the work unit back to "available" to simulate expiry
	ok, msg, err = j.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Available,
		"worker_id": "child",
	})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Available)

	// Now kill it from the parent
	ok, msg, err = j.UpdateWorkUnit(workSpecName, "unit", map[string]interface{}{
		"status":    jobserver.Failed,
		"worker_id": "parent",
	})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Failed)
}

// TestDelayedUnit creates a work unit to run in the future.
func TestDelayedUnit(t *testing.T) {
	j := setUpTest(t, "TestDelayedUnit")
	defer tearDownTest(t, j)

	empty := map[string]interface{}{}
	workSpecName := setWorkSpec(t, j, WorkSpecData)

	ok, msg, err := j.AddWorkUnits(workSpecName, []interface{}{
		cborrpc.PythonTuple{Items: []interface{}{
			"unit",
			empty,
			map[string]interface{}{"delay": 90},
		}},
	})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// Even though it is delayed, we should report it as available
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Available)

	// Get-work should return nothing
	doNoWork(t, j)

	// If we wait 60 seconds (out of 90) we should still get nothing
	Clock.Add(60 * time.Second)
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Available)
	doNoWork(t, j)

	// If we wait another 60 seconds we should be able to do it
	Clock.Add(60 * time.Second)
	checkWorkUnitStatus(t, j, workSpecName, "unit", jobserver.Available)
	doOneWork(t, j, workSpecName, "unit")
}
