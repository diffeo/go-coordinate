// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestWorkerAncestry does basic tests on worker parents and children.
func TestWorkerAncestry(t *testing.T) {
	var (
		err                   error
		parent, child, worker coordinate.Worker
		kids                  []coordinate.Worker
	)

	sts := SimpleTestSetup{NamespaceName: "TestWorkerAncestry"}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// start in the middle
	parent, err = sts.Namespace.Worker("parent")
	if !assert.NoError(t, err) {
		return
	}

	worker, err = parent.Parent()
	if assert.NoError(t, err) {
		assert.Nil(t, worker)
	}
	kids, err = parent.Children()
	if assert.NoError(t, err) {
		assert.Empty(t, kids)
	}

	// Create a child
	child, err = sts.Namespace.Worker("child")
	if !assert.NoError(t, err) {
		return
	}
	err = child.SetParent(parent)
	assert.NoError(t, err)

	// this should update the parent metadata
	worker, err = parent.Parent()
	if assert.NoError(t, err) {
		assert.Nil(t, worker)
	}
	kids, err = parent.Children()
	if assert.NoError(t, err) && assert.Len(t, kids, 1) {
		assert.Equal(t, "child", kids[0].Name())
	}

	// and also the child metadata
	worker, err = child.Parent()
	if assert.NoError(t, err) && assert.NotNil(t, worker) {
		assert.Equal(t, "parent", worker.Name())
	}
	kids, err = child.Children()
	if assert.NoError(t, err) {
		assert.Empty(t, kids)
	}
}

// TestWorkerAdoption hands a child worker to a new parent.
func TestWorkerAdoption(t *testing.T) {
	var (
		err                                 error
		child, oldParent, newParent, worker coordinate.Worker
		kids                                []coordinate.Worker
	)

	sts := SimpleTestSetup{NamespaceName: "TestWorkerAdoption"}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// Create the worker objects
	child, err = sts.Namespace.Worker("child")
	if !assert.NoError(t, err) {
		return
	}
	oldParent, err = sts.Namespace.Worker("old")
	if !assert.NoError(t, err) {
		return
	}
	newParent, err = sts.Namespace.Worker("new")
	if !assert.NoError(t, err) {
		return
	}

	// Set up the original ancestry
	err = child.SetParent(oldParent)
	assert.NoError(t, err)

	// Move it to the new parent
	err = child.SetParent(newParent)
	assert.NoError(t, err)

	// Checks
	worker, err = child.Parent()
	if assert.NoError(t, err) && assert.NotNil(t, worker) {
		assert.Equal(t, "new", worker.Name())
	}
	kids, err = child.Children()
	if assert.NoError(t, err) {
		assert.Empty(t, kids)
	}

	worker, err = oldParent.Parent()
	if assert.NoError(t, err) {
		assert.Nil(t, worker)
	}
	kids, err = oldParent.Children()
	if assert.NoError(t, err) {
		assert.Empty(t, kids)
	}

	worker, err = newParent.Parent()
	if assert.NoError(t, err) {
		assert.Nil(t, worker)
	}
	kids, err = newParent.Children()
	if assert.NoError(t, err) && assert.Len(t, kids, 1) {
		assert.Equal(t, "child", kids[0].Name())
	}
}

// TestWorkerMetadata tests the various metadata fields.
func TestWorkerMetadata(t *testing.T) {
	now := Clock.Now()
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkerMetadata",
		WorkerName:    "worker",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// With no explicit setup, we should get these defaults
	active, err := sts.Worker.Active()
	if assert.NoError(t, err) {
		assert.True(t, active)
	}

	mode, err := sts.Worker.Mode()
	if assert.NoError(t, err) {
		assert.Equal(t, "", mode)
	}

	DataEmpty(t, sts.Worker)

	lastUpdate, err := sts.Worker.LastUpdate()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, now, lastUpdate, 1*time.Millisecond)
	}

	expiration, err := sts.Worker.Expiration()
	if assert.NoError(t, err) {
		assert.Equal(t, 15*time.Minute, expiration.Sub(lastUpdate))
	}

	// Run an update
	Clock.Add(1 * time.Minute)
	now = Clock.Now()
	then := now.Add(15 * time.Minute)
	theData := map[string]interface{}{"key": "value"}
	theMode := "run"

	err = sts.Worker.Update(theData, now, then, theMode)
	assert.NoError(t, err)

	active, err = sts.Worker.Active()
	if assert.NoError(t, err) {
		assert.True(t, active)
	}

	mode, err = sts.Worker.Mode()
	if assert.NoError(t, err) {
		assert.Equal(t, theMode, mode)
	}

	DataMatches(t, sts.Worker, theData)

	expiration, err = sts.Worker.Expiration()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, then, expiration, 1*time.Millisecond)
	}

	lastUpdate, err = sts.Worker.LastUpdate()
	if assert.NoError(t, err) {
		assert.WithinDuration(t, now, lastUpdate, 1*time.Millisecond)
	}

	// Deactivate ourselves
	err = sts.Worker.Deactivate()
	assert.NoError(t, err)

	active, err = sts.Worker.Active()
	if assert.NoError(t, err) {
		assert.False(t, active)
	}

	// Re-update, which should reactivate
	err = sts.Worker.Update(theData, now, then, theMode)
	assert.NoError(t, err)

	active, err = sts.Worker.Active()
	if assert.NoError(t, err) {
		assert.True(t, active)
	}
}

// TestWorkerAttempts checks the association between attempts and workers.
func TestWorkerAttempts(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkerAttempts",
		WorkerName:    "child",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	// Manually set up a parent worker
	parent, err := sts.Namespace.Worker("parent")
	if !assert.NoError(t, err) {
		return
	}
	child := sts.Worker
	err = child.SetParent(parent)
	assert.NoError(t, err)

	// Create and perform one work unit
	_, err = sts.AddWorkUnit("one")
	assert.NoError(t, err)
	attempt := sts.RequestOneAttempt(t)
	assert.Equal(t, "child", attempt.Worker().Name())
	assert.Equal(t, "spec", attempt.WorkUnit().WorkSpec().Name())
	assert.Equal(t, "one", attempt.WorkUnit().Name())
	err = attempt.Finish(nil)
	assert.NoError(t, err)

	// Create and start (but don't finish) a second one
	_, err = sts.AddWorkUnit("two")
	assert.NoError(t, err)
	attempt = sts.RequestOneAttempt(t)
	assert.Equal(t, "child", attempt.Worker().Name())
	assert.Equal(t, "spec", attempt.WorkUnit().WorkSpec().Name())
	assert.Equal(t, "two", attempt.WorkUnit().Name())

	// Validate the child worker's attempts
	attempts, err := child.ActiveAttempts()
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "child", attempts[0].Worker().Name())
		assert.Equal(t, "spec", attempts[0].WorkUnit().WorkSpec().Name())
		assert.Equal(t, "two", attempts[0].WorkUnit().Name())
	}

	attempts, err = child.AllAttempts()
	if assert.NoError(t, err) && assert.Len(t, attempts, 2) {
		assert.Equal(t, "child", attempts[0].Worker().Name())
		assert.Equal(t, "child", attempts[1].Worker().Name())

		assert.Equal(t, "spec", attempts[0].WorkUnit().WorkSpec().Name())
		assert.Equal(t, "spec", attempts[1].WorkUnit().WorkSpec().Name())
		if attempts[0].WorkUnit().Name() == "one" {
			assert.Equal(t, "two", attempts[1].WorkUnit().Name())
		} else {
			assert.Equal(t, "two", attempts[0].WorkUnit().Name())
			assert.Equal(t, "one", attempts[1].WorkUnit().Name())
		}
	}

	attempts, err = child.ChildAttempts()
	if assert.NoError(t, err) {
		assert.Empty(t, attempts)
	}

	// Check the parent's attempt lists
	attempts, err = parent.ActiveAttempts()
	if assert.NoError(t, err) {
		assert.Empty(t, attempts)
	}

	attempts, err = parent.AllAttempts()
	if assert.NoError(t, err) {
		assert.Empty(t, attempts)
	}

	attempts, err = parent.ChildAttempts()
	if assert.NoError(t, err) && assert.Len(t, attempts, 1) {
		assert.Equal(t, "child", attempts[0].Worker().Name())
		assert.Equal(t, "spec", attempts[0].WorkUnit().WorkSpec().Name())
		assert.Equal(t, "two", attempts[0].WorkUnit().Name())
	}
}

// TestDeactivateChild tests that deactivating a worker with a parent
// works successfully.  This is a regression test for a specific issue
// in the REST API.
func TestDeactivateChild(t *testing.T) {
	sts := SimpleTestSetup{NamespaceName: "TestDeactivateChild"}
	sts.SetUp(t)
	defer sts.TearDown(t)

	parent, err := sts.Namespace.Worker("parent")
	if !assert.NoError(t, err) {
		return
	}
	child, err := sts.Namespace.Worker("child")
	if !assert.NoError(t, err) {
		return
	}
	err = child.SetParent(parent)
	assert.NoError(t, err)
	err = child.Deactivate()
	assert.NoError(t, err)
}
