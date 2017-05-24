// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// TestWorkerAncestry does basic tests on worker parents and children.
func (s *Suite) TestWorkerAncestry() {
	var (
		err                   error
		parent, child, worker coordinate.Worker
		kids                  []coordinate.Worker
	)

	sts := SimpleTestSetup{NamespaceName: "TestWorkerAncestry"}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// start in the middle
	parent, err = sts.Namespace.Worker("parent")
	if !s.NoError(err) {
		return
	}

	worker, err = parent.Parent()
	if s.NoError(err) {
		s.Nil(worker)
	}
	kids, err = parent.Children()
	if s.NoError(err) {
		s.Empty(kids)
	}

	// Create a child
	child, err = sts.Namespace.Worker("child")
	if !s.NoError(err) {
		return
	}
	err = child.SetParent(parent)
	s.NoError(err)

	// this should update the parent metadata
	worker, err = parent.Parent()
	if s.NoError(err) {
		s.Nil(worker)
	}
	kids, err = parent.Children()
	if s.NoError(err) && s.Len(kids, 1) {
		s.Equal("child", kids[0].Name())
	}

	// and also the child metadata
	worker, err = child.Parent()
	if s.NoError(err) && s.NotNil(worker) {
		s.Equal("parent", worker.Name())
	}
	kids, err = child.Children()
	if s.NoError(err) {
		s.Empty(kids)
	}
}

// TestWorkerAdoption hands a child worker to a new parent.
func (s *Suite) TestWorkerAdoption() {
	var (
		err                                 error
		child, oldParent, newParent, worker coordinate.Worker
		kids                                []coordinate.Worker
	)

	sts := SimpleTestSetup{NamespaceName: "TestWorkerAdoption"}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// Create the worker objects
	child, err = sts.Namespace.Worker("child")
	if !s.NoError(err) {
		return
	}
	oldParent, err = sts.Namespace.Worker("old")
	if !s.NoError(err) {
		return
	}
	newParent, err = sts.Namespace.Worker("new")
	if !s.NoError(err) {
		return
	}

	// Set up the original ancestry
	err = child.SetParent(oldParent)
	s.NoError(err)

	// Move it to the new parent
	err = child.SetParent(newParent)
	s.NoError(err)

	// Checks
	worker, err = child.Parent()
	if s.NoError(err) && s.NotNil(worker) {
		s.Equal("new", worker.Name())
	}
	kids, err = child.Children()
	if s.NoError(err) {
		s.Empty(kids)
	}

	worker, err = oldParent.Parent()
	if s.NoError(err) {
		s.Nil(worker)
	}
	kids, err = oldParent.Children()
	if s.NoError(err) {
		s.Empty(kids)
	}

	worker, err = newParent.Parent()
	if s.NoError(err) {
		s.Nil(worker)
	}
	kids, err = newParent.Children()
	if s.NoError(err) && s.Len(kids, 1) {
		s.Equal("child", kids[0].Name())
	}
}

// TestWorkerMetadata tests the various metadata fields.
func (s *Suite) TestWorkerMetadata() {
	now := s.Clock.Now()
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkerMetadata",
		WorkerName:    "worker",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// With no explicit setup, we should get these defaults
	active, err := sts.Worker.Active()
	if s.NoError(err) {
		s.True(active)
	}

	mode, err := sts.Worker.Mode()
	if s.NoError(err) {
		s.Equal("", mode)
	}

	s.DataEmpty(sts.Worker)

	lastUpdate, err := sts.Worker.LastUpdate()
	if s.NoError(err) {
		s.WithinDuration(now, lastUpdate, 1*time.Millisecond)
	}

	expiration, err := sts.Worker.Expiration()
	if s.NoError(err) {
		s.Equal(15*time.Minute, expiration.Sub(lastUpdate))
	}

	// Run an update
	s.Clock.Add(1 * time.Minute)
	now = s.Clock.Now()
	then := now.Add(15 * time.Minute)
	theData := map[string]interface{}{"key": "value"}
	theMode := "run"

	err = sts.Worker.Update(theData, now, then, theMode)
	s.NoError(err)

	active, err = sts.Worker.Active()
	if s.NoError(err) {
		s.True(active)
	}

	mode, err = sts.Worker.Mode()
	if s.NoError(err) {
		s.Equal(theMode, mode)
	}

	s.DataMatches(sts.Worker, theData)

	expiration, err = sts.Worker.Expiration()
	if s.NoError(err) {
		s.WithinDuration(then, expiration, 1*time.Millisecond)
	}

	lastUpdate, err = sts.Worker.LastUpdate()
	if s.NoError(err) {
		s.WithinDuration(now, lastUpdate, 1*time.Millisecond)
	}

	// Deactivate ourselves
	err = sts.Worker.Deactivate()
	s.NoError(err)

	active, err = sts.Worker.Active()
	if s.NoError(err) {
		s.False(active)
	}

	// Re-update, which should reactivate
	err = sts.Worker.Update(theData, now, then, theMode)
	s.NoError(err)

	active, err = sts.Worker.Active()
	if s.NoError(err) {
		s.True(active)
	}
}

// TestWorkerAttempts checks the association between attempts and workers.
func (s *Suite) TestWorkerAttempts() {
	sts := SimpleTestSetup{
		NamespaceName: "TestWorkerAttempts",
		WorkerName:    "child",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	// Manually set up a parent worker
	parent, err := sts.Namespace.Worker("parent")
	if !s.NoError(err) {
		return
	}
	child := sts.Worker
	err = child.SetParent(parent)
	s.NoError(err)

	// Create and perform one work unit
	_, err = sts.AddWorkUnit("one")
	s.NoError(err)
	attempt := sts.RequestOneAttempt(s)
	s.Equal("child", attempt.Worker().Name())
	s.Equal("spec", attempt.WorkUnit().WorkSpec().Name())
	s.Equal("one", attempt.WorkUnit().Name())
	err = attempt.Finish(nil)
	s.NoError(err)

	// Create and start (but don't finish) a second one
	_, err = sts.AddWorkUnit("two")
	s.NoError(err)
	attempt = sts.RequestOneAttempt(s)
	s.Equal("child", attempt.Worker().Name())
	s.Equal("spec", attempt.WorkUnit().WorkSpec().Name())
	s.Equal("two", attempt.WorkUnit().Name())

	// Validate the child worker's attempts
	attempts, err := child.ActiveAttempts()
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("child", attempts[0].Worker().Name())
		s.Equal("spec", attempts[0].WorkUnit().WorkSpec().Name())
		s.Equal("two", attempts[0].WorkUnit().Name())
	}

	attempts, err = child.AllAttempts()
	if s.NoError(err) && s.Len(attempts, 2) {
		s.Equal("child", attempts[0].Worker().Name())
		s.Equal("child", attempts[1].Worker().Name())

		s.Equal("spec", attempts[0].WorkUnit().WorkSpec().Name())
		s.Equal("spec", attempts[1].WorkUnit().WorkSpec().Name())
		if attempts[0].WorkUnit().Name() == "one" {
			s.Equal("two", attempts[1].WorkUnit().Name())
		} else {
			s.Equal("two", attempts[0].WorkUnit().Name())
			s.Equal("one", attempts[1].WorkUnit().Name())
		}
	}

	attempts, err = child.ChildAttempts()
	if s.NoError(err) {
		s.Empty(attempts)
	}

	// Check the parent's attempt lists
	attempts, err = parent.ActiveAttempts()
	if s.NoError(err) {
		s.Empty(attempts)
	}

	attempts, err = parent.AllAttempts()
	if s.NoError(err) {
		s.Empty(attempts)
	}

	attempts, err = parent.ChildAttempts()
	if s.NoError(err) && s.Len(attempts, 1) {
		s.Equal("child", attempts[0].Worker().Name())
		s.Equal("spec", attempts[0].WorkUnit().WorkSpec().Name())
		s.Equal("two", attempts[0].WorkUnit().Name())
	}
}

// TestDeactivateChild tests that deactivating a worker with a parent
// works successfully.  This is a regression test for a specific issue
// in the REST API.
func (s *Suite) TestDeactivateChild() {
	sts := SimpleTestSetup{NamespaceName: "TestDeactivateChild"}
	sts.SetUp(s)
	defer sts.TearDown(s)

	parent, err := sts.Namespace.Worker("parent")
	if !s.NoError(err) {
		return
	}
	child, err := sts.Namespace.Worker("child")
	if !s.NoError(err) {
		return
	}
	err = child.SetParent(parent)
	s.NoError(err)
	err = child.Deactivate()
	s.NoError(err)
}
