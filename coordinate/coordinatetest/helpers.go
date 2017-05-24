// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// ---------------------------------------------------------------------------
// Support functions for common tests

// AttemptMatches checks that two attempts are attempting the same thing.
func (s *Suite) AttemptMatches(expected, actual coordinate.Attempt) bool {
	return (s.Equal(expected.Worker().Name(), actual.Worker().Name()) &&
		s.Equal(expected.WorkUnit().Name(), actual.WorkUnit().Name()) &&
		s.Equal(expected.WorkUnit().WorkSpec().Name(), actual.WorkUnit().WorkSpec().Name()))
}

// AttemptStatus checks that an attempt has an expected status.
func (s *Suite) AttemptStatus(
	expected coordinate.AttemptStatus,
	attempt coordinate.Attempt,
) bool {
	actual, err := attempt.Status()
	return s.NoError(err) && s.Equal(expected, actual)
}

// HasData describes attempts, workers, and work units that can return
// their own data (probably).
type HasData interface {
	Data() (map[string]interface{}, error)
}

// DataEmpty checks that an object's data is empty.
func (s *Suite) DataEmpty(obj HasData) bool {
	data, err := obj.Data()
	return s.NoError(err) && s.Empty(data)
}

// DataMatches checks that an object's data matches an expected value.
func (s *Suite) DataMatches(obj HasData, expected map[string]interface{}) bool {
	data, err := obj.Data()
	if !s.NoError(err) {
		return false
	}
	ok := true
	// assert.Equal is reflect.DeepEqual.
	// assert.EqualValues does a type conversion first if needed.
	// What we actually need is a recursive match, which
	// doesn't exist; but actually we just need this shallower
	for key, value := range expected {
		if s.Contains(data, key, "missing data[%q]", key) {
			if !s.EqualValues(value, data[key], "data[%q]", key) {
				ok = false
			}
		} else {
			ok = false
		}
		for key := range data {
			if !s.Contains(expected, key, "extra data[%q]", key) {
				ok = false
			}
		}
	}
	return ok
}

// UnitHasPriority validates the priority of a work unit.
func (s *Suite) UnitHasPriority(unit coordinate.WorkUnit, priority float64) bool {
	actual, err := unit.Priority()
	return s.NoError(err) && s.Equal(priority, actual)
}

// ---------------------------------------------------------------------------
// SimpleTestSetup

// SimpleTestSetup defines parameters for common tests, that use
// a small number of workers, work specs, etc.
type SimpleTestSetup struct {
	// NamespaceName, if non-empty, requests a Namespace be
	// created with this name.  It is frequently the name of the
	// test.
	NamespaceName string

	// Namespace is the namespace to use.  If this is nil then
	// a new namespace will be created from NamespaceName, even
	// if that is empty.
	Namespace coordinate.Namespace

	// WorkerName, if non-empty, requests a Worker be created
	// with this name.
	WorkerName string

	// Worker is set on output.
	Worker coordinate.Worker

	// WorkSpecName, if non-empty, sets the name of the work spec.
	WorkSpecName string

	// WorkSpecData, if non-empty, provides additional data for
	// the work spec.  If WorkSpecName is also set it will overwrite
	// this name.
	WorkSpecData map[string]interface{}

	// WorkSpec is set on output.
	WorkSpec coordinate.WorkSpec

	// WorkUnitName, if non-empty, gives the name of a work unit.
	WorkUnitName string

	// WorkUnitData, if non-empty, provides the corresponding
	// work unit data.
	WorkUnitData map[string]interface{}

	// WorkUnitMeta gives additional options for the work unit.
	WorkUnitMeta coordinate.WorkUnitMeta

	// WorkUnit is set on output.
	WorkUnit coordinate.WorkUnit
}

// SetUp populates the output fields of the test setup, or fails using
// t.FailNow().
func (sts *SimpleTestSetup) SetUp(s *Suite) {
	var err error

	// Create the namespace
	if sts.Namespace == nil {
		sts.Namespace, err = s.Coordinate.Namespace(sts.NamespaceName)
		if !s.NoError(err) {
			s.FailNow("could not create namespace")
		}
	}

	// Create the work spec
	if sts.WorkSpecName != "" || sts.WorkSpecData != nil {
		if sts.WorkSpecData == nil {
			sts.WorkSpecData = map[string]interface{}{}
		}
		if _, present := sts.WorkSpecData["min_gb"]; !present {
			sts.WorkSpecData["min_gb"] = 1
		}
		if sts.WorkSpecName != "" {
			sts.WorkSpecData["name"] = sts.WorkSpecName
		}
		sts.WorkSpec, err = sts.Namespace.SetWorkSpec(sts.WorkSpecData)
		if !(s.NoError(err) &&
			s.Equal(sts.WorkSpecData["name"], sts.WorkSpec.Name())) {
			s.FailNow("could not create work spec")
		}
	}

	// Create the work unit
	if sts.WorkSpec != nil && sts.WorkUnitName != "" {
		if sts.WorkUnitData == nil {
			sts.WorkUnitData = map[string]interface{}{}
		}
		sts.WorkUnit, err = sts.WorkSpec.AddWorkUnit(sts.WorkUnitName, sts.WorkUnitData, sts.WorkUnitMeta)
		if !(s.NoError(err) &&
			s.Equal(sts.WorkUnitName, sts.WorkUnit.Name()) &&
			s.Equal(sts.WorkSpec.Name(), sts.WorkUnit.WorkSpec().Name())) {
			s.FailNow("could not create work unit")
		}
	}

	// Create the worker
	if sts.WorkerName != "" {
		sts.Worker, err = sts.Namespace.Worker(sts.WorkerName)
		if !(s.NoError(err) &&
			s.Equal(sts.WorkerName, sts.Worker.Name())) {
			s.FailNow("could not create worker")
		}
	}
}

// TearDown destroys the namespace and all other resources created in
// SetUp.
func (sts *SimpleTestSetup) TearDown(s *Suite) {
	if sts.Namespace != nil {
		err := sts.Namespace.Destroy()
		s.NoError(err)
	}
}

// AddWorkUnit adds a single work unit to the work spec with default
// (empty) data and metadata.
func (sts *SimpleTestSetup) AddWorkUnit(name string) (coordinate.WorkUnit, error) {
	return sts.WorkSpec.AddWorkUnit(name, map[string]interface{}{}, coordinate.WorkUnitMeta{})
}

// MakeWorkUnits creates a handful of work units within a work spec.
// These have keys "available", "pending", "finished", "failed",
// "expired", and "retryable", and wind up in the corresponding
// states.
func (sts *SimpleTestSetup) MakeWorkUnits() (map[string]coordinate.WorkUnit, error) {
	result := map[string]coordinate.WorkUnit{
		"available": nil,
		"pending":   nil,
		"finished":  nil,
		"failed":    nil,
		"expired":   nil,
		"retryable": nil,
		"delayed":   nil,
	}
	for key := range result {
		unit, err := sts.AddWorkUnit(key)
		if err != nil {
			return nil, err
		}
		result[key] = unit

		// Run the workflow
		if key == "available" {
			continue
		}
		attempt, err := sts.Worker.MakeAttempt(unit, time.Duration(0))
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
			err = attempt.Retry(nil, time.Duration(0))
		case "delayed":
			err = attempt.Retry(nil, time.Duration(24)*time.Hour)
		}
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// CheckUnitStatus checks that the test's work unit's status matches
// an expected value.
func (sts *SimpleTestSetup) CheckUnitStatus(s *Suite, status coordinate.WorkUnitStatus) {
	actual, err := sts.WorkUnit.Status()
	if s.NoError(err) {
		s.Equal(status, actual)
	}
}

// RequestOneAttempt gets a single attempt from the test's worker, or
// fails the test immediately if not exactly one attempt was returned.
func (sts *SimpleTestSetup) RequestOneAttempt(s *Suite) coordinate.Attempt {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if !(s.NoError(err) && s.Len(attempts, 1)) {
		s.FailNow("did not get an attempt")
	}
	return attempts[0]
}

// RequestNoAttempts requests attempts and asserts that nothing was
// returned.  It does not fail the test if something does come back.
func (sts *SimpleTestSetup) RequestNoAttempts(s *Suite) {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if s.NoError(err) {
		s.Empty(attempts)
	}
}

// CheckWorkUnitOrder requests every possible attempt, one at a time,
// virtually pausing 5 seconds between each.  It checks that the
// resulting ordering matches the provided order of work unit names.
func (sts *SimpleTestSetup) CheckWorkUnitOrder(s *Suite, unitNames ...string) {
	var processedUnits []string
	for {
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if !s.NoError(err) {
			s.FailNow("could not request attempt")
		}
		if len(attempts) == 0 {
			break
		}
		if !s.Len(attempts, 1) {
			s.FailNow("got too many attempts")
		}
		attempt := attempts[0]
		s.Equal(sts.WorkSpec.Name(), attempt.WorkUnit().WorkSpec().Name())
		processedUnits = append(processedUnits, attempt.WorkUnit().Name())
		err = attempt.Finish(nil)
		if !s.NoError(err) {
			s.FailNow("could not finish attempt")
		}
	}

	s.Equal(unitNames, processedUnits)
}
