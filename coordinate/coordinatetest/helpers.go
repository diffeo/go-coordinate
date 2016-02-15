// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Support functions for common tests

// AttemptMatches checks that two attempts are attempting the same thing.
func AttemptMatches(t *testing.T, expected, actual coordinate.Attempt) bool {
	return (assert.Equal(t, expected.Worker().Name(), actual.Worker().Name()) &&
		assert.Equal(t, expected.WorkUnit().Name(), actual.WorkUnit().Name()) &&
		assert.Equal(t, expected.WorkUnit().WorkSpec().Name(), actual.WorkUnit().WorkSpec().Name()))
}

// AttemptStatus checks that an attempt has an expected status.
func AttemptStatus(t *testing.T, expected coordinate.AttemptStatus, attempt coordinate.Attempt) {
	actual, err := attempt.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, expected, actual)
	}
}

// HasData describes attempts, workers, and work units that can return
// their own data (probably).
type HasData interface {
	Data() (map[string]interface{}, error)
}

// DataEmpty checks that an object's data is empty.
func DataEmpty(t *testing.T, obj HasData) {
	data, err := obj.Data()
	if assert.NoError(t, err) {
		assert.Empty(t, data)
	}
}

// DataMatches checks that an object's data matches an expected value.
func DataMatches(t *testing.T, obj HasData, expected map[string]interface{}) {
	data, err := obj.Data()
	if assert.NoError(t, err) {
		// assert.Equal is reflect.DeepEqual.
		// assert.EqualValues does a type conversion first if needed.
		// What we actually need is a recursive match, which
		// doesn't exist; but actually we just need this shallower
		for key, value := range expected {
			if assert.Contains(t, data, key,
				"missing data[%q]", key) {
				assert.EqualValues(t, value, data[key],
					"data[%q]", key)
			}
		}
		for key := range data {
			assert.Contains(t, expected, key,
				"extra data[%q]", key)
		}
	}
}

// UnitHasPriority validates the priority of a work unit.
func UnitHasPriority(t *testing.T, unit coordinate.WorkUnit, priority float64) {
	actual, err := unit.Priority()
	if assert.NoError(t, err) {
		assert.Equal(t, priority, actual)
	}
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
func (sts *SimpleTestSetup) SetUp(t *testing.T) {
	var err error

	// Create the namespace
	if sts.Namespace == nil {
		sts.Namespace, err = Coordinate.Namespace(sts.NamespaceName)
		if !assert.NoError(t, err) {
			t.FailNow()
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
		if !(assert.NoError(t, err) &&
			assert.Equal(t, sts.WorkSpecData["name"], sts.WorkSpec.Name())) {
			t.FailNow()
		}
	}

	// Create the work unit
	if sts.WorkSpec != nil && sts.WorkUnitName != "" {
		if sts.WorkUnitData == nil {
			sts.WorkUnitData = map[string]interface{}{}
		}
		sts.WorkUnit, err = sts.WorkSpec.AddWorkUnit(sts.WorkUnitName, sts.WorkUnitData, sts.WorkUnitMeta)
		if !(assert.NoError(t, err) &&
			assert.Equal(t, sts.WorkUnitName, sts.WorkUnit.Name()) &&
			assert.Equal(t, sts.WorkSpec.Name(), sts.WorkUnit.WorkSpec().Name())) {
			t.FailNow()
		}
	}

	// Create the worker
	if sts.WorkerName != "" {
		sts.Worker, err = sts.Namespace.Worker(sts.WorkerName)
		if !(assert.NoError(t, err) &&
			assert.Equal(t, sts.WorkerName, sts.Worker.Name())) {
			t.FailNow()
		}
	}
}

// TearDown destroys the namespace and all other resources created in
// SetUp.
func (sts *SimpleTestSetup) TearDown(t *testing.T) {
	if sts.Namespace != nil {
		err := sts.Namespace.Destroy()
		assert.NoError(t, err)
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
func (sts *SimpleTestSetup) CheckUnitStatus(t *testing.T, status coordinate.WorkUnitStatus) {
	actual, err := sts.WorkUnit.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, status, actual)
	}
}

// RequestOneAttempt gets a single attempt from the test's worker, or
// fails the test immediately if not exactly one attempt was returned.
func (sts *SimpleTestSetup) RequestOneAttempt(t *testing.T) coordinate.Attempt {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if !(assert.NoError(t, err) && assert.Len(t, attempts, 1)) {
		t.FailNow()
	}
	return attempts[0]
}

// RequestNoAttempts requests attempts and asserts that nothing was
// returned.  It does not fail the test if something does come back.
func (sts *SimpleTestSetup) RequestNoAttempts(t *testing.T) {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	if assert.NoError(t, err) {
		assert.Empty(t, attempts)
	}
}

// CheckWorkUnitOrder requests every possible attempt, one at a time,
// virtually pausing 5 seconds between each.  It checks that the
// resulting ordering matches the provided order of work unit names.
func (sts *SimpleTestSetup) CheckWorkUnitOrder(t *testing.T, unitNames ...string) {
	var processedUnits []string
	for {
		Clock.Add(time.Duration(5) * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if len(attempts) == 0 {
			break
		}
		if !assert.Len(t, attempts, 1) {
			t.FailNow()
		}
		attempt := attempts[0]
		assert.Equal(t, sts.WorkSpec.Name(), attempt.WorkUnit().WorkSpec().Name())
		processedUnits = append(processedUnits, attempt.WorkUnit().Name())
		err = attempt.Finish(nil)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}

	assert.Equal(t, unitNames, processedUnits)
}
