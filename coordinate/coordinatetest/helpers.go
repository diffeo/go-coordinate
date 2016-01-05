// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
	"reflect"
	"time"
)

// ---------------------------------------------------------------------------
// HasKeys

type hasKeysChecker struct {
	*check.CheckerInfo
}

func (c hasKeysChecker) Info() *check.CheckerInfo {
	return c.CheckerInfo
}

func (c hasKeysChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "incorrect number of parameters to HasKeys check"
	}
	if len(names) != 2 {
		return false, "incorrect number of names to HasKeys check"
	}
	obtained := reflect.ValueOf(params[0])
	if obtained.Type().Kind() != reflect.Map {
		return false, fmt.Sprintf("%v value is not a map", names[0])
	}
	expected, ok := params[1].([]string)
	if !ok {
		return false, "expected keys for HasKeys check not a []string"
	}
	for _, key := range expected {
		value := obtained.MapIndex(reflect.ValueOf(key))
		if !value.IsValid() {
			return false, fmt.Sprintf("missing key %v", key)
		}
	}
	return true, ""
}

// The HasKeys checker verifies that a map has an expected set of keys.
// The values of the provided map are not checked, and extra keys are not
// checked for (try a HasLen check in addition to this).
//
//     var map[string]interface{} actual
//     actual = ...
//     c.Check(actual, HasKeys, []string{"foo", "bar"})
var HasKeys check.Checker = &hasKeysChecker{
	&check.CheckerInfo{
		Name:   "HasKeys",
		Params: []string{"obtained", "expected"},
	},
}

// ---------------------------------------------------------------------------
// AttemptMatches

type attemptMatchesChecker struct {
	*check.CheckerInfo
}

func (c attemptMatchesChecker) Info() *check.CheckerInfo {
	return c.CheckerInfo
}

func (c attemptMatchesChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "incorrect number of parameters to AttemptMatches check"
	}
	obtained, ok := params[0].(coordinate.Attempt)
	if !ok {
		return false, "non-Attempt obtained value"
	}
	expected, ok := params[1].(coordinate.Attempt)
	if !ok {
		return false, "non-Attempt expected value"
	}
	if obtained.Worker().Name() != expected.Worker().Name() {
		return false, "mismatched workers"
	}
	if obtained.WorkUnit().Name() != expected.WorkUnit().Name() {
		return false, "mismatched work units"
	}
	if obtained.WorkUnit().WorkSpec().Name() != expected.WorkUnit().WorkSpec().Name() {
		return false, "mismatched work specs"
	}
	return true, ""
}

// The AttemptMatches checker verifies that two attempts are compatible
// based on their observable data.
var AttemptMatches check.Checker = &attemptMatchesChecker{
	&check.CheckerInfo{
		Name:   "AttemptMatches",
		Params: []string{"obtained", "expected"},
	},
}

// ---------------------------------------------------------------------------
// SameTime

type sameTimeChecker struct {
	*check.CheckerInfo
}

func (c sameTimeChecker) Info() *check.CheckerInfo {
	return c.CheckerInfo
}

func (c sameTimeChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "incorrect number of parameters to SameTime check"
	}
	obtained, ok := params[0].(time.Time)
	if !ok {
		return false, "non-Time obtained value"
	}
	expected, ok := params[1].(time.Time)
	if !ok {
		return false, "non-Time expected value"
	}
	// NB: the postgres backend rounds to the microsecond
	maxDelta := time.Duration(1) * time.Microsecond
	delta := obtained.Sub(expected)
	return delta < maxDelta && delta > -maxDelta, ""
}

// The SameTime checker verifies that two times are identical, or at
// least, very very close.
var SameTime check.Checker = &sameTimeChecker{
	&check.CheckerInfo{
		Name:   "SameTime",
		Params: []string{"obtained", "expected"},
	},
}

// ---------------------------------------------------------------------------
// Like

type likeChecker struct {
	*check.CheckerInfo
}

func (c likeChecker) Info() *check.CheckerInfo {
	return c.CheckerInfo
}

func (c likeChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "incorrect number of parameters to Like check"
	}
	obtained := params[0]
	expected := params[1]
	obtainedV := reflect.ValueOf(obtained)
	expectedV := reflect.ValueOf(expected)
	obtainedT := obtainedV.Type()
	expectedT := expectedV.Type()

	if !obtainedT.ConvertibleTo(expectedT) {
		return false, "wrong type"
	}

	convertedV := obtainedV.Convert(expectedT)
	converted := convertedV.Interface()
	return converted == expected, ""
}

// The SameTime checker verifies that two objects are equal, once the
// obtained value has been cast to the type of the expected value.
var Like check.Checker = &likeChecker{
	&check.CheckerInfo{
		Name:   "Like",
		Params: []string{"obtained", "expected"},
	},
}

// ---------------------------------------------------------------------------
// SimpleTestSetup

// SimpleTestSetup defines parameters for common tests, that use
// a small number of workers, work specs, etc.
type SimpleTestSetup struct {
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

// Do populates the output fields of the test setup, or fails using
// c.Assert().
func (sts *SimpleTestSetup) Do(s *Suite, c *check.C) {
	var err error

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
		sts.WorkSpec, err = s.Namespace.SetWorkSpec(sts.WorkSpecData)
		c.Assert(err, check.IsNil)
		c.Assert(sts.WorkSpec.Name(), check.DeepEquals, sts.WorkSpecData["name"])
	}

	// Create the work unit
	if sts.WorkSpec != nil && sts.WorkUnitName != "" {
		if sts.WorkUnitData == nil {
			sts.WorkUnitData = map[string]interface{}{}
		}
		sts.WorkUnit, err = sts.WorkSpec.AddWorkUnit(sts.WorkUnitName, sts.WorkUnitData, sts.WorkUnitMeta)
		c.Assert(err, check.IsNil)
		c.Assert(sts.WorkUnit.Name(), check.Equals, sts.WorkUnitName)
		c.Assert(sts.WorkUnit.WorkSpec().Name(), check.Equals, sts.WorkSpec.Name())
	}

	// Create the worker
	if sts.WorkerName != "" {
		sts.Worker, err = s.Namespace.Worker(sts.WorkerName)
		c.Assert(err, check.IsNil)
		c.Assert(sts.Worker.Name(), check.Equals, sts.WorkerName)
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

// CheckUnitStatus checks in c that the test's work unit's status matches
// an expected value.
func (sts *SimpleTestSetup) CheckUnitStatus(c *check.C, status coordinate.WorkUnitStatus) {
	actual, err := sts.WorkUnit.Status()
	c.Assert(err, check.IsNil)
	c.Check(actual, check.Equals, status)
}

// RequestOneAttempt gets a single attempt from the test's worker,
// or asserts in c if not exactly one attempt was returned.
func (sts *SimpleTestSetup) RequestOneAttempt(c *check.C) coordinate.Attempt {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 1)
	return attempts[0]
}

// RequestNoAttempts requests attempts and asserts in c that nothing
// was returned.
func (sts *SimpleTestSetup) RequestNoAttempts(c *check.C) {
	attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(attempts, check.HasLen, 0)
}

// CheckWorkUnitOrder requests every possible attempt, one at a time,
// virtually pausing 5 seconds between each.  It checks in c that the
// resulting ordering matches the provided order of work unit names.
func (sts *SimpleTestSetup) CheckWorkUnitOrder(s *Suite, c *check.C, unitNames ...string) {
	var processedUnits []string
	for {
		s.Clock.Add(time.Duration(5) * time.Second)
		attempts, err := sts.Worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		if len(attempts) == 0 {
			break
		}
		c.Assert(attempts, check.HasLen, 1)
		attempt := attempts[0]
		c.Check(attempt.WorkUnit().WorkSpec().Name(), check.Equals, sts.WorkSpec.Name())
		processedUnits = append(processedUnits, attempt.WorkUnit().Name())
		err = attempt.Finish(nil)
		c.Assert(err, check.IsNil)
	}

	c.Check(processedUnits, check.DeepEquals, unitNames)
}
