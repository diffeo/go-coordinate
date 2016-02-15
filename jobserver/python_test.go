// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

// This file has ports of the assorted interesting Python-based tests
// from https://github.com/diffeo/coordinate/coordinate/test.

import (
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/jobserver"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Clock contains the mock time source.
var Clock = clock.NewMock()

// Coordinate contains the top-level interface to the backend
// for the job server.
var Coordinate = memory.NewWithClock(Clock)

// WorkSpecData contains the reference work spec.
var WorkSpecData = map[string]interface{}{
	"name":         "test_job_client",
	"min_gb":       1,
	"module":       "coordinate.tests.test_job_client",
	"run_function": "run_function",
}

func setUpTest(t *testing.T, name string) *jobserver.JobServer {
	namespace, err := Coordinate.Namespace(name)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return &jobserver.JobServer{
		Namespace: namespace,
		Clock:     Clock,
	}
}

func tearDownTest(t *testing.T, j *jobserver.JobServer) {
	err := j.Namespace.Destroy()
	assert.NoError(t, err)
}

// Assorted useful constantish values:
var (
	// noWorkUnits is an empty work unit map
	noWorkUnits = map[string]map[string]interface{}{}

	// gwuEverything is parameters for GetWorkUnits returning
	// all work units
	gwuEverything = map[string]interface{}{}

	// gwuDefault is the default list_work_units settings for
	// GetWorkUnits, returning both available and pending units
	gwuDefault = map[string]interface{}{
		"state": []interface{}{jobserver.Available, jobserver.Pending},
	}

	// gwuFinished is a GetWorkUnits parameter map returning only
	// finished units
	gwuFinished = map[string]interface{}{
		"state": jobserver.Finished,
	}

	// allStates lists out the valid work unit states.
	allStates = []jobserver.WorkUnitStatus{
		jobserver.Available,
		jobserver.Pending,
		jobserver.Finished,
		jobserver.Failed,
	}

	// stateShortName gives brief abbreviations for state names
	// that are used as work unit keys.
	stateShortName = map[jobserver.WorkUnitStatus]string{
		jobserver.Available: "AV",
		jobserver.Pending:   "PE",
		jobserver.Finished:  "FI",
		jobserver.Failed:    "FA",
	}

	// stateShortName2 gives another set of work unit keys.
	stateShortName2 = map[jobserver.WorkUnitStatus]string{
		jobserver.Available: "AI",
		jobserver.Pending:   "ND",
		jobserver.Finished:  "NI",
		jobserver.Failed:    "IL",
	}
)

// Helpers that do some checking

// makeWorkSpec creates a copy of WorkSpecData with additional fields
// added in.
func makeWorkSpec(extras map[string]interface{}) map[string]interface{} {
	data := make(map[string]interface{})
	for k, v := range WorkSpecData {
		data[k] = v
	}
	for k, v := range extras {
		data[k] = v
	}
	return data
}

// setWorkSpec calls the eponymous JobServer function, checking that
// it ran successfully, and returns the work spec name on success.
func setWorkSpec(t *testing.T, j *jobserver.JobServer, workSpec map[string]interface{}) string {
	ok, msg, err := j.SetWorkSpec(workSpec)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	workSpecName, ok := workSpec["name"].(string)
	assert.True(t, ok, "workSpec[\"name\"] not a string")
	return workSpecName
}

// addWorkUnit packages a single work unit key and data dictionary
// into the tuple format JobServer expects, and calls AddWorkUnits(),
// checking the result.
func addWorkUnit(t *testing.T, j *jobserver.JobServer, workSpecName, key string, data map[string]interface{}) {
	keyDataPair := cborrpc.PythonTuple{Items: []interface{}{key, data}}
	keyDataList := []interface{}{keyDataPair}
	ok, msg, err := j.AddWorkUnits(workSpecName, keyDataList)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

// addWorkUnits adds a batch of work units to the system in one call.
func addWorkUnits(t *testing.T, j *jobserver.JobServer, workSpecName string, workUnits map[string]map[string]interface{}) {
	// Assemble the parameters to AddWorkUnits as one big list of
	// pairs of (key, data)
	var awu []interface{}
	for name, data := range workUnits {
		pair := []interface{}{name, data}
		awu = append(awu, pair)
	}
	ok, msg, err := j.AddWorkUnits(workSpecName, awu)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

// prioritizeWorkUnit changes the priority of a single work unit.
func prioritizeWorkUnit(t *testing.T, j *jobserver.JobServer, workSpecName, key string, priority int, adjust bool) {
	options := map[string]interface{}{
		"work_unit_keys": []interface{}{key},
	}
	if adjust {
		options["priority"] = nil
		options["adjustment"] = priority
	} else {
		options["priority"] = priority
		options["adjustment"] = nil
	}
	ok, msg, err := j.PrioritizeWorkUnits(workSpecName, options)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

// addPrefixedWorkUnits adds a series of similarly-named work units
// to a work spec.  If prefix is "u", it adds count work units named
// u001, u002, ....  The work spec dictionaries have a single key "k"
// with values v1, v2, ....
func addPrefixedWorkUnits(t *testing.T, j *jobserver.JobServer, workSpecName, prefix string, count int) {
	workUnitKvps := make([]interface{}, count)
	for i := range workUnitKvps {
		key := fmt.Sprintf("%s%03d", prefix, i+1)
		data := map[string]interface{}{"k": fmt.Sprintf("v%v", i+1)}
		items := []interface{}{key, data}
		workUnitKvps[i] = cborrpc.PythonTuple{Items: items}
	}
	ok, msg, err := j.AddWorkUnits(workSpecName, workUnitKvps)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

// expectPrefixedWorkUnits returns a list of expected return values from
// the GetWork call, if the returns are the first count work units from
// workSpecName with prefix prefix.
func expectPrefixedWorkUnits(workSpecName, prefix string, count int) interface{} {
	result := make([]cborrpc.PythonTuple, count)
	for i := range result {
		key := fmt.Sprintf("%s%03d", prefix, i+1)
		data := map[string]interface{}{"k": fmt.Sprintf("v%v", i+1)}
		items := []interface{}{workSpecName, []byte(key), data}
		result[i] = cborrpc.PythonTuple{Items: items}
	}
	return result
}

// getOneWorkUnit calls GetWorkUnits for a single specific work unit,
// checks the results, and returns its data dictionary (or nil if absent).
func getOneWorkUnit(t *testing.T, j *jobserver.JobServer, workSpecName, workUnitKey string) map[string]interface{} {
	list, msg, err := j.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{workUnitKey}})
	if !assert.NoError(t, err) {
		return nil
	}
	assert.Empty(t, msg)
	if !assert.Len(t, list, 1) {
		return nil
	}
	if !assert.IsType(t, cborrpc.PythonTuple{}, list[0]) {
		return nil
	}
	tuple := list[0].(cborrpc.PythonTuple)
	if !assert.Len(t, tuple.Items, 2) {
		return nil
	}
	assert.Equal(t, []byte(workUnitKey), tuple.Items[0])
	if assert.IsType(t, map[string]interface{}{}, tuple.Items[1]) {
		return tuple.Items[1].(map[string]interface{})
	}
	return nil
}

// listWorkUnits calls GetWorkUnits (as the similarly-named Python
// function does) and validates that the response matches an expected
// set of work units.
func listWorkUnits(t *testing.T, j *jobserver.JobServer, workSpecName string, options map[string]interface{}, expected map[string]map[string]interface{}) {
	gwu, msg, err := j.GetWorkUnits(workSpecName, options)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, msg)
	missing := make(map[string]struct{})
	for name := range expected {
		missing[name] = struct{}{}
	}
	for _, item := range gwu {
		if !assert.IsType(t, cborrpc.PythonTuple{}, item) {
			continue
		}
		tuple := item.(cborrpc.PythonTuple)
		if !assert.Len(t, tuple.Items, 2) {
			continue
		}
		if !assert.IsType(t, []byte{}, tuple.Items[0]) {
			continue
		}
		name := string(tuple.Items[0].([]byte))
		assert.IsType(t, map[string]interface{}{}, tuple.Items[1])
		if assert.Contains(t, expected, name, "unexpected work unit") {
			assert.Equal(t, expected[name], tuple.Items[1])
		}
		assert.Contains(t, missing, name, "duplicate work unit")
		delete(missing, name)
	}
}

// finishWorkUnit marks a specific work unit as finished.
func finishWorkUnit(t *testing.T, j *jobserver.JobServer, workSpecName, workUnitKey string, data map[string]interface{}) {
	options := map[string]interface{}{
		"status": jobserver.Finished,
	}
	if data != nil {
		options["data"] = data
	}
	ok, msg, err := j.UpdateWorkUnit(workSpecName, workUnitKey, options)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

// checkWorkUnitStatus makes a weak assertion about a specific work
// unit's status by calling GetWorkUnitStatus for it.
func checkWorkUnitStatus(t *testing.T, j *jobserver.JobServer, workSpecName, workUnitKey string, status jobserver.WorkUnitStatus) {
	dicts, msg, err := j.GetWorkUnitStatus(workSpecName, []string{workUnitKey})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)
		if assert.Len(t, dicts, 1) {
			assert.Equal(t, status, dicts[0]["status"])
		}
	}
}

func checkChildWorkUnits(t *testing.T, j *jobserver.JobServer, parent, child, workSpecName string, expected map[string]map[string]interface{}) {
	missing := make(map[string]struct{})
	for name := range expected {
		missing[name] = struct{}{}
	}
	units, msg, err := j.GetChildWorkUnits(parent)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, msg)
	assert.Len(t, units, 1)
	if assert.Contains(t, units, child) {
		for _, unit := range units[child] {
			assert.Equal(t, child, unit["worker_id"])
			assert.Equal(t, workSpecName, unit["work_spec_name"])
			if assert.IsType(t, []byte{}, unit["work_unit_key"]) {
				bName := unit["work_unit_key"].([]byte)
				name := string(bName)
				if assert.Contains(t, expected, name) {
					assert.Equal(t, expected[name], unit["work_unit_data"])
				}
				assert.Contains(t, missing, name, "duplicate child work unit")
				delete(missing, name)
			}
		}
	}
	assert.Empty(t, missing)
}

func getOneWork(t *testing.T, j *jobserver.JobServer) (ok bool, workSpecName, workUnitKey string, workUnitData map[string]interface{}) {
	anything, msg, err := j.GetWork("test", map[string]interface{}{"available_gb": 1})
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, msg)
	// Since we didn't request multiple work units we should always
	// get at most one, but maybe none
	if assert.NotNil(t, anything) &&
		assert.IsType(t, cborrpc.PythonTuple{}, anything) {
		tuple := anything.(cborrpc.PythonTuple)
		if assert.Len(t, tuple.Items, 3) {
			// "no work unit" gets returned as tuple (nil,
			// nil, nil)
			if tuple.Items[0] != nil &&
				assert.IsType(t, "", tuple.Items[0]) &&
				assert.IsType(t, []byte{}, tuple.Items[1]) &&
				assert.IsType(t, map[string]interface{}{}, tuple.Items[2]) {
				ok = true
				workSpecName = tuple.Items[0].(string)
				bWorkUnitKey := tuple.Items[1].([]byte)
				workUnitKey = string(bWorkUnitKey)
				workUnitData = tuple.Items[2].(map[string]interface{})
			}
		}
	}
	return
}

// getSpecificWork calls GetWork expecting a specific work unit to
// come back, and returns its data dictionary.
func getSpecificWork(t *testing.T, j *jobserver.JobServer, workSpecName, workUnitKey string) map[string]interface{} {
	anything, msg, err := j.GetWork("test", map[string]interface{}{"available_gb": 1})
	if !assert.NoError(t, err) {
		return nil
	}
	assert.Empty(t, msg)
	if assert.NotNil(t, anything) && assert.IsType(t, cborrpc.PythonTuple{}, anything) {
		tuple := anything.(cborrpc.PythonTuple)
		if assert.Len(t, tuple.Items, 3) && assert.NotNil(t, tuple.Items[0]) {
			assert.Equal(t, workSpecName, tuple.Items[0])
			assert.Equal(t, []byte(workUnitKey), tuple.Items[1])
			if assert.IsType(t, tuple.Items[2], map[string]interface{}{}) {
				return tuple.Items[2].(map[string]interface{})
			}
		}
	}
	return nil
}

func doOneWork(t *testing.T, j *jobserver.JobServer, workSpecName, workUnitKey string) {
	ok, gotSpec, gotKey, _ := getOneWork(t, j)
	if assert.True(t, ok) {
		assert.Equal(t, workSpecName, gotSpec)
		assert.Equal(t, workUnitKey, gotKey)
		finishWorkUnit(t, j, workSpecName, workUnitKey, nil)
	}
}

func doNoWork(t *testing.T, j *jobserver.JobServer) {
	ok, gotSpec, gotKey, _ := getOneWork(t, j)
	assert.False(t, ok, "got work spec %v key %v", gotSpec, gotKey)
}

// Tests from test_job_client.py

// DoWork runs through a basic sequence of creating a work spec,
// adding a work unit to it, and running it.  There are several
// validity checks along the way.  Returns the final value of the work
// unit state after running the test, or calls c.Assert() (e.g.,
// panics) if this becomes impossible.
func DoWork(t *testing.T, j *jobserver.JobServer, key string, data map[string]interface{}) map[string]interface{} {
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, key, data)

	dict := getOneWorkUnit(t, j, workSpecName, key)
	assert.Equal(t, data, dict)
	checkWorkUnitStatus(t, j, workSpecName, key, jobserver.Available)

	wuData := getSpecificWork(t, j, workSpecName, key)
	assert.Equal(t, data, wuData)

	listWorkUnits(t, j, workSpecName,
		map[string]interface{}{"work_unit_keys": []interface{}{key}},
		map[string]map[string]interface{}{key: data})

	checkWorkUnitStatus(t, j, workSpecName, key, jobserver.Pending)

	// This "runs" the work unit
	wuData["output"] = map[string]interface{}{
		"foo": map[string]interface{}{"bar": "baz"},
	}
	wuData["args"] = cborrpc.PythonTuple{Items: []interface{}{"arg"}}

	finishWorkUnit(t, j, workSpecName, key, wuData)
	checkWorkUnitStatus(t, j, workSpecName, key, jobserver.Finished)
	dict = getOneWorkUnit(t, j, workSpecName, key)
	assert.NotNil(t, dict)
	return dict
}

// TestDataUpdates runs through the full sequence of creating a work
// spec and work unit and running them, and verifies that the data
// dictionary did in fact get updated.
func TestDataUpdates(t *testing.T) {
	j := setUpTest(t, "TestDataUpdates")
	defer tearDownTest(t, j)

	res := DoWork(t, j, "u", map[string]interface{}{"k": "v"})
	assert.Equal(t, map[string]interface{}{
		"k": "v",
		"output": map[string]interface{}{
			"foo": map[string]interface{}{
				"bar": "baz",
			},
		},
		"args": cborrpc.PythonTuple{
			Items: []interface{}{"arg"},
		},
	}, res)
}

// Skipping TestArgs and TestKwargs: these test specific behaviors of
// the Python WorkUnit.run() call and how it invokes run_function,
// which are out of scope here

// TestPause validates that pausing and unpausing a work spec affect
// what GetWork returns.
func TestPause(t *testing.T) {
	j := setUpTest(t, "TestPause")
	defer tearDownTest(t, j)

	workUnits := map[string]map[string]interface{}{"u": {"k": "v"}}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)

	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)
	checkWorkUnitStatus(t, j, workSpecName, "u", jobserver.Available)

	// Pause the work spec
	ok, msg, err := j.ControlWorkSpec(workSpecName, map[string]interface{}{"status": jobserver.Paused})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// We should not get work now
	ok, spec, unit, _ := getOneWork(t, j)
	assert.False(t, ok)

	// Resume the work spec
	ok, msg, err = j.ControlWorkSpec(workSpecName, map[string]interface{}{"status": jobserver.Runnable})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// We should the work unit back
	ok, spec, unit, _ = getOneWork(t, j)
	if assert.True(t, ok) {
		assert.Equal(t, workSpecName, spec)
		assert.Equal(t, "u", unit)
	}
}

// TestGetMany tests that the GetWork call with a "max_jobs" parameter
// actually retrieves the requested number of jobs.
func TestGetMany(t *testing.T) {
	j := setUpTest(t, "TestGetMany")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addPrefixedWorkUnits(t, j, workSpecName, "u", 100)

	anything, msg, err := j.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	if assert.Nil(t, err) {
		assert.Empty(t, msg)
		expected := expectPrefixedWorkUnits(workSpecName, "u", 10)
		assert.Equal(t, expected, anything)
	}
}

// TestGetManyMaxGetwork tests that the GetWork call with a "max_jobs"
// parameter actually retrieves the requested number of jobs.
func TestGetManyMaxGetwork(t *testing.T) {
	j := setUpTest(t, "TestGetManyMaxGetwork")
	defer tearDownTest(t, j)

	data := makeWorkSpec(map[string]interface{}{"max_getwork": 5})
	workSpecName := setWorkSpec(t, j, data)
	addPrefixedWorkUnits(t, j, workSpecName, "u", 100)

	anything, msg, err := j.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)

		// Even though we requested 10 jobs, we should
		// actually get 5 (e.g. the work spec's max_getwork)
		expected := expectPrefixedWorkUnits(workSpecName, "u", 5)
		assert.Equal(t, expected, anything)
	}
}

// TestGetTooMany tests what happens when there are two work specs,
// and the one that gets chosen has fewer work units than are requested.
// This test validates that the higher-weight work spec is chosen and
// that the GetWork call does not "spill" to retrieving work units from
// the other work spec.
func TestGetTooMany(t *testing.T) {
	j := setUpTest(t, "TestGetTooMany")
	defer tearDownTest(t, j)

	data := makeWorkSpec(map[string]interface{}{"weight": 1})
	workSpecName := setWorkSpec(t, j, data)
	addPrefixedWorkUnits(t, j, workSpecName, "u", 100)

	otherWorkSpec := map[string]interface{}{
		"name":         "ws2",
		"min_gb":       0.1,
		"module":       "coordinate.tests.test_job_client",
		"run_function": "run_function",
		"priority":     2,
	}
	otherWorkSpecName := setWorkSpec(t, j, otherWorkSpec)
	addPrefixedWorkUnits(t, j, otherWorkSpecName, "z", 4)

	anything, msg, err := j.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)

		// We requested 10 jobs.  Since ws2 has higher
		// priority, the scheduler should choose it.  But,
		// that has only 4 work units in it.  We should get
		// exactly those.
		expected := expectPrefixedWorkUnits(otherWorkSpecName, "z", 4)
		assert.Equal(t, expected, anything)
	}
}

// TestPrioritize tests basic work unit prioritization.
func TestPrioritize(t *testing.T) {
	j := setUpTest(t, "TestPrioritize")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "b", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Default order is alphabetical
	doOneWork(t, j, workSpecName, "a")

	// If we prioritize c, it should go first, before b
	prioritizeWorkUnit(t, j, workSpecName, "c", 1, false)

	doOneWork(t, j, workSpecName, "c")
	doOneWork(t, j, workSpecName, "b")
	doNoWork(t, j)
}

// TestPrioritizeAdjust tests using the "adjust" mode to change work unit
// priorities.
func TestPrioritizeAdjust(t *testing.T) {
	j := setUpTest(t, "TestPrioritizeAdjust")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "b", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Use "adjust" mode to adjust the priorities
	prioritizeWorkUnit(t, j, workSpecName, "a", 10, true)
	prioritizeWorkUnit(t, j, workSpecName, "b", 20, true)
	prioritizeWorkUnit(t, j, workSpecName, "c", 30, true)

	// Highest priority goes first
	doOneWork(t, j, workSpecName, "c")
	doOneWork(t, j, workSpecName, "b")
	doOneWork(t, j, workSpecName, "a")
	doNoWork(t, j)
}

// TestReprioritize tests that changing a work unit priority mid-stream
// correctly adjusts the work unit priorities.
func TestReprioritize(t *testing.T) {
	j := setUpTest(t, "TestReprioritize")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "b", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Use "priority" mode to set the priorities
	prioritizeWorkUnit(t, j, workSpecName, "a", 10, false)
	prioritizeWorkUnit(t, j, workSpecName, "b", 20, false)
	prioritizeWorkUnit(t, j, workSpecName, "c", 30, false)

	// Highest priority goes first
	doOneWork(t, j, workSpecName, "c")

	// Now adjust "a" to have higher priority
	prioritizeWorkUnit(t, j, workSpecName, "a", 15, true) // +10 = 25

	doOneWork(t, j, workSpecName, "a")
	doOneWork(t, j, workSpecName, "b")
	doNoWork(t, j)
}

// TestSucceedFail tests that failing a finished work unit is a no-op.
// This can happen if a work unit finishes successfully just before its
// timeout, and its parent worker tries to kill it.
func TestSucceedFail(t *testing.T) {
	j := setUpTest(t, "TestSucceedFail")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})
	doOneWork(t, j, workSpecName, "a")

	// ...meanwhile, the parent nukes us from orbit
	ok, _, err := j.UpdateWorkUnit(workSpecName, "a", map[string]interface{}{"status": jobserver.Failed})
	if assert.NoError(t, err) {
		// This should return "no-op"
		assert.False(t, ok)
	}

	// The end status should be "succeeded"
	checkWorkUnitStatus(t, j, workSpecName, "a", jobserver.Finished)
}

// TestFailSucceed tests that finishing a failed work unit makes it
// finished.  This happens under the same conditions as
// TestSucceedFail, but with different timing.
func TestFailSucceed(t *testing.T) {
	j := setUpTest(t, "TestFailSucceed")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})

	// Get the work unit
	ok, gotSpec, gotKey, _ := getOneWork(t, j)
	if assert.True(t, ok) {
		assert.Equal(t, workSpecName, gotSpec)
		assert.Equal(t, "a", gotKey)
	}

	// Meanwhile, the parent nukes us from orbit
	ok, msg, err := j.UpdateWorkUnit(workSpecName, "a", map[string]interface{}{"status": jobserver.Failed})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// But wait!  We actually did the job!
	finishWorkUnit(t, j, workSpecName, "a", nil)

	// The end status should be "succeeded"
	checkWorkUnitStatus(t, j, workSpecName, "a", jobserver.Finished)
}

// TestGetChildUnitsBasic verifies the GetChildWorkUnits call with a
// basic work flow.
func TestGetChildUnitsBasic(t *testing.T) {
	j := setUpTest(t, "TestGetChildUnitsBasic")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})

	// register parent worker
	ok, msg, err := j.WorkerHeartbeat("parent", "run", 6000, nil, "")
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// register child worker
	ok, msg, err = j.WorkerHeartbeat("child", "run", 6000, nil, "parent")
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// right now there should be no child units
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, noWorkUnits)

	// get the work unit
	anything, msg, err := j.GetWork("child", map[string]interface{}{"available_gb": 1})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)
		if assert.NotNil(t, anything) && assert.IsType(t, cborrpc.PythonTuple{}, anything) {
			tuple := anything.(cborrpc.PythonTuple)
			if assert.Len(t, tuple.Items, 3) {
				assert.Equal(t, workSpecName, tuple.Items[0])
				assert.Equal(t, []byte("a"), tuple.Items[1])
				assert.Equal(t, map[string]interface{}{"k": "v"}, tuple.Items[2])
			}
		}
	}

	// it should be reported as a child unit
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, map[string]map[string]interface{}{"a": {"k": "v"}})

	// now finish it
	finishWorkUnit(t, j, workSpecName, "a", nil)

	// there should be no work units left now
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, noWorkUnits)
}

// TestGetChildUnitsMulti verifies the GetChildWorkUnits call when the child gets multiple work units.
func TestGetChildUnitsMulti(t *testing.T) {
	j := setUpTest(t, "TestGetChildUnitsMulti")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "a", map[string]interface{}{"k": "v"})
	addWorkUnit(t, j, workSpecName, "b", map[string]interface{}{"k": "v"})

	// register parent worker
	ok, msg, err := j.WorkerHeartbeat("parent", "run", 6000, nil, "")
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// register child worker
	ok, msg, err = j.WorkerHeartbeat("child", "run", 6000, nil, "parent")
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// right now there should be no child units
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, noWorkUnits)

	// get the work units
	anything, msg, err := j.GetWork("child", map[string]interface{}{"available_gb": 1, "max_jobs": 10})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)
		if assert.NotNil(t, anything) && assert.IsType(t, []cborrpc.PythonTuple{}, anything) {
			list := anything.([]cborrpc.PythonTuple)
			assert.Len(t, list, 2)
			for i, tuple := range list {
				if assert.Len(t, tuple.Items, 3) {
					assert.Equal(t, workSpecName, tuple.Items[0])
					if i == 0 {
						assert.Equal(t, []byte("a"), tuple.Items[1])
					} else {
						assert.Equal(t, []byte("b"), tuple.Items[1])
					}
					assert.Equal(t, map[string]interface{}{"k": "v"}, tuple.Items[2])
				}
			}
		}
	}

	// both should be reported as child units
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, map[string]map[string]interface{}{"a": {"k": "v"}, "b": {"k": "v"}})

	// finish "a"
	finishWorkUnit(t, j, workSpecName, "a", nil)

	// we should have "b" left
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, map[string]map[string]interface{}{"b": {"k": "v"}})

	// now finish b
	finishWorkUnit(t, j, workSpecName, "b", nil)

	// there should be no work units left now
	checkChildWorkUnits(t, j, "parent", "child", workSpecName, noWorkUnits)
}

// Tests from test_job_flow.py
//
// Note that several of the tests here really test the client-side
// TaskMaster.add_flow() function and some of the parameter passing
// of the Python worker implementation.  The important and interesting
// tests are the ones that exercise the work spec "then" field.
//
// Also note that the SimplifiedScheduler does not automatically
// prefer later work specs to earlier ones.  None of the ported tests
// take advantage of that.
//
// There are two tests that really exercise the "job flow" part.

// TestSimpleFlow verifies that the output of an earlier work spec
// can become a work unit in a later work spec.  The work unit "output"
// field is a mapping of work unit name to definition.
func TestSimpleFlow(t *testing.T) {
	j := setUpTest(t, "TestSimpleFlow")
	defer tearDownTest(t, j)

	setWorkSpec(t, j, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	setWorkSpec(t, j, map[string]interface{}{
		"name": "second",
	})
	addWorkUnit(t, j, "first", "u", map[string]interface{}{"k": "v"})

	data := getSpecificWork(t, j, "first", "u")
	assert.Equal(t, map[string]interface{}{"k": "v"}, data)
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"x": "y"},
	}
	finishWorkUnit(t, j, "first", "u", data)

	data = getSpecificWork(t, j, "second", "u")
	assert.Equal(t, map[string]interface{}{"x": "y"}, data)
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	finishWorkUnit(t, j, "second", "u", data)

	doNoWork(t, j)
}

// TestSimpleOutput verifies that the output of an earlier work spec
// can become a work unit in a later work spec.  The work unit "output"
// field is a flat list of work unit keys.
func TestSimpleOutput(t *testing.T) {
	j := setUpTest(t, "TestSimpleOutput")
	defer tearDownTest(t, j)

	setWorkSpec(t, j, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	setWorkSpec(t, j, map[string]interface{}{
		"name": "second",
	})
	addWorkUnit(t, j, "first", "u", map[string]interface{}{"k": "v"})

	data := getSpecificWork(t, j, "first", "u")
	assert.Equal(t, map[string]interface{}{"k": "v"}, data)
	data["output"] = []interface{}{"u"}
	finishWorkUnit(t, j, "first", "u", data)

	data = getSpecificWork(t, j, "second", "u")
	assert.Equal(t, map[string]interface{}{}, data)
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	finishWorkUnit(t, j, "second", "u", data)

	doNoWork(t, j)
}

// TestSimpleOutputBytes is the same as TestSimpleOutput, but the "next"
// work unit name is a byte string.
func TestSimpleOutputBytes(t *testing.T) {
	j := setUpTest(t, "TestSimpleOutputBytes")
	defer tearDownTest(t, j)

	setWorkSpec(t, j, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	setWorkSpec(t, j, map[string]interface{}{
		"name": "second",
	})
	addWorkUnit(t, j, "first", "u", map[string]interface{}{"k": "v"})

	data := getSpecificWork(t, j, "first", "u")
	assert.Equal(t, map[string]interface{}{"k": "v"}, data)
	data["output"] = []interface{}{[]byte("u")}
	finishWorkUnit(t, j, "first", "u", data)

	data = getSpecificWork(t, j, "second", "u")
	assert.Equal(t, map[string]interface{}{}, data)
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	finishWorkUnit(t, j, "second", "u", data)

	doNoWork(t, j)
}

// Tests from test_task_master.py

// TestListWorkSpecs verifies that ListWorkSpecs will return one work
// spec when added.
func TestListWorkSpecs(t *testing.T) {
	j := setUpTest(t, "TestListWorkSpecs")
	defer tearDownTest(t, j)

	// Initial state is nothing
	specs, next, err := j.ListWorkSpecs(map[string]interface{}{})
	if assert.NoError(t, err) {
		assert.Equal(t, []map[string]interface{}{}, specs)
		assert.Equal(t, "", next)
	}

	workSpecName := setWorkSpec(t, j, WorkSpecData)

	specs, next, err = j.ListWorkSpecs(map[string]interface{}{})
	if assert.NoError(t, err) {
		if assert.Len(t, specs, 1) {
			assert.Equal(t, workSpecName, specs[0]["name"])
		}
		assert.Equal(t, next, "")
	}
}

// TestClear verifies that Clear will remove work specs.
func TestClear(t *testing.T) {
	j := setUpTest(t, "TestClear")
	defer tearDownTest(t, j)

	specs, next, err := j.ListWorkSpecs(map[string]interface{}{})
	if assert.NoError(t, err) {
		assert.Equal(t, []map[string]interface{}{}, specs)
		assert.Equal(t, "", next)
	}

	workSpecName := setWorkSpec(t, j, WorkSpecData)

	specs, next, err = j.ListWorkSpecs(map[string]interface{}{})
	if assert.NoError(t, err) {
		if assert.Len(t, specs, 1) {
			assert.Equal(t, workSpecName, specs[0]["name"])
		}
		assert.Equal(t, next, "")
	}

	dropped, err := j.Clear()
	if assert.NoError(t, err) {
		assert.Equal(t, dropped, 1)
	}

	specs, next, err = j.ListWorkSpecs(map[string]interface{}{})
	if assert.NoError(t, err) {
		assert.Equal(t, []map[string]interface{}{}, specs)
		assert.Equal(t, "", next)
	}
}

// TestListWorkUnits tests state-based paths of the GetWorkUnits call.
func TestListWorkUnits(t *testing.T) {
	j := setUpTest(t, "TestListWorkUnits")
	defer tearDownTest(t, j)

	workUnits := map[string]map[string]interface{}{
		"foo":    {"length": 3},
		"foobar": {"length": 6},
	}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)

	// Initial check: both work units are there
	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)
	listWorkUnits(t, j, workSpecName, gwuDefault, workUnits)
	listWorkUnits(t, j, workSpecName, gwuFinished, noWorkUnits)

	// Start one unit; should still be there
	ok, spec, unit, data := getOneWork(t, j)
	if assert.True(t, ok) {
		assert.Equal(t, workSpecName, spec)
		if assert.Contains(t, workUnits, unit) {
			assert.Equal(t, workUnits[unit], data)
		}
	}
	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)
	listWorkUnits(t, j, workSpecName, gwuDefault, workUnits)
	listWorkUnits(t, j, workSpecName, gwuFinished, noWorkUnits)

	// Finish that unit; should be gone, the other should be there
	finishWorkUnit(t, j, workSpecName, unit, nil)
	available := map[string]map[string]interface{}{}
	finished := map[string]map[string]interface{}{}
	if unit == "foo" {
		available["foobar"] = workUnits["foobar"]
		finished["foo"] = workUnits["foo"]
	} else {
		available["foo"] = workUnits["foo"]
		finished["foobar"] = workUnits["foobar"]
	}
	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)
	listWorkUnits(t, j, workSpecName, gwuDefault, available)
	listWorkUnits(t, j, workSpecName, gwuFinished, finished)
}

// TestListWorkUnitsStartLimit validates a simple case of the
// GetWorkUnits "start" and "limit" parameters.
func TestListWorkUnitsStartLimit(t *testing.T) {
	j := setUpTest(t, "TestListWorkUnitsStartLimit")
	defer tearDownTest(t, j)

	workUnits := map[string]map[string]interface{}{
		"foo": {"length": 3},
		"bar": {"length": 6},
	}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)

	listWorkUnits(t, j, workSpecName, map[string]interface{}{
		"limit": 1,
	}, map[string]map[string]interface{}{
		"bar": {"length": 6},
	})

	listWorkUnits(t, j, workSpecName, map[string]interface{}{
		"start": "bar",
		"limit": 1,
	}, map[string]map[string]interface{}{
		"foo": {"length": 3},
	})

	listWorkUnits(t, j, workSpecName, map[string]interface{}{
		"start": "foo",
		"limit": 1,
	}, map[string]map[string]interface{}{})
}

func TestDelWorkUnitsSimple(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsSimple")
	defer tearDownTest(t, j)

	workUnits := map[string]map[string]interface{}{
		"foo": {"length": 3},
		"bar": {"length": 6},
	}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)

	count, msg, err := j.DelWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{"foo"}})
	if assert.NoError(t, err) {
		assert.Equal(t, 1, count)
		assert.Empty(t, msg)
	}
	listWorkUnits(t, j, workSpecName, gwuEverything, map[string]map[string]interface{}{"bar": {"length": 6}})
}

func prepareSomeOfEach(t *testing.T, j *jobserver.JobServer, n int) (workSpecName string, expected map[string]map[string]interface{}) {
	data := map[string]interface{}{"x": 1}
	expected = map[string]map[string]interface{}{}
	workSpecName = setWorkSpec(t, j, WorkSpecData)

	for _, name := range []string{"FA", "IL"}[:n] {
		addWorkUnit(t, j, workSpecName, name, data)
		getSpecificWork(t, j, workSpecName, name)
		ok, msg, err := j.UpdateWorkUnit(workSpecName, name, map[string]interface{}{"status": jobserver.Failed})
		if assert.NoError(t, err) {
			assert.True(t, ok)
			assert.Empty(t, msg)
		}
		expected[name] = data
	}

	for _, name := range []string{"FI", "NI"}[:n] {
		addWorkUnit(t, j, workSpecName, name, data)
		getSpecificWork(t, j, workSpecName, name)
		finishWorkUnit(t, j, workSpecName, name, nil)
		expected[name] = data
	}

	for _, name := range []string{"PE", "ND"}[:n] {
		addWorkUnit(t, j, workSpecName, name, data)
		getSpecificWork(t, j, workSpecName, name)
		expected[name] = data
	}

	for _, name := range []string{"AV", "AI"}[:n] {
		addWorkUnit(t, j, workSpecName, name, data)
		expected[name] = data
	}

	return
}

// delWorkUnitsBy is the core of the DelWorkUnits tests that expect to
// delet single work units.  It calls options(state) to get options
// to DelWorkUnits, and verifies that this deletes the single work unit
// associated with state.
func delWorkUnitsBy(t *testing.T, j *jobserver.JobServer, n int, state jobserver.WorkUnitStatus, options func(jobserver.WorkUnitStatus) map[string]interface{}) {
	workSpecName, expected := prepareSomeOfEach(t, j, n)
	delete(expected, stateShortName[state])

	count, msg, err := j.DelWorkUnits(workSpecName, options(state))
	if assert.NoError(t, err) {
		assert.Equal(t, 1, count)
		assert.Empty(t, msg)
	}
	listWorkUnits(t, j, workSpecName, gwuEverything, expected)

	_, err = j.Clear()
	assert.NoError(t, err)
}

// allDelWorkUnitsBy calls the delWorkUnitsBy core for all states.
func allDelWorkUnitsBy(t *testing.T, j *jobserver.JobServer, n int, options func(jobserver.WorkUnitStatus) map[string]interface{}) {
	for _, state := range allStates {
		delWorkUnitsBy(t, j, n, state, options)
	}
}

// TestDelWorkUnitsByName tests that deleting specific work units by
// their keys works.
func TestDelWorkUnitsByName(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByName")
	defer tearDownTest(t, j)

	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
		}
	}
	allDelWorkUnitsBy(t, j, 1, options)
}

// TestDelWorkUnitsByName2 creates 2 work units in each state and
// deletes one specific one by name.
func TestDelWorkUnitsByName2(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByName2")
	defer tearDownTest(t, j)

	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
		}
	}
	allDelWorkUnitsBy(t, j, 2, options)
}

// TestDelWorkUnitsByState tests that deleting specific work units by
// their current state works.
func TestDelWorkUnitsByState(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByState")
	defer tearDownTest(t, j)

	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"state": state,
		}
	}
	allDelWorkUnitsBy(t, j, 1, options)
}

// TestDelWorkUnitsByState2 creates two work units in each state, then
// deletes the pair by state.
func TestDelWorkUnitsByState2(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByState2")
	defer tearDownTest(t, j)

	for _, state := range allStates {
		workSpecName, expected := prepareSomeOfEach(t, j, 2)
		delete(expected, stateShortName[state])
		delete(expected, stateShortName2[state])

		count, msg, err := j.DelWorkUnits(workSpecName, map[string]interface{}{"state": state})
		if assert.NoError(t, err) {
			assert.Equal(t, 2, count)
			assert.Empty(t, msg)
		}
		listWorkUnits(t, j, workSpecName, gwuEverything, expected)

		_, err = j.Clear()
		assert.NoError(t, err)
	}
}

// TestDelWorkUnitsByNameAndState tests that deleting specific work
// units by specifying both their name and current state works.
func TestDelWorkUnitsByNameAndState(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByNameAndState")
	defer tearDownTest(t, j)

	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
			"state":          state,
		}
	}
	allDelWorkUnitsBy(t, j, 1, options)
}

// TestDelWorkUnitsByNameAndState2 creates two work units in each
// state, and deletes specific ones by specifying both their name and
// current state.
func TestDelWorkUnitsByNameAndState2(t *testing.T) {
	j := setUpTest(t, "TestDelWorkUnitsByNameAndState2")
	defer tearDownTest(t, j)

	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
			"state":          state,
		}
	}
	allDelWorkUnitsBy(t, j, 2, options)
}

// TestRegenerate verifies that getting work lets us resubmit the work
// spec.
func TestRegenerate(t *testing.T) {
	j := setUpTest(t, "TestRegenerate")
	defer tearDownTest(t, j)

	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnit(t, j, workSpecName, "one",
		map[string]interface{}{"number": 1})

	getSpecificWork(t, j, workSpecName, "one")
	wsn := setWorkSpec(t, j, WorkSpecData)
	assert.Equal(t, workSpecName, wsn)
	addWorkUnit(t, j, workSpecName, "two",
		map[string]interface{}{"number": 2})
	finishWorkUnit(t, j, workSpecName, "one", nil)

	doOneWork(t, j, workSpecName, "two")
	doNoWork(t, j)
}

// TestBinaryWorkUnit tests that arbitrary work unit keys in various
// combinations are accepted, even if they are not valid UTF-8.
func TestBinaryWorkUnit(t *testing.T) {
	j := setUpTest(t, "TestBinaryWorkUnit")
	defer tearDownTest(t, j)

	workUnits := map[string]map[string]interface{}{
		"\x00":             {"k": "\x00", "t": "single null"},
		"\x00\x01\x02\x03": {"k": "\x00\x01\x02\x03", "t": "control chars"},
		"\x00a\x00b":       {"k": "\x00a\x00b", "t": "UTF-16BE"},
		"a\x00b\x00":       {"k": "a\x00b\x00", "t": "UTF-16LE"},
		"f\xc3\xbc":        {"k": "f\xc3\xbc", "t": "UTF-8"},
		"f\xfc":            {"k": "f\xfc", "t": "ISO-8859-1"},
		"\xf0\x0f":         {"k": "\xf0\x0f", "t": "F00F"},
		"\xff":             {"k": "\xff", "t": "FF"},
		"\xff\x80":         {"k": "\xff\x80", "t": "FF80"},
	}

	// Add those work units
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)
	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)

	completed := map[string]struct{}{}
	for len(completed) < len(workUnits) {
		ok, spec, unit, data := getOneWork(t, j)
		if assert.True(t, ok) {
			assert.Equal(t, workSpecName, spec)
			if assert.Contains(t, workUnits, unit) {
				assert.Equal(t, workUnits[unit], data)
			}
			assert.NotContains(t, completed, unit)
			completed[unit] = struct{}{}
			finishWorkUnit(t, j, spec, unit, nil)
		}
	}

	doNoWork(t, j)
	listWorkUnits(t, j, workSpecName, gwuDefault, noWorkUnits)
	listWorkUnits(t, j, workSpecName, gwuFinished, workUnits)
}

// TestWorkUnitValue tests that the various supported types of data can
// be stored in a work unit.
func TestWorkUnitValue(t *testing.T) {
	j := setUpTest(t, "TestWorkUnitValue")
	defer tearDownTest(t, j)

	aUUID, err := uuid.FromString("01234567-89ab-cdef-0123-456789abcdef")
	if !assert.NoError(t, err) {
		return
	}
	workUnits := map[string]map[string]interface{}{
		"k": {
			"list":     []interface{}{1, 2, 3},
			"tuple":    cborrpc.PythonTuple{Items: []interface{}{4, 5, 6}},
			"mixed":    []interface{}{1, cborrpc.PythonTuple{Items: []interface{}{2, []interface{}{3, 4}}}},
			"uuid":     aUUID,
			"str":      []byte("foo"),
			"unicode":  "foo",
			"unicode2": "fü",
		},
	}
	workSpecName := setWorkSpec(t, j, WorkSpecData)
	addWorkUnits(t, j, workSpecName, workUnits)
	listWorkUnits(t, j, workSpecName, gwuEverything, workUnits)
}
