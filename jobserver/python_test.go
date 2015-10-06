package jobserver_test

// This file has ports of the assorted interesting Python-based tests
// from https://github.com/diffeo/coordinate/coordinate/test.

import (
	"flag"
	"fmt"
	"github.com/dmaze/goordinate/backend"
	"github.com/dmaze/goordinate/cborrpc"
	"github.com/dmaze/goordinate/coordinate"
	"github.com/dmaze/goordinate/jobserver"
	"github.com/satori/go.uuid"
	"gopkg.in/check.v1"
	"os"
	"testing"
)

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

// TestMain is called from the command line.
func TestMain(m *testing.M) {
	backend := backend.Backend{Implementation: "memory"}
	flag.Var(&backend, "backend", "impl:address of coordinate storage")
	flag.Parse()
	c, err := backend.Coordinate()
	if err != nil {
		panic(err)
	}
	check.Suite(&PythonSuite{Coordinate: c})
	os.Exit(m.Run())
}

// PythonSuite collects together the Python-based tests.
type PythonSuite struct {
	// Coordinate contains the top-level interface to the backend
	// for the job server.
	Coordinate coordinate.Coordinate

	// Namespace contains the namespace object for the current test.
	Namespace coordinate.Namespace

	// JobServer contains the job server object under test.
	JobServer jobserver.JobServer

	// WorkSpec contains the reference work spec.
	WorkSpec map[string]interface{}
}

func (s *PythonSuite) SetUpTest(c *check.C) {
	var err error
	s.Namespace, err = s.Coordinate.Namespace(c.TestName())
	if err != nil {
		c.Error(err)
		return
	}
	s.JobServer = jobserver.JobServer{Namespace: s.Namespace}
	// Reset the "default" work spec for every test; some modify it
	s.WorkSpec = map[string]interface{}{
		"name":         "test_job_client",
		"min_gb":       1,
		"module":       "coordinate.tests.test_job_client",
		"run_function": "run_function",
	}
}

func (s *PythonSuite) TearDownTest(c *check.C) {
	err := s.Namespace.Destroy()
	if err != nil {
		c.Error(err)
	}
	s.Namespace = nil
	s.JobServer.Namespace = nil
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

// setWorkSpec calls the eponymous JobServer function, checking that
// it ran successfully, and returns the work spec name on success.
func (s *PythonSuite) setWorkSpec(c *check.C, workSpec map[string]interface{}) string {
	ok, msg, err := s.JobServer.SetWorkSpec(workSpec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	workSpecName, ok := workSpec["name"].(string)
	c.Assert(ok, check.Equals, true)
	return workSpecName
}

// addWorkUnit packages a single work unit key and data dictionary
// into the tuple format JobServer expects, and calls AddWorkUnits(),
// checking the result.
func (s *PythonSuite) addWorkUnit(c *check.C, workSpecName, key string, data map[string]interface{}) {
	keyDataPair := cborrpc.PythonTuple{Items: []interface{}{key, data}}
	keyDataList := []interface{}{keyDataPair}
	ok, msg, err := s.JobServer.AddWorkUnits(workSpecName, keyDataList)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")
}

// addWorkUnits adds a batch of work units to the system in one call.
func (s *PythonSuite) addWorkUnits(c *check.C, workSpecName string, workUnits map[string]map[string]interface{}) {
	// Assemble the parameters to AddWorkUnits as one big list of
	// pairs of (key, data)
	var awu []interface{}
	for name, data := range workUnits {
		pair := []interface{}{name, data}
		awu = append(awu, pair)
	}
	ok, msg, err := s.JobServer.AddWorkUnits(workSpecName, awu)
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Assert(ok, check.Equals, true)
}

// prioritizeWorkUnit changes the priority of a single work unit.
func (s *PythonSuite) prioritizeWorkUnit(c *check.C, workSpecName, key string, priority int, adjust bool) {
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
	ok, msg, err := s.JobServer.PrioritizeWorkUnits(workSpecName, options)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")
}

// addPrefixedWorkUnits adds a series of similarly-named work units
// to a work spec.  If prefix is "u", it adds count work units named
// u001, u002, ....  The work spec dictionaries have a single key "k"
// with values v1, v2, ....
func (s *PythonSuite) addPrefixedWorkUnits(c *check.C, workSpecName, prefix string, count int) {
	workUnitKvps := make([]interface{}, count)
	for i := range workUnitKvps {
		key := fmt.Sprintf("%s%03d", prefix, i+1)
		data := map[string]interface{}{"k": fmt.Sprintf("v%v", i+1)}
		items := []interface{}{key, data}
		workUnitKvps[i] = cborrpc.PythonTuple{Items: items}
	}
	ok, msg, err := s.JobServer.AddWorkUnits(workSpecName, workUnitKvps)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")
}

// expectPrefixedWorkUnits returns a list of expected return values from
// the GetWork call, if the returns are the first count work units from
// workSpecName with prefix prefix.
func (s *PythonSuite) expectPrefixedWorkUnits(workSpecName, prefix string, count int) interface{} {
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
func (s *PythonSuite) getOneWorkUnit(c *check.C, workSpecName, workUnitKey string) map[string]interface{} {
	list, msg, err := s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{workUnitKey}})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	if len(list) == 0 {
		return nil
	}
	c.Check(list, check.HasLen, 1)
	tuple, ok := list[0].(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Check(tuple.Items, check.HasLen, 2)
	c.Check(tuple.Items[0], check.DeepEquals, []byte(workUnitKey))
	result, ok := tuple.Items[1].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return result
}

// listWorkUnits calls GetWorkUnits (as the similarly-named Python
// function does) and validates that the response matches an expected
// set of work units.
func (s *PythonSuite) listWorkUnits(c *check.C, workSpecName string, options map[string]interface{}, expected map[string]map[string]interface{}) {
	gwu, msg, err := s.JobServer.GetWorkUnits(workSpecName, options)
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	missing := make(map[string]struct{})
	for name := range expected {
		missing[name] = struct{}{}
	}
	for _, item := range gwu {
		tuple, ok := item.(cborrpc.PythonTuple)
		c.Assert(ok, check.Equals, true)
		c.Assert(tuple.Items, check.HasLen, 2)
		bName, ok := tuple.Items[0].([]byte)
		c.Assert(ok, check.Equals, true)
		name := string(bName)
		exData, ok := expected[name]
		c.Assert(ok, check.Equals, true,
			check.Commentf("unexpected work unit %v", name))
		_, ok = missing[name]
		c.Assert(ok, check.Equals, true,
			check.Commentf("duplicate work unit %v", name))
		delete(missing, name)
		data, ok := tuple.Items[1].(map[string]interface{})
		c.Assert(ok, check.Equals, true)
		c.Check(data, check.DeepEquals, exData)
	}
}

// finishWorkUnit marks a specific work unit as finished.
func (s *PythonSuite) finishWorkUnit(c *check.C, workSpecName, workUnitKey string, data map[string]interface{}) {
	options := map[string]interface{}{
		"status": jobserver.Finished,
	}
	if data != nil {
		options["data"] = data
	}
	ok, msg, err := s.JobServer.UpdateWorkUnit(workSpecName, workUnitKey, options)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")
}

// checkWorkUnitStatus makes a weak assertion about a specific work
// unit's status by calling GetWorkUnitStatus for it.
func (s *PythonSuite) checkWorkUnitStatus(c *check.C, workSpecName, workUnitKey string, status jobserver.WorkUnitStatus) {
	dicts, msg, err := s.JobServer.GetWorkUnitStatus(workSpecName, []string{workUnitKey})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(dicts, check.HasLen, 1)
	if len(dicts) > 0 {
		c.Check(dicts[0]["status"], check.Equals, status)
	}
}

func (s *PythonSuite) checkChildWorkUnits(c *check.C, parent, child, workSpecName string, expected map[string]map[string]interface{}) {
	missing := make(map[string]struct{})
	for name := range expected {
		missing[name] = struct{}{}
	}
	units, msg, err := s.JobServer.GetChildWorkUnits(parent)
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	childUnits, ok := units[child]
	c.Assert(ok, check.Equals, true,
		check.Commentf("no %v worker in child work units", child))
	for _, unit := range childUnits {
		c.Check(unit["worker_id"], check.Equals, child)
		c.Check(unit["work_spec_name"], check.Equals, workSpecName)
		c.Check(unit["work_unit_key"], check.FitsTypeOf, []byte{})
		bKey, ok := unit["work_unit_key"].([]byte)
		if ok {
			name := string(bKey)
			data, ok := expected[name]
			c.Check(ok, check.Equals, true,
				check.Commentf("unexpected child work unit %v", name))
			if ok {
				c.Check(unit["work_unit_data"], check.DeepEquals, data)
			}
			_, ok = missing[name]
			c.Check(ok, check.Equals, true,
				check.Commentf("duplicate child work unit %v", name))
			delete(missing, name)
		}
	}
	c.Check(missing, check.DeepEquals, map[string]struct{}{})
}

func (s *PythonSuite) getOneWork(c *check.C) (ok bool, workSpecName, workUnitKey string, workUnitData map[string]interface{}) {
	anything, msg, err := s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	// Since we didn't request multiple work units we should always
	// get at most one, but maybe none
	c.Assert(anything, check.NotNil)
	tuple, ok := anything.(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Assert(tuple.Items, check.HasLen, 3)
	// "no work unit" gets returned as tuple (nil, nil, nil)
	if tuple.Items[0] == nil {
		ok = false
		return
	}
	workSpecName, ok = tuple.Items[0].(string)
	c.Assert(ok, check.Equals, true)
	bWorkUnitKey, ok := tuple.Items[1].([]byte)
	c.Assert(ok, check.Equals, true)
	workUnitKey = string(bWorkUnitKey)
	workUnitData, ok = tuple.Items[2].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return
}

// getSpecificWork calls GetWork expecting a specific work unit to
// come back, and returns its data dictionary.
func (s *PythonSuite) getSpecificWork(c *check.C, workSpecName, workUnitKey string) map[string]interface{} {
	anything, msg, err := s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Assert(anything, check.NotNil)
	tuple, ok := anything.(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Assert(tuple.Items, check.HasLen, 3)
	c.Assert(tuple.Items[0], check.NotNil)
	c.Check(tuple.Items[0], check.DeepEquals, workSpecName)
	c.Check(tuple.Items[1], check.DeepEquals, []byte(workUnitKey))
	workUnitData, ok := tuple.Items[2].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return workUnitData
}

func (s *PythonSuite) doOneWork(c *check.C, workSpecName, workUnitKey string) {
	ok, gotSpec, gotKey, _ := s.getOneWork(c)
	c.Check(ok, check.Equals, true)
	if ok {
		c.Check(gotSpec, check.Equals, workSpecName)
		c.Check(gotKey, check.Equals, workUnitKey)
		s.finishWorkUnit(c, workSpecName, workUnitKey, nil)
	}
}

func (s *PythonSuite) doNoWork(c *check.C) {
	ok, gotSpec, gotKey, _ := s.getOneWork(c)
	c.Check(ok, check.Equals, false,
		check.Commentf("got work spec %v key %v", gotSpec, gotKey))
}

// Tests from test_job_client.py

// DoWork runs through a basic sequence of creating a work spec,
// adding a work unit to it, and running it.  There are several
// validity checks along the way.  Returns the final value of the work
// unit state after running the test, or calls c.Assert() (e.g.,
// panics) if this becomes impossible.
func (s *PythonSuite) DoWork(c *check.C, key string, data map[string]interface{}) map[string]interface{} {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, key, data)

	dict := s.getOneWorkUnit(c, workSpecName, key)
	c.Check(dict, check.DeepEquals, data)
	s.checkWorkUnitStatus(c, workSpecName, key, jobserver.Available)

	wuData := s.getSpecificWork(c, workSpecName, key)
	c.Check(wuData, check.DeepEquals, data)

	s.listWorkUnits(c, workSpecName,
		map[string]interface{}{"work_unit_keys": []interface{}{key}},
		map[string]map[string]interface{}{key: data})

	s.checkWorkUnitStatus(c, workSpecName, key, jobserver.Pending)

	// This "runs" the work unit
	wuData["output"] = map[string]interface{}{
		"foo": map[string]interface{}{"bar": "baz"},
	}
	wuData["args"] = cborrpc.PythonTuple{Items: []interface{}{"arg"}}

	s.finishWorkUnit(c, workSpecName, key, wuData)
	s.checkWorkUnitStatus(c, workSpecName, key, jobserver.Finished)
	dict = s.getOneWorkUnit(c, workSpecName, key)
	c.Assert(dict, check.NotNil)
	return dict
}

// TestDataUpdates runs through the full sequence of creating a work
// spec and work unit and running them, and verifies that the data
// dictionary did in fact get updated.
func (s *PythonSuite) TestDataUpdates(c *check.C) {
	res := s.DoWork(c, "u", map[string]interface{}{"k": "v"})
	c.Check(res, check.DeepEquals, map[string]interface{}{
		"k": "v",
		"output": map[string]interface{}{
			"foo": map[string]interface{}{
				"bar": "baz",
			},
		},
		"args": cborrpc.PythonTuple{
			Items: []interface{}{"arg"},
		},
	})
}

// Skipping TestArgs and TestKwargs: these test specific behaviors of
// the Python WorkUnit.run() call and how it invokes run_function,
// which are out of scope here

// TestPause validates that pausing and unpausing a work spec affect
// what GetWork returns.
func (s *PythonSuite) TestPause(c *check.C) {
	workUnits := map[string]map[string]interface{}{"u": {"k": "v"}}
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)

	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)
	s.checkWorkUnitStatus(c, workSpecName, "u", jobserver.Available)

	// Pause the work spec
	ok, msg, err := s.JobServer.ControlWorkSpec(workSpecName, map[string]interface{}{"status": jobserver.Paused})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// We should not get work now
	ok, spec, unit, _ := s.getOneWork(c)
	c.Check(ok, check.Equals, false)

	// Resume the work spec
	ok, msg, err = s.JobServer.ControlWorkSpec(workSpecName, map[string]interface{}{"status": jobserver.Runnable})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// We should the work unit back
	ok, spec, unit, _ = s.getOneWork(c)
	c.Check(ok, check.Equals, true)
	if ok {
		c.Check(spec, check.Equals, workSpecName)
		c.Check(unit, check.Equals, "u")
	}
}

// TestGetMany tests that the GetWork call with a "max_jobs" parameter
// actually retrieves the requested number of jobs.
func (s *PythonSuite) TestGetMany(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addPrefixedWorkUnits(c, workSpecName, "u", 100)

	anything, msg, err := s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")

	expected := s.expectPrefixedWorkUnits(workSpecName, "u", 10)
	c.Check(anything, check.DeepEquals, expected)
}

// TestGetManyMaxGetwork tests that the GetWork call with a "max_jobs"
// parameter actually retrieves the requested number of jobs.
func (s *PythonSuite) TestGetManyMaxGetwork(c *check.C) {
	s.WorkSpec["max_getwork"] = 5
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addPrefixedWorkUnits(c, workSpecName, "u", 100)

	anything, msg, err := s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")

	// Even though we requested 10 jobs, we should actually get 5
	// (e.g. the work spec's max_getwork)
	expected := s.expectPrefixedWorkUnits(workSpecName, "u", 5)
	c.Check(anything, check.DeepEquals, expected)
}

// TestGetTooMany tests what happens when there are two work specs,
// and the one that gets chosen has fewer work units than are requested.
// This test validates that the higher-weight work spec is chosen and
// that the GetWork call does not "spill" to retrieving work units from
// the other work spec.
func (s *PythonSuite) TestGetTooMany(c *check.C) {
	s.WorkSpec["weight"] = 1
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addPrefixedWorkUnits(c, workSpecName, "u", 100)

	otherWorkSpec := map[string]interface{}{
		"name":         "ws2",
		"min_gb":       0.1,
		"module":       "coordinate.tests.test_job_client",
		"run_function": "run_function",
		"priority":     2,
	}
	otherWorkSpecName := s.setWorkSpec(c, otherWorkSpec)
	s.addPrefixedWorkUnits(c, otherWorkSpecName, "z", 4)

	anything, msg, err := s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1, "lease_time": 300, "max_jobs": 10})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")

	// We requested 10 jobs.  Since ws2 has higher priority,
	// the scheduler should choose it.  But, that has only
	// 4 work units in it.  We should get exactly those.
	expected := s.expectPrefixedWorkUnits(otherWorkSpecName, "z", 4)
	c.Check(anything, check.DeepEquals, expected)
}

// TestPrioritize tests basic work unit prioritization.
func (s *PythonSuite) TestPrioritize(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "b", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Default order is alphabetical
	s.doOneWork(c, workSpecName, "a")

	// If we prioritize c, it should go first, before b
	s.prioritizeWorkUnit(c, workSpecName, "c", 1, false)

	s.doOneWork(c, workSpecName, "c")
	s.doOneWork(c, workSpecName, "b")
	s.doNoWork(c)
}

// TestPrioritizeAdjust tests using the "adjust" mode to change work unit
// priorities.
func (s *PythonSuite) TestPrioritizeAdjust(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "b", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Use "adjust" mode to adjust the priorities
	s.prioritizeWorkUnit(c, workSpecName, "a", 10, true)
	s.prioritizeWorkUnit(c, workSpecName, "b", 20, true)
	s.prioritizeWorkUnit(c, workSpecName, "c", 30, true)

	// Highest priority goes first
	s.doOneWork(c, workSpecName, "c")
	s.doOneWork(c, workSpecName, "b")
	s.doOneWork(c, workSpecName, "a")
	s.doNoWork(c)
}

// TestReprioritize tests that changing a work unit priority mid-stream
// correctly adjusts the work unit priorities.
func (s *PythonSuite) TestReprioritize(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "b", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "c", map[string]interface{}{"k": "v"})

	// Use "priority" mode to set the priorities
	s.prioritizeWorkUnit(c, workSpecName, "a", 10, false)
	s.prioritizeWorkUnit(c, workSpecName, "b", 20, false)
	s.prioritizeWorkUnit(c, workSpecName, "c", 30, false)

	// Highest priority goes first
	s.doOneWork(c, workSpecName, "c")

	// Now adjust "a" to have higher priority
	s.prioritizeWorkUnit(c, workSpecName, "a", 15, true) // +10 = 25

	s.doOneWork(c, workSpecName, "a")
	s.doOneWork(c, workSpecName, "b")
	s.doNoWork(c)
}

// TestSucceedFail tests that failing a finished work unit is a no-op.
// This can happen if a work unit finishes successfully just before its
// timeout, and its parent worker tries to kill it.
func (s *PythonSuite) TestSucceedFail(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})
	s.doOneWork(c, workSpecName, "a")

	// ...meanwhile, the parent nukes us from orbit
	ok, _, err := s.JobServer.UpdateWorkUnit(workSpecName, "a", map[string]interface{}{"status": jobserver.Failed})
	c.Assert(err, check.IsNil)
	// This should return "no-op"
	c.Check(ok, check.Equals, false)

	// The end status should be "succeeded"
	s.checkWorkUnitStatus(c, workSpecName, "a", jobserver.Finished)
}

// TestFailSucceed tests that finishing a failed work unit makes it
// finished.  This happens under the same conditions as
// TestSucceedFail, but with different timing.
func (s *PythonSuite) TestFailSucceed(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})

	// Get the work unit
	ok, gotSpec, gotKey, _ := s.getOneWork(c)
	c.Check(ok, check.Equals, true)
	if ok {
		c.Check(gotSpec, check.Equals, workSpecName)
		c.Check(gotKey, check.Equals, "a")
	}

	// Meanwhile, the parent nukes us from orbit
	ok, msg, err := s.JobServer.UpdateWorkUnit(workSpecName, "a", map[string]interface{}{"status": jobserver.Failed})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// But wait!  We actually did the job!
	s.finishWorkUnit(c, workSpecName, "a", nil)

	// The end status should be "succeeded"
	s.checkWorkUnitStatus(c, workSpecName, "a", jobserver.Finished)
}

// TestGetChildUnitsBasic verifies the GetChildWorkUnits call with a
// basic work flow.
func (s *PythonSuite) TestGetChildUnitsBasic(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})

	// register parent worker
	ok, msg, err := s.JobServer.WorkerHeartbeat("parent", 0, 6000, nil, "")
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// register child worker
	ok, msg, err = s.JobServer.WorkerHeartbeat("child", 0, 6000, nil, "parent")
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// right now there should be no child units
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, noWorkUnits)

	// get the work unit
	anything, msg, err := s.JobServer.GetWork("child", map[string]interface{}{"available_gb": 1})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Assert(anything, check.NotNil)
	tuple, ok := anything.(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Assert(tuple.Items, check.HasLen, 3)
	c.Check(tuple.Items[0], check.DeepEquals, workSpecName)
	c.Check(tuple.Items[1], check.DeepEquals, []byte("a"))
	c.Check(tuple.Items[2], check.DeepEquals, map[string]interface{}{"k": "v"})

	// it should be reported as a child unit
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, map[string]map[string]interface{}{"a": {"k": "v"}})

	// now finish it
	s.finishWorkUnit(c, workSpecName, "a", nil)

	// there should be no work units left now
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, noWorkUnits)
}

// TestGetChildUnitsMulti verifies the GetChildWorkUnits call when the child gets multiple work units.
func (s *PythonSuite) TestGetChildUnitsMulti(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "a", map[string]interface{}{"k": "v"})
	s.addWorkUnit(c, workSpecName, "b", map[string]interface{}{"k": "v"})

	// register parent worker
	ok, msg, err := s.JobServer.WorkerHeartbeat("parent", 0, 6000, nil, "")
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// register child worker
	ok, msg, err = s.JobServer.WorkerHeartbeat("child", 0, 6000, nil, "parent")
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// right now there should be no child units
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, noWorkUnits)

	// get the work units
	anything, msg, err := s.JobServer.GetWork("child", map[string]interface{}{"available_gb": 1, "max_jobs": 10})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Assert(anything, check.NotNil)
	list, ok := anything.([]cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Check(list, check.HasLen, 2)
	for i, tuple := range list {
		c.Assert(tuple.Items, check.HasLen, 3)
		c.Check(tuple.Items[0], check.DeepEquals, workSpecName)
		if i == 0 {
			c.Check(tuple.Items[1], check.DeepEquals, []byte("a"))
		} else {
			c.Check(tuple.Items[1], check.DeepEquals, []byte("b"))
		}
		c.Check(tuple.Items[2], check.DeepEquals, map[string]interface{}{"k": "v"})
	}

	// both should be reported as child units
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, map[string]map[string]interface{}{"a": {"k": "v"}, "b": {"k": "v"}})

	// finish "a"
	s.finishWorkUnit(c, workSpecName, "a", nil)

	// we should have "b" left
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, map[string]map[string]interface{}{"b": {"k": "v"}})

	// now finish b
	s.finishWorkUnit(c, workSpecName, "b", nil)

	// there should be no work units left now
	s.checkChildWorkUnits(c, "parent", "child", workSpecName, noWorkUnits)
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
func (s *PythonSuite) TestSimpleFlow(c *check.C) {
	s.setWorkSpec(c, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	s.setWorkSpec(c, map[string]interface{}{
		"name": "second",
	})
	s.addWorkUnit(c, "first", "u", map[string]interface{}{"k": "v"})

	data := s.getSpecificWork(c, "first", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{
		"k": "v",
	})
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"x": "y"},
	}
	s.finishWorkUnit(c, "first", "u", data)

	data = s.getSpecificWork(c, "second", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{
		"x": "y",
	})
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	s.finishWorkUnit(c, "second", "u", data)

	s.doNoWork(c)
}

// TestSimpleOutput verifies that the output of an earlier work spec
// can become a work unit in a later work spec.  The work unit "output"
// field is a flat list of work unit keys.
func (s *PythonSuite) TestSimpleOutput(c *check.C) {
	s.setWorkSpec(c, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	s.setWorkSpec(c, map[string]interface{}{
		"name": "second",
	})
	s.addWorkUnit(c, "first", "u", map[string]interface{}{"k": "v"})

	data := s.getSpecificWork(c, "first", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{
		"k": "v",
	})
	data["output"] = []interface{}{"u"}
	s.finishWorkUnit(c, "first", "u", data)

	data = s.getSpecificWork(c, "second", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{})
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	s.finishWorkUnit(c, "second", "u", data)

	s.doNoWork(c)
}

// TestSimpleOutputBytes is the same as TestSimpleOutput, but the "next"
// work unit name is a byte string.
func (s *PythonSuite) TestSimpleOutputBytes(c *check.C) {
	s.setWorkSpec(c, map[string]interface{}{
		"name": "first",
		"then": "second",
	})
	s.setWorkSpec(c, map[string]interface{}{
		"name": "second",
	})
	s.addWorkUnit(c, "first", "u", map[string]interface{}{"k": "v"})

	data := s.getSpecificWork(c, "first", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{
		"k": "v",
	})
	data["output"] = []interface{}{[]byte("u")}
	s.finishWorkUnit(c, "first", "u", data)

	data = s.getSpecificWork(c, "second", "u")
	c.Check(data, check.DeepEquals, map[string]interface{}{})
	data["output"] = map[string]interface{}{
		"u": map[string]interface{}{"mode": "foo"},
	}
	s.finishWorkUnit(c, "second", "u", data)

	s.doNoWork(c)
}

// Tests from test_task_master.py

// TestListWorkSpecs verifies that ListWorkSpecs will return one work
// spec when added.
func (s *PythonSuite) TestListWorkSpecs(c *check.C) {
	// Initial state is nothing
	specs, next, err := s.JobServer.ListWorkSpecs(map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(specs, check.DeepEquals, []map[string]interface{}{})
	c.Check(next, check.Equals, "")

	workSpecName := s.setWorkSpec(c, s.WorkSpec)

	specs, next, err = s.JobServer.ListWorkSpecs(map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(next, check.Equals, "")
	c.Check(specs, check.HasLen, 1)
	if len(specs) > 0 {
		c.Check(specs[0]["name"], check.Equals, workSpecName)
	}
}

// TestClear verifies that Clear will remove work specs.
func (s *PythonSuite) TestClear(c *check.C) {
	specs, next, err := s.JobServer.ListWorkSpecs(map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(specs, check.DeepEquals, []map[string]interface{}{})
	c.Check(next, check.Equals, "")

	workSpecName := s.setWorkSpec(c, s.WorkSpec)

	specs, next, err = s.JobServer.ListWorkSpecs(map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(next, check.Equals, "")
	c.Check(specs, check.HasLen, 1)
	if len(specs) > 0 {
		c.Check(specs[0]["name"], check.Equals, workSpecName)
	}

	dropped, err := s.JobServer.Clear()
	c.Assert(err, check.IsNil)
	c.Check(dropped, check.Equals, 1)

	specs, next, err = s.JobServer.ListWorkSpecs(map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(specs, check.DeepEquals, []map[string]interface{}{})
	c.Check(next, check.Equals, "")
}

// TestListWorkUnits tests state-based paths of the GetWorkUnits call.
func (s *PythonSuite) TestListWorkUnits(c *check.C) {
	workUnits := map[string]map[string]interface{}{
		"foo":    {"length": 3},
		"foobar": {"length": 6},
	}
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)

	// Initial check: both work units are there
	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)
	s.listWorkUnits(c, workSpecName, gwuDefault, workUnits)
	s.listWorkUnits(c, workSpecName, gwuFinished, noWorkUnits)

	// Start one unit; should still be there
	ok, spec, unit, data := s.getOneWork(c)
	c.Assert(ok, check.Equals, true)
	c.Check(spec, check.Equals, workSpecName)
	c.Check(workUnits[unit], check.NotNil)
	c.Check(data, check.DeepEquals, workUnits[unit])
	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)
	s.listWorkUnits(c, workSpecName, gwuDefault, workUnits)
	s.listWorkUnits(c, workSpecName, gwuFinished, noWorkUnits)

	// Finish that unit; should be gone, the other should be there
	s.finishWorkUnit(c, workSpecName, unit, nil)
	available := map[string]map[string]interface{}{}
	finished := map[string]map[string]interface{}{}
	if unit == "foo" {
		available["foobar"] = workUnits["foobar"]
		finished["foo"] = workUnits["foo"]
	} else {
		available["foo"] = workUnits["foo"]
		finished["foobar"] = workUnits["foobar"]
	}
	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)
	s.listWorkUnits(c, workSpecName, gwuDefault, available)
	s.listWorkUnits(c, workSpecName, gwuFinished, finished)
}

// TestListWorkUnitsStartLimit validates a simple case of the
// GetWorkUnits "start" and "limit" parameters.
func (s *PythonSuite) TestListWorkUnitsStartLimit(c *check.C) {
	workUnits := map[string]map[string]interface{}{
		"foo": {"length": 3},
		"bar": {"length": 6},
	}
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)

	s.listWorkUnits(c, workSpecName, map[string]interface{}{
		"limit": 1,
	}, map[string]map[string]interface{}{
		"bar": {"length": 6},
	})

	s.listWorkUnits(c, workSpecName, map[string]interface{}{
		"start": "bar",
		"limit": 1,
	}, map[string]map[string]interface{}{
		"foo": {"length": 3},
	})

	s.listWorkUnits(c, workSpecName, map[string]interface{}{
		"start": "foo",
		"limit": 1,
	}, map[string]map[string]interface{}{})
}

func (s *PythonSuite) TestDelWorkUnitsSimple(c *check.C) {
	workUnits := map[string]map[string]interface{}{
		"foo": {"length": 3},
		"bar": {"length": 6},
	}
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)

	count, msg, err := s.JobServer.DelWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{"foo"}})
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)
	c.Check(msg, check.Equals, "")
	s.listWorkUnits(c, workSpecName, gwuEverything, map[string]map[string]interface{}{"bar": {"length": 6}})
}

func (s *PythonSuite) prepareSomeOfEach(c *check.C, n int) (workSpecName string, expected map[string]map[string]interface{}) {
	data := map[string]interface{}{"x": 1}
	expected = map[string]map[string]interface{}{}
	workSpecName = s.setWorkSpec(c, s.WorkSpec)

	for _, name := range []string{"FA", "IL"}[:n] {
		s.addWorkUnit(c, workSpecName, name, data)
		s.getSpecificWork(c, workSpecName, name)
		ok, msg, err := s.JobServer.UpdateWorkUnit(workSpecName, name, map[string]interface{}{"status": jobserver.Failed})
		c.Assert(err, check.IsNil)
		c.Check(ok, check.Equals, true)
		c.Check(msg, check.Equals, "")
		expected[name] = data
	}

	for _, name := range []string{"FI", "NI"}[:n] {
		s.addWorkUnit(c, workSpecName, name, data)
		s.getSpecificWork(c, workSpecName, name)
		s.finishWorkUnit(c, workSpecName, name, nil)
		expected[name] = data
	}

	for _, name := range []string{"PE", "ND"}[:n] {
		s.addWorkUnit(c, workSpecName, name, data)
		s.getSpecificWork(c, workSpecName, name)
		expected[name] = data
	}

	for _, name := range []string{"AV", "AI"}[:n] {
		s.addWorkUnit(c, workSpecName, name, data)
		expected[name] = data
	}

	return
}

// delWorkUnitsBy is the core of the DelWorkUnits tests that expect to
// delet single work units.  It calls options(state) to get options
// to DelWorkUnits, and verifies that this deletes the single work unit
// associated with state.
func (s *PythonSuite) delWorkUnitsBy(c *check.C, n int, state jobserver.WorkUnitStatus, options func(jobserver.WorkUnitStatus) map[string]interface{}) {
	workSpecName, expected := s.prepareSomeOfEach(c, n)
	delete(expected, stateShortName[state])

	count, msg, err := s.JobServer.DelWorkUnits(workSpecName, options(state))
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 1)
	c.Check(msg, check.Equals, "")
	s.listWorkUnits(c, workSpecName, gwuEverything, expected)

	_, err = s.JobServer.Clear()
	c.Assert(err, check.IsNil)
}

// allDelWorkUnitsBy calls the delWorkUnitsBy core for all states.
func (s *PythonSuite) allDelWorkUnitsBy(c *check.C, n int, options func(jobserver.WorkUnitStatus) map[string]interface{}) {
	for _, state := range allStates {
		s.delWorkUnitsBy(c, n, state, options)
	}
}

// TestDelWorkUnitsByName tests that deleting specific work units by
// their keys works.
func (s *PythonSuite) TestDelWorkUnitsByName(c *check.C) {
	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
		}
	}
	s.allDelWorkUnitsBy(c, 1, options)
}

// TestDelWorkUnitsByName2 creates 2 work units in each state and
// deletes one specific one by name.
func (s *PythonSuite) TestDelWorkUnitsByName2(c *check.C) {
	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
		}
	}
	s.allDelWorkUnitsBy(c, 2, options)
}

// TestDelWorkUnitsByState tests that deleting specific work units by
// their current state works.
func (s *PythonSuite) TestDelWorkUnitsByState(c *check.C) {
	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"state": state,
		}
	}
	s.allDelWorkUnitsBy(c, 1, options)
}

// TestDelWorkUnitsByState2 creates two work units in each state, then
// deletes the pair by state.
func (s *PythonSuite) TestDelWorkUnitsByState2(c *check.C) {
	for _, state := range allStates {
		workSpecName, expected := s.prepareSomeOfEach(c, 2)
		delete(expected, stateShortName[state])
		delete(expected, stateShortName2[state])

		count, msg, err := s.JobServer.DelWorkUnits(workSpecName, map[string]interface{}{"state": state})
		c.Assert(err, check.IsNil)
		c.Check(count, check.Equals, 2)
		c.Check(msg, check.Equals, "")
		s.listWorkUnits(c, workSpecName, gwuEverything, expected)

		_, err = s.JobServer.Clear()
		c.Assert(err, check.IsNil)
	}
}

// TestDelWorkUnitsByNameAndState tests that deleting specific work
// units by specifying both their name and current state works.
func (s *PythonSuite) TestDelWorkUnitsByNameAndState(c *check.C) {
	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
			"state":          state,
		}
	}
	s.allDelWorkUnitsBy(c, 1, options)
}

// TestDelWorkUnitsByNameAndState2 creates two work units in each
// state, and deletes specific ones by specifying both their name and
// current state.
func (s *PythonSuite) TestDelWorkUnitsByNameAndState2(c *check.C) {
	options := func(state jobserver.WorkUnitStatus) map[string]interface{} {
		return map[string]interface{}{
			"work_unit_keys": []interface{}{stateShortName[state]},
			"state":          state,
		}
	}
	s.allDelWorkUnitsBy(c, 2, options)
}

// TestRegenerate verifies that getting work lets us resubmit the work
// spec.
func (s *PythonSuite) TestRegenerate(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "one",
		map[string]interface{}{"number": 1})

	s.getSpecificWork(c, workSpecName, "one")
	wsn := s.setWorkSpec(c, s.WorkSpec)
	c.Check(wsn, check.Equals, workSpecName)
	s.addWorkUnit(c, workSpecName, "two",
		map[string]interface{}{"number": 2})
	s.finishWorkUnit(c, workSpecName, "one", nil)

	s.doOneWork(c, workSpecName, "two")
	s.doNoWork(c)
}

// TestBinaryWorkUnit tests that arbitrary work unit keys in various
// combinations are accepted, even if they are not valid UTF-8.
func (s *PythonSuite) TestBinaryWorkUnit(c *check.C) {
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
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)
	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)

	completed := map[string]struct{}{}
	for len(completed) < len(workUnits) {
		ok, spec, unit, data := s.getOneWork(c)
		c.Assert(ok, check.Equals, true)
		c.Check(spec, check.Equals, workSpecName)
		expected, inWorkUnits := workUnits[unit]
		_, alreadyCompleted := completed[unit]
		c.Check(inWorkUnits, check.Equals, true)
		if inWorkUnits {
			c.Check(data, check.DeepEquals, expected)
		}
		c.Check(alreadyCompleted, check.Equals, false)
		completed[unit] = struct{}{}
		s.finishWorkUnit(c, spec, unit, nil)
	}

	s.doNoWork(c)
	s.listWorkUnits(c, workSpecName, gwuDefault, noWorkUnits)
	s.listWorkUnits(c, workSpecName, gwuFinished, workUnits)
}

// TestWorkUnitValue tests that the various supported types of data can
// be stored in a work unit.
func (s *PythonSuite) TestWorkUnitValue(c *check.C) {
	aUUID, err := uuid.FromString("01234567-89ab-cdef-0123-456789abcdef")
	c.Assert(err, check.IsNil)
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
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnits(c, workSpecName, workUnits)
	s.listWorkUnits(c, workSpecName, gwuEverything, workUnits)
}
