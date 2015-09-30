package jobserver_test

// This file has ports of the assorted interesting Python-based tests
// from https://github.com/diffeo/coordinate/coordinate/test.

import (
	"flag"
	"github.com/dmaze/goordinate/backend"
	"github.com/dmaze/goordinate/cborrpc"
	"github.com/dmaze/goordinate/coordinate"
	"github.com/dmaze/goordinate/jobserver"
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

// SetUpSuite does one-time setup for the entire test suite.
func (s *PythonSuite) SetUpSuite(c *check.C) {
	// The work spec every test here uses is constant, but a map
	// with constant keys and values isn't compile-time constant
	// according to Go rules apparently.  :-/
	s.WorkSpec = map[string]interface{}{
		"name":         "test_job_client",
		"min_gb":       1,
		"module":       "coordinate.tests.test_job_client",
		"run_function": "run_function",
	}
}

func (s *PythonSuite) SetUpTest(c *check.C) {
	var err error
	s.Namespace, err = s.Coordinate.Namespace(c.TestName())
	if err != nil {
		c.Error(err)
		return
	}
	s.JobServer = jobserver.JobServer{Namespace: s.Namespace}
}

func (s *PythonSuite) TearDownTest(c *check.C) {
	err := s.Namespace.Destroy()
	if err != nil {
		c.Error(err)
	}
	s.Namespace = nil
	s.JobServer.Namespace = nil
}

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
	c.Check(tuple.Items[0], check.DeepEquals, workUnitKey)
	result, ok := tuple.Items[1].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return result
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
	workUnitKey, ok = tuple.Items[1].(string)
	c.Assert(ok, check.Equals, true)
	workUnitData, ok = tuple.Items[2].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return
}

// Tests from test_job_client.py

// DoWork runs through a basic sequence of creating a work spec,
// adding a work unit to it, and running it.  There are several
// validity checks along the way.  Returns the final value of the work
// unit state after running the test, or calls c.Assert() (e.g.,
// panics) if this becomes impossible.
func (s *PythonSuite) DoWork(c *check.C, key string, data map[string]interface{}) map[string]interface{} {
	var (
		// Various return values
		ok   bool
		msg  string
		err  error
		list []interface{}
		dict map[string]interface{}
		// Return values from getOneWork
		wuSpec string
		wuKey  string
		wuData map[string]interface{}
		// workSpecName is workSpec["name"], extracted just once
		workSpecName string
		// keyDataPair is the pair (key, data)
		keyDataPair cborrpc.PythonTuple
		// keyDataList is the single-item list containing
		// keyDataPair, e.g. [(key, data)]
		keyDataList []interface{}
	)

	keyDataPair = cborrpc.PythonTuple{Items: []interface{}{key, data}}
	keyDataList = []interface{}{keyDataPair}

	workSpecName = s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, key, data)

	dict = s.getOneWorkUnit(c, workSpecName, key)
	c.Check(dict, check.DeepEquals, data)
	s.checkWorkUnitStatus(c, workSpecName, key, jobserver.Available)

	ok, wuSpec, wuKey, wuData = s.getOneWork(c)
	c.Assert(ok, check.Equals, true)
	c.Check(wuSpec, check.Equals, workSpecName)
	c.Check(wuKey, check.Equals, key)
	c.Check(wuData, check.DeepEquals, data)

	list, msg, err = s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{key}})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(list, check.DeepEquals, keyDataList)

	s.checkWorkUnitStatus(c, workSpecName, key, jobserver.Pending)

	// This "runs" the work unit
	wuData["output"] = map[string]interface{}{
		"foo": map[string]interface{}{"bar": "baz"},
	}

	ok, msg, err = s.JobServer.UpdateWorkUnit(workSpecName, key, map[string]interface{}{"data": wuData, "status": jobserver.Finished})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

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
	})
}

// Skipping TestArgs and TestKwargs: these test specific behaviors of
// the Python WorkUnit.run() call and how it invokes run_function,
// which are out of scope here

// TestPause validates that pausing and unpausing a work spec affect
// what GetWork returns.
func (s *PythonSuite) TestPause(c *check.C) {
	workSpecName := s.setWorkSpec(c, s.WorkSpec)
	s.addWorkUnit(c, workSpecName, "u", map[string]interface{}{"k": "v"})

	list, msg, err := s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(list, check.DeepEquals, []interface{}{
		cborrpc.PythonTuple{Items: []interface{}{
			"u",
			map[string]interface{}{"k": "v"},
		}},
	})
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
