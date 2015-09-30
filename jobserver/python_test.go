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

// Tests from test_job_client.py

// DoWork runs through a basic sequence of creating a work spec,
// adding a work unit to it, and running it.  There are several
// validity checks along the way.  Returns the final value of the work
// unit state after running the test, or calls c.Assert() (e.g.,
// panics) if this becomes impossible.
func (s *PythonSuite) DoWork(c *check.C, key string, data map[string]interface{}) map[string]interface{} {
	var (
		// Various return values
		ok       bool
		msg      string
		err      error
		list     []interface{}
		dict     map[string]interface{}
		wuData   map[string]interface{}
		dicts    []map[string]interface{}
		tuple    cborrpc.PythonTuple
		anything interface{}
		// workSpecName is workSpec["name"], extracted just once
		workSpecName string
		// keyDataPair is the pair (key, data)
		keyDataPair cborrpc.PythonTuple
		// keyDataList is the single-item list containing
		// keyDataPair, e.g. [(key, data)]
		keyDataList []interface{}
	)

	workSpecName, ok = s.WorkSpec["name"].(string)
	c.Assert(ok, check.Equals, true)
	keyDataPair = cborrpc.PythonTuple{Items: []interface{}{key, data}}
	keyDataList = []interface{}{keyDataPair}

	ok, msg, err = s.JobServer.SetWorkSpec(s.WorkSpec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	ok, msg, err = s.JobServer.AddWorkUnits(workSpecName, keyDataList)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	list, msg, err = s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{key}})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(list, check.DeepEquals, keyDataList)

	dicts, msg, err = s.JobServer.GetWorkUnitStatus(workSpecName, []string{key})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(dicts, check.HasLen, 1)
	if len(dicts) > 0 {
		c.Check(dicts[0]["status"], check.Equals, jobserver.Available)
	}

	anything, msg, err = s.JobServer.GetWork("test", map[string]interface{}{"available_gb": 1})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	// Since we didn't request multiple work units we should always
	// get exactly one; and we must have succeeded in this to move on
	c.Assert(anything, check.NotNil)
	tuple, ok = anything.(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Assert(tuple.Items, check.HasLen, 3)
	// tuple.Items contains work spec name, work unit name, data
	c.Check(tuple.Items[0], check.DeepEquals, workSpecName)
	c.Check(tuple.Items[1], check.DeepEquals, key)
	wuData, ok = tuple.Items[2].(map[string]interface{})
	c.Assert(ok, check.Equals, true)

	list, msg, err = s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{key}})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(list, check.DeepEquals, keyDataList)

	dicts, msg, err = s.JobServer.GetWorkUnitStatus(workSpecName, []string{key})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(dicts, check.HasLen, 1)
	if len(dicts) > 0 {
		c.Check(dicts[0]["status"], check.Equals, jobserver.Pending)
	}

	// This "runs" the work unit
	wuData["output"] = map[string]interface{}{
		"foo": map[string]interface{}{"bar": "baz"},
	}

	ok, msg, err = s.JobServer.UpdateWorkUnit(workSpecName, key, map[string]interface{}{"data": wuData, "status": jobserver.Finished})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	dicts, msg, err = s.JobServer.GetWorkUnitStatus(workSpecName, []string{key})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(dicts, check.HasLen, 1)
	if len(dicts) > 0 {
		c.Check(dicts[0]["status"], check.Equals, jobserver.Finished)
	}

	list, msg, err = s.JobServer.GetWorkUnits(workSpecName, map[string]interface{}{"work_unit_keys": []interface{}{key}})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(list, check.HasLen, 1)
	// This final get_work_units call returns the output data, so we
	// must make strong asserts about its return value and type or
	// else there is nothing to return.
	tuple, ok = list[0].(cborrpc.PythonTuple)
	c.Assert(ok, check.Equals, true)
	c.Check(tuple.Items, check.HasLen, 2)
	c.Check(tuple.Items[0], check.DeepEquals, key)
	dict, ok = tuple.Items[1].(map[string]interface{})
	c.Assert(ok, check.Equals, true)
	return dict
}

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
