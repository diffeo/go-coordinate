// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package worker

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/memory"
	"golang.org/x/net/context"
	"gopkg.in/check.v1"
	"testing"
	"time"
)

type Suite struct {
	Clock     *clock.Mock
	Namespace coordinate.Namespace
	Worker    Worker
	Bit       bool
	GotWork   chan bool
	Finished  chan string
	Stop      chan struct{}
}

func init() {
	check.Suite(&Suite{})
}

func Test(t *testing.T) {
	check.TestingT(t)
}

func (s *Suite) SetUpTest(c *check.C) {
	s.Clock = clock.NewMock()
	backend := memory.NewWithClock(s.Clock)
	var err error
	s.Namespace, err = backend.Namespace(c.TestName())
	c.Assert(err, check.IsNil)
	s.Worker = Worker{
		Namespace: s.Namespace,
	}
	s.Bit = false
	s.GotWork = make(chan bool)
	s.Finished = make(chan string)
	s.Stop = make(chan struct{})

	s.Worker.Tasks = map[string]func(context.Context, []coordinate.Attempt){
		"sanity": func(ctx context.Context, attempts []coordinate.Attempt) {
			c.Assert(attempts, check.HasLen, 1)
			c.Assert(attempts[0].WorkUnit().Name(), check.Equals, "unit")
			c.Assert(attempts[0].WorkUnit().WorkSpec().Name(), check.Equals, "spec")
			s.Bit = true
			err := attempts[0].Finish(nil)
			c.Assert(err, check.IsNil)
		},

		"timeout": func(ctx context.Context, attempts []coordinate.Attempt) {
			c.Assert(attempts, check.HasLen, 1)
			var err error
			select {
			case <-ctx.Done():
				s.Bit = false
				err = attempts[0].Fail(nil)
				c.Assert(err, check.IsNil)
			case <-s.Stop:
				s.Bit = true
				err = attempts[0].Finish(nil)
				c.Assert(err, check.IsNil)
			}
		},
	}

}

func (s *Suite) BootstrapWorker(c *check.C) {
	s.Worker.setDefaults()
	err := s.Worker.bootstrap()
	c.Assert(err, check.IsNil)
}

func (s *Suite) CreateSpecAndUnit(c *check.C, task string) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":    "spec",
		"runtime": "go",
		"task":    task,
	})
	c.Assert(err, check.IsNil)

	_, err = spec.AddWorkUnit("unit", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	c.Assert(err, check.IsNil)
}

func (s *Suite) GoDoWork(c *check.C) {
	id := "child"
	worker, err := s.Namespace.Worker(id)
	c.Assert(err, check.IsNil)
	err = worker.SetParent(s.Worker.parentWorker)
	c.Assert(err, check.IsNil)
	s.Worker.childWorkers[id] = worker
	go s.Worker.doWork(id, context.Background(), s.GotWork, s.Finished)
}

func (s *Suite) GetWork(c *check.C, shouldHaveWork bool) {
	select {
	case work := <-s.GotWork:
		c.Check(work, check.Equals, shouldHaveWork)
	case <-s.Finished:
		c.Fatalf("got finished flag before gotWork")
	}
}

func (s *Suite) Finish(c *check.C) {
	select {
	case <-s.GotWork:
		c.Fatalf("got gotWork flag twice")
	case id := <-s.Finished:
		c.Check(id, check.Equals, "child")
	}
}

func (s *Suite) TestIdleChildren(c *check.C) {
	s.Worker.Concurrency = 2
	s.BootstrapWorker(c)

	w1 := s.Worker.getIdleChild()
	c.Assert(w1, check.Not(check.Equals), "")
	w2 := s.Worker.getIdleChild()
	c.Assert(w2, check.Not(check.Equals), "")
	w3 := s.Worker.getIdleChild()
	c.Assert(w3, check.Equals, "")
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
	c.Assert(s.Worker.childWorkers, check.HasLen, 2)

	s.Worker.returnIdleChild(w1)
	w4 := s.Worker.getIdleChild()
	c.Assert(w4, check.Equals, w1)
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
	c.Assert(s.Worker.childWorkers, check.HasLen, 2)

	s.Worker.returnIdleChild(w1)
	s.Worker.returnIdleChild(w2)
	c.Assert(s.Worker.idleWorkers, check.HasLen, 2)
	c.Assert(s.Worker.childWorkers, check.HasLen, 2)
}

func (s *Suite) TestIdleChildrenIdleSystem(c *check.C) {
	s.Worker.Concurrency = 2
	s.BootstrapWorker(c)

	w1 := s.Worker.getIdleChild()
	c.Assert(w1, check.Not(check.Equals), "")
	w2 := s.Worker.getIdleChild()
	c.Assert(w2, check.Not(check.Equals), "")
	w3 := s.Worker.getIdleChild()
	c.Assert(w3, check.Equals, "")
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
	c.Assert(s.Worker.childWorkers, check.HasLen, 2)

	s.Worker.systemIdle = true

	s.Worker.returnIdleChild(w1)
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
	c.Assert(s.Worker.childWorkers, check.HasLen, 1)

	s.Worker.returnIdleChild(w2)
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
	c.Assert(s.Worker.idleWorkers, check.HasLen, 0)
}

func (s *Suite) TestDoNoWork(c *check.C) {
	s.BootstrapWorker(c)

	s.GoDoWork(c)
	s.GetWork(c, false)
	s.Finish(c)
}

func (s *Suite) TestDoOneWork(c *check.C) {
	s.CreateSpecAndUnit(c, "sanity")
	s.BootstrapWorker(c)

	c.Assert(s.Bit, check.Equals, false)

	s.GoDoWork(c)
	s.GetWork(c, true)
	s.Finish(c)

	c.Assert(s.Bit, check.Equals, true)

	s.GoDoWork(c)
	s.GetWork(c, false)
	s.Finish(c)
}

func (s *Suite) TestHeartbeat(c *check.C) {
	s.BootstrapWorker(c)

	s.Worker.heartbeat()

	worker, err := s.Namespace.Worker(s.Worker.WorkerID)
	c.Assert(err, check.IsNil)
	data, err := worker.Data()
	c.Assert(err, check.IsNil)
	c.Check(data["cpus"], check.NotNil)
	c.Check(data["go"], check.NotNil)
	c.Check(data["goroutines"], check.NotNil)
	c.Check(data["pid"], check.NotNil)
}

func (s *Suite) TestNonExpiration(c *check.C) {
	s.CreateSpecAndUnit(c, "timeout")
	s.BootstrapWorker(c)

	s.GoDoWork(c)
	s.GetWork(c, true)

	// Non-test: if we signal the "stop" flag, then this should finish
	s.Stop <- struct{}{}
	s.Finish(c)

	c.Check(s.Bit, check.Equals, true)

	spec, err := s.Namespace.WorkSpec("spec")
	c.Assert(err, check.IsNil)
	unit, err := spec.WorkUnit("unit")
	c.Assert(err, check.IsNil)
	status, err := unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.FinishedUnit)
}

func (s *Suite) TestExpirationCooperating(c *check.C) {
	s.CreateSpecAndUnit(c, "timeout")
	s.BootstrapWorker(c)

	s.GoDoWork(c)
	s.GetWork(c, true)

	// Push the clock up into the range where the work unit will
	// be considered almost dead, and so findStaleUnits() will flag
	// it, but not so late that coordinate will expire it on its own
	s.Clock.Add(14*time.Minute + 40*time.Second)
	s.Worker.findStaleUnits()

	// That should have caused the unit to clean itself up
	s.Finish(c)

	c.Check(s.Bit, check.Equals, false)

	spec, err := s.Namespace.WorkSpec("spec")
	c.Assert(err, check.IsNil)
	unit, err := spec.WorkUnit("unit")
	c.Assert(err, check.IsNil)
	status, err := unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.FailedUnit)
}

func (s *Suite) TestExpirationIgnoring(c *check.C) {
	s.CreateSpecAndUnit(c, "timeout")
	s.BootstrapWorker(c)

	s.GoDoWork(c)
	s.GetWork(c, true)

	// Push the clock up into the range where the work unit will
	// be considered even closer to dead, to the point where it is
	// assumed the worker has stopped caring
	s.Clock.Add(14*time.Minute + 50*time.Second)
	s.Worker.findStaleUnits()

	// Now the unit should be failed
	spec, err := s.Namespace.WorkSpec("spec")
	c.Assert(err, check.IsNil)
	unit, err := spec.WorkUnit("unit")
	c.Assert(err, check.IsNil)
	status, err := unit.Status()
	c.Assert(err, check.IsNil)
	c.Check(status, check.Equals, coordinate.FailedUnit)

	// Run the rest of the cleanup anyways
	s.Finish(c)
	c.Check(s.Bit, check.Equals, false)
}
