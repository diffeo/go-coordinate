// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package worker

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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

func (s *Suite) SetUpTest(t *testing.T) {
	s.Clock = clock.NewMock()
	backend := memory.NewWithClock(s.Clock)
	var err error
	s.Namespace, err = backend.Namespace("")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	s.Worker = Worker{
		Namespace: s.Namespace,
	}
	s.Bit = false
	s.GotWork = make(chan bool)
	s.Finished = make(chan string)
	s.Stop = make(chan struct{})

	s.Worker.Tasks = map[string]func(context.Context, []coordinate.Attempt){
		"sanity": func(ctx context.Context, attempts []coordinate.Attempt) {
			if assert.Len(t, attempts, 1) {
				assert.Equal(t, "unit", attempts[0].WorkUnit().Name())
				assert.Equal(t, "spec", attempts[0].WorkUnit().WorkSpec().Name())
				s.Bit = true
				err := attempts[0].Finish(nil)
				assert.NoError(t, err, "finishing attempt in sanity")
			}
		},

		"timeout": func(ctx context.Context, attempts []coordinate.Attempt) {
			if !assert.Len(t, attempts, 1) {
				return
			}
			select {
			case <-ctx.Done():
				s.Bit = false
				status, err := attempts[0].Status()
				if assert.NoError(t, err) && status == coordinate.Pending {
					err = attempts[0].Fail(nil)
					assert.NoError(t, err, "failing attempt in timeout (status=%v)", status)
				}
			case <-s.Stop:
				s.Bit = true
				status, err := attempts[0].Status()
				if assert.NoError(t, err) && status == coordinate.Pending {
					err = attempts[0].Finish(nil)
					assert.NoError(t, err, "finishing attempt in timeout (status=%v)", status)
				}
			}
		},
	}

}

func (s *Suite) BootstrapWorker(t *testing.T) {
	s.Worker.setDefaults()
	err := s.Worker.bootstrap()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}

func (s *Suite) CreateSpecAndUnit(t *testing.T, task string) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name":    "spec",
		"runtime": "go",
		"task":    task,
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = spec.AddWorkUnit("unit", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}

func (s *Suite) GoDoWork(t *testing.T) {
	id := "child"
	worker, err := s.Namespace.Worker(id)
	if assert.NoError(t, err) {
		err = worker.SetParent(s.Worker.parentWorker)
		if assert.NoError(t, err) {
			s.Worker.childWorkers[id] = worker
			go s.Worker.doWork(id, context.Background(),
				s.GotWork, s.Finished)
		}
	}
}

func (s *Suite) GetWork(t *testing.T, shouldHaveWork bool) {
	select {
	case work := <-s.GotWork:
		assert.Equal(t, shouldHaveWork, work)
	case <-s.Finished:
		assert.Fail(t, "got finished flag before gotWork")
	}
}

func (s *Suite) Finish(t *testing.T) {
	select {
	case <-s.GotWork:
		assert.Fail(t, "got gotWork flag twice")
	case id := <-s.Finished:
		assert.Equal(t, "child", id)
	}
}

func TestIdleChildren(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.Worker.Concurrency = 2
	s.BootstrapWorker(t)

	w1 := s.Worker.getIdleChild()
	assert.NotEmpty(t, w1)
	w2 := s.Worker.getIdleChild()
	assert.NotEmpty(t, w2)
	w3 := s.Worker.getIdleChild()
	assert.Empty(t, w3)
	assert.Len(t, s.Worker.idleWorkers, 0)
	assert.Len(t, s.Worker.childWorkers, 2)

	s.Worker.returnIdleChild(w1)
	w4 := s.Worker.getIdleChild()
	assert.Equal(t, w1, w4)
	assert.Len(t, s.Worker.idleWorkers, 0)
	assert.Len(t, s.Worker.childWorkers, 2)

	s.Worker.returnIdleChild(w1)
	s.Worker.returnIdleChild(w2)
	assert.Len(t, s.Worker.idleWorkers, 2)
	assert.Len(t, s.Worker.childWorkers, 2)
}

func TestIdleChildrenIdleSystem(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.Worker.Concurrency = 2
	s.BootstrapWorker(t)

	w1 := s.Worker.getIdleChild()
	assert.NotEmpty(t, w1)
	w2 := s.Worker.getIdleChild()
	assert.NotEmpty(t, w2)
	w3 := s.Worker.getIdleChild()
	assert.Empty(t, w3)
	assert.Len(t, s.Worker.idleWorkers, 0)
	assert.Len(t, s.Worker.childWorkers, 2)

	s.Worker.systemIdle = true

	s.Worker.returnIdleChild(w1)
	assert.Len(t, s.Worker.idleWorkers, 0)
	assert.Len(t, s.Worker.childWorkers, 1)

	s.Worker.returnIdleChild(w2)
	assert.Len(t, s.Worker.idleWorkers, 0)
	assert.Len(t, s.Worker.childWorkers, 0)
}

func TestDoNoWork(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.BootstrapWorker(t)

	s.GoDoWork(t)
	s.GetWork(t, false)
	s.Finish(t)
}

func TestDoOneWork(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.CreateSpecAndUnit(t, "sanity")
	s.BootstrapWorker(t)

	assert.False(t, s.Bit)

	s.GoDoWork(t)
	s.GetWork(t, true)
	s.Finish(t)

	assert.True(t, s.Bit)

	s.GoDoWork(t)
	s.GetWork(t, false)
	s.Finish(t)
}

func TestHeartbeat(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.BootstrapWorker(t)

	s.Worker.heartbeat()

	worker, err := s.Namespace.Worker(s.Worker.WorkerID)
	if !assert.NoError(t, err) {
		return
	}
	data, err := worker.Data()
	if assert.NoError(t, err) {
		assert.Contains(t, data, "cpus")
		assert.Contains(t, data, "go")
		assert.Contains(t, data, "goroutines")
		assert.Contains(t, data, "pid")
	}
}

func TestNonExpiration(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.CreateSpecAndUnit(t, "timeout")
	s.BootstrapWorker(t)

	s.GoDoWork(t)
	s.GetWork(t, true)

	// Non-test: if we signal the "stop" flag, then this should finish
	s.Stop <- struct{}{}
	s.Finish(t)

	assert.True(t, s.Bit)

	spec, err := s.Namespace.WorkSpec("spec")
	if !assert.NoError(t, err) {
		return
	}
	unit, err := spec.WorkUnit("unit")
	if !assert.NoError(t, err) {
		return
	}
	status, err := unit.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, coordinate.FinishedUnit, status)
	}
}

func TestExpirationCooperating(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.CreateSpecAndUnit(t, "timeout")
	s.BootstrapWorker(t)

	s.GoDoWork(t)
	s.GetWork(t, true)

	// Push the clock up into the range where the work unit will
	// be considered almost dead, and so findStaleUnits() will flag
	// it, but not so late that coordinate will expire it on its own
	s.Clock.Add(14*time.Minute + 40*time.Second)
	s.Worker.findStaleUnits()

	// That should have caused the unit to clean itself up
	s.Finish(t)

	assert.False(t, s.Bit)

	spec, err := s.Namespace.WorkSpec("spec")
	if !assert.NoError(t, err) {
		return
	}
	unit, err := spec.WorkUnit("unit")
	if !assert.NoError(t, err) {
		return
	}
	status, err := unit.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, coordinate.FailedUnit, status)
	}
}

func TestExpirationIgnoring(t *testing.T) {
	var s Suite
	s.SetUpTest(t)
	s.CreateSpecAndUnit(t, "timeout")
	s.BootstrapWorker(t)

	s.GoDoWork(t)
	s.GetWork(t, true)

	// Push the clock up into the range where the work unit will
	// be considered even closer to dead, to the point where it is
	// assumed the worker has stopped caring
	s.Clock.Add(14*time.Minute + 50*time.Second)
	s.Worker.findStaleUnits()

	// Now the unit should be failed
	spec, err := s.Namespace.WorkSpec("spec")
	if !assert.NoError(t, err) {
		return
	}
	unit, err := spec.WorkUnit("unit")
	if !assert.NoError(t, err) {
		return
	}
	status, err := unit.Status()
	if assert.NoError(t, err) {
		assert.Equal(t, coordinate.FailedUnit, status)
	}

	// Run the rest of the cleanup anyways
	s.Finish(t)
	assert.False(t, s.Bit)
}
