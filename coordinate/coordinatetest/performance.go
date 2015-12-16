// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
	"runtime"
	"sync"
)

// ------------------------------------------------------------------------
// Concurrent test execution helpers:

// sequentially calls an execution function c.N times, one at a time.
func sequentially(f func(i, seq int), c *check.C) {
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		f(i, 0)
	}
}

// workerCount returns the number of workers "concurrently" will produce.
func workerCount() int {
	return runtime.GOMAXPROCS(0) * 4
}

// pooled calls an execution function workerCount times in separate
// goroutines and waits for them to finish.  The worker function is
// responsible for doing its own work and exiting when done.
func pooled(f func(seq int), c *check.C, parallel bool) {
	if !parallel {
		c.ResetTimer()
		f(0)
		return
	}
	wait := sync.WaitGroup{}
	count := workerCount()
	wait.Add(count)
	errors := make(chan interface{}, count)
	defer close(errors)
	c.ResetTimer()
	for seq := 0; seq < count; seq++ {
		go func(seq int) {
			defer func() {
				if err := recover(); err != nil {
					errors <- err
				}
				wait.Done()
			}()
			f(seq)
		}(seq)
	}
	wait.Wait()
	if len(errors) > 0 {
		for err := range errors {
			c.Error(err)
		}
		c.Fail()
	}
}

// concurrently calls an execution function c.N times, spawning several
// goroutines to run them.  This roughly reimplements the standard
// testing.B.RunParallel() for gocheck.
func concurrently(f func(i, seq int), c *check.C) {
	// NB: in the "for i..." loop, the current loop index is stored
	// in counter.
	counter := make(chan int, 1)
	counter <- 0
	worker := func(seq int) {
		for {
			i := <-counter
			if i >= c.N {
				counter <- i
				return
			}
			counter <- i + 1
			f(i, seq)
		}
	}
	pooled(worker, c, true)
	<-counter
	close(counter)
}

// ------------------------------------------------------------------------
// Coordinate setup helpers:
func createWorkUnits(spec coordinate.WorkSpec, n int, c *check.C) {
	for i := 0; i < n; i++ {
		_, err := spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, 0.0)
		c.Assert(err, check.IsNil)
	}
}

func createWorkers(namespace coordinate.Namespace, c *check.C) []coordinate.Worker {
	workers := make([]coordinate.Worker, workerCount())
	for i := range workers {
		var err error
		workers[i], err = namespace.Worker(fmt.Sprintf("worker%v", i))
		c.Assert(err, check.IsNil)
	}
	return workers
}

// ------------------------------------------------------------------------
// Concurrent execution tests:

// TestConcurrentExecution creates 100 work units and runs them
// concurrently, testing that each gets executed only once.
func (s *Suite) TestConcurrentExecution(c *check.C) {
	// Create the work spec
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)
	numUnits := 100
	createWorkUnits(spec, numUnits, c)
	workers := createWorkers(s.Namespace, c)
	results := make(chan map[string]int, workerCount())

	doWork := func(seq int) {
		worker := workers[seq]
		done := make(map[string]int)
		for {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			c.Assert(err, check.IsNil)
			if len(attempts) == 0 {
				results <- done
				return
			}
			for _, attempt := range attempts {
				done[attempt.WorkUnit().Name()] = seq
				err = attempt.Finish(nil)
				c.Assert(err, check.IsNil)
			}
		}
	}
	pooled(doWork, c, true)

	close(results)
	allResults := make(map[string]int)
	for result := range results {
		for name, seq := range result {
			if other, dup := allResults[name]; dup {
				c.Errorf("work unit %v done by both %v and %v\n", name, other, seq)
			} else {
				allResults[name] = seq
			}
		}
	}
	for i := 0; i < numUnits; i++ {
		name := fmt.Sprintf("u%v", i)
		if _, present := allResults[name]; !present {
			c.Errorf("work unit %v not done by anybody\n", name)
		}
	}
}

// TestAddSameUnit creates the same work unit many times in parallel
// and checks for errors.
func (s *Suite) TestAddSameUnit(c *check.C) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)
	numUnits := 1000
	doWork := func(int) {
		for i := 0; i < numUnits; i++ {
			unit := fmt.Sprintf("unit%03d", i)
			_, err := spec.AddWorkUnit(unit, map[string]interface{}{}, 0.0)
			c.Assert(err, check.IsNil)
		}
	}
	pooled(doWork, c, true)
}

// ------------------------------------------------------------------------
// Actual benchmarks:

// BenchmarkWorkUnitCreation times simply creating a significant
// number of work units in a single work spec.
func (s *Suite) BenchmarkWorkUnitCreation(c *check.C) {
	s.benchmarkWorkUnitCreation(c, sequentially)
}

// BenchmarkConcurrentWorkUnitCreation times creating a significant
// number of work units in a single work spec with concurrent
// execution.
func (s *Suite) BenchmarkConcurrentWorkUnitCreation(c *check.C) {
	s.benchmarkWorkUnitCreation(c, concurrently)
}

func (s *Suite) benchmarkWorkUnitCreation(c *check.C, executor func(func(i, seq int), *check.C)) {
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)

	createWorkUnit := func(i, seq int) {
		_, err := spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, 0.0)
		c.Check(err, check.IsNil)
	}
	executor(createWorkUnit, c)
}

// BenchmarkWorkUnitExecution benchmarks retrieving and completing work
// units.
func (s *Suite) BenchmarkWorkUnitExecution(c *check.C) {
	s.benchmarkWorkUnitExecution(c, sequentially)
}

// BenchmarkConcurrentWorkUnitExecution benchmarks retrieving and
// completing work units, with multiple concurrent workers.
func (s *Suite) BenchmarkConcurrentWorkUnitExecution(c *check.C) {
	s.benchmarkWorkUnitExecution(c, concurrently)
}

func (s *Suite) benchmarkWorkUnitExecution(c *check.C, executor func(f func(i, seq int), c *check.C)) {
	// Create the work spec
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)
	createWorkUnits(spec, c.N, c)
	workers := createWorkers(s.Namespace, c)

	// Do some work
	doWorkUnit := func(i, seq int) {
		worker := workers[seq]
		attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
		c.Assert(err, check.IsNil)
		c.Assert(attempts, check.HasLen, 1)
		err = attempts[0].Finish(nil)
		c.Assert(err, check.IsNil)
	}
	executor(doWorkUnit, c)
}

// BenchmarkMultiAttempts times executing work with multiple attempts
// coming back from one attempt.
func (s *Suite) BenchmarkMultiAttempts(c *check.C) {
	s.benchmarkMultiAttempts(c, false)

}

// BenchmarkConcurrentMultiAttempts times executing work with multiple
// attempts coming back from one request.
func (s *Suite) BenchmarkConcurrentMultiAttempts(c *check.C) {
	s.benchmarkMultiAttempts(c, true)
}

func (s *Suite) benchmarkMultiAttempts(c *check.C, parallel bool) {
	// Create the work spec
	spec, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	c.Assert(err, check.IsNil)
	createWorkUnits(spec, c.N, c)
	workers := createWorkers(s.Namespace, c)

	doWork := func(seq int) {
		worker := workers[seq]
		for {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{
				NumberOfWorkUnits: 20,
			})
			c.Assert(err, check.IsNil)
			if len(attempts) == 0 {
				return
			}
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				c.Assert(err, check.IsNil)
			}
		}
	}
	pooled(doWork, c, parallel)
}

// BenchmarkUnitOutput times work unit execution, where a first work spec
// creates work units in a second.
func (s *Suite) BenchmarkUnitOutput(c *check.C) {
	s.benchmarkUnitOutput(c, false)
}

// BenchmarkConcurrentUnitOutput times work unit execution, where a first
// work spec creates work units in a second.
func (s *Suite) BenchmarkConcurrentUnitOutput(c *check.C) {
	s.benchmarkUnitOutput(c, true)
}

func (s *Suite) benchmarkUnitOutput(c *check.C, parallel bool) {
	// Create the work specs
	one, err := s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	c.Assert(err, check.IsNil)
	_, err = s.Namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	c.Assert(err, check.IsNil)

	createWorkUnits(one, c.N, c)
	workers := createWorkers(s.Namespace, c)

	// Do some work
	doWork := func(seq int) {
		worker := workers[seq]
		for {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			c.Assert(err, check.IsNil)
			if len(attempts) == 0 {
				return
			}
			c.Assert(attempts, check.HasLen, 1)
			attempt := attempts[0]
			unit := attempt.WorkUnit()
			err = attempt.Finish(map[string]interface{}{
				"output": []string{unit.Name()},
			})
			c.Assert(err, check.IsNil)
		}
	}
	pooled(doWork, c, parallel)
}
