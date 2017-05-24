// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"math/rand"
	"strings"
	"sync"
)

// ------------------------------------------------------------------------
// Concurrent test execution helpers:

// pooled calls an execution function workerCount times in separate
// goroutines and waits for them to finish.  The worker function is
// responsible for doing its own work and exiting when done.  Returns
// a slice of panic objects, or nil if all were successful.
func pooled(f func()) []interface{} {
	wait := sync.WaitGroup{}
	count := 8
	wait.Add(count)
	errors := make(chan interface{}, count)
	for seq := 0; seq < count; seq++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					errors <- err
				}
				wait.Done()
			}()
			f()
		}()
	}
	wait.Wait()
	close(errors)
	var result []interface{}
	for err := range errors {
		result = append(result, err)
	}
	return result
}

// count generates a stream of integers to a channel, until told to stop.
func count(val chan<- int, stop <-chan struct{}) {
	for i := 0; ; i++ {
		select {
		case val <- i:
		case <-stop:
			close(val)
			return
		}
	}
}

// ------------------------------------------------------------------------
// Coordinate setup helpers:
func (s *Suite) createWorkUnits(spec coordinate.WorkSpec, n int) {
	for i := 0; i < n; i++ {
		_, err := spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, coordinate.WorkUnitMeta{})
		s.NoError(err)
	}
}

// createWorker creates a worker with a random name.  If there is a
// failure creating the worker, panics.
func createWorker(namespace coordinate.Namespace) coordinate.Worker {
	// Construct a random worker name:
	workerName := strings.Map(func(rune) rune {
		return rune('A' + rand.Intn(26))
	}, "12345678")
	worker, err := namespace.Worker(workerName)
	if err != nil {
		panic(err)
	}
	return worker
}

// ------------------------------------------------------------------------
// Concurrent execution tests:

// TestConcurrentExecution creates 100 work units and runs them
// concurrently, testing that each gets executed only once.
func (s *Suite) TestConcurrentExecution() {
	sts := SimpleTestSetup{
		NamespaceName: "TestConcurrentExecution",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	numUnits := 100
	s.createWorkUnits(sts.WorkSpec, numUnits)
	results := make(chan map[string]string, 8)
	panics := pooled(func() {
		worker := createWorker(sts.Namespace)
		me := worker.Name()
		done := make(map[string]string)
		for {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			if !s.NoError(err) {
				return
			}
			if len(attempts) == 0 {
				results <- done
				return
			}
			for _, attempt := range attempts {
				done[attempt.WorkUnit().Name()] = me
				err = attempt.Finish(nil)
				if !s.NoError(err) {
					return
				}
			}
		}
	})
	s.Empty(panics)

	close(results)
	allResults := make(map[string]string)
	for result := range results {
		for name, seq := range result {
			if other, dup := allResults[name]; dup {
				s.Fail("duplicate work unit",
					"work unit %v done by both %v and %v", name, other, seq)
			} else {
				allResults[name] = seq
			}
		}
	}
	for i := 0; i < numUnits; i++ {
		name := fmt.Sprintf("u%v", i)
		s.Contains(allResults, name,
			"work unit %v not done by anybody", name)
	}
}

// TestAddSameUnit creates the same work unit many times in parallel
// and checks for errors.
func (s *Suite) TestAddSameUnit() {
	sts := SimpleTestSetup{
		NamespaceName: "TestAddSameUnit",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	numUnits := 1000
	panics := pooled(func() {
		for i := 0; i < numUnits; i++ {
			unit := fmt.Sprintf("unit%03d", i)
			_, err := sts.WorkSpec.AddWorkUnit(unit, map[string]interface{}{}, coordinate.WorkUnitMeta{})
			s.NoError(err)
		}
	})
	s.Empty(panics)
}
