// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"sync"
	"testing"
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
func createWorkUnits(spec coordinate.WorkSpec, n int, t assert.TestingT) {
	for i := 0; i < n; i++ {
		_, err := spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, coordinate.WorkUnitMeta{})
		assert.NoError(t, err)
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
func TestConcurrentExecution(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestConcurrentExecution",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	numUnits := 100
	createWorkUnits(sts.WorkSpec, numUnits, t)
	results := make(chan map[string]string, 8)
	panics := pooled(func() {
		worker := createWorker(sts.Namespace)
		me := worker.Name()
		done := make(map[string]string)
		for {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			if !assert.NoError(t, err) {
				return
			}
			if len(attempts) == 0 {
				results <- done
				return
			}
			for _, attempt := range attempts {
				done[attempt.WorkUnit().Name()] = me
				err = attempt.Finish(nil)
				if !assert.NoError(t, err) {
					return
				}
			}
		}
	})
	assert.Empty(t, panics)

	close(results)
	allResults := make(map[string]string)
	for result := range results {
		for name, seq := range result {
			if other, dup := allResults[name]; dup {
				assert.Fail(t, "duplicate work unit",
					"work unit %v done by both %v and %v", name, other, seq)
			} else {
				allResults[name] = seq
			}
		}
	}
	for i := 0; i < numUnits; i++ {
		name := fmt.Sprintf("u%v", i)
		assert.Contains(t, allResults, name,
			"work unit %v not done by anybody", name)
	}
}

// TestAddSameUnit creates the same work unit many times in parallel
// and checks for errors.
func TestAddSameUnit(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestAddSameUnit",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	numUnits := 1000
	panics := pooled(func() {
		for i := 0; i < numUnits; i++ {
			unit := fmt.Sprintf("unit%03d", i)
			_, err := sts.WorkSpec.AddWorkUnit(unit, map[string]interface{}{}, coordinate.WorkUnitMeta{})
			assert.NoError(t, err)
		}
	})
	assert.Empty(t, panics)
}

// ------------------------------------------------------------------------
// Actual benchmarks:

// BenchmarkWorkUnitCreation times simply creating a significant
// number of work units in a single work spec.
func BenchmarkWorkUnitCreation(b *testing.B) {
	namespace, err := Coordinate.Namespace("BenchmarkWorkUnitCreation")
	if err != nil {
		b.Fatalf("error creating namespace: %+v", err)
	}
	defer namespace.Destroy()

	spec, err := namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	if err != nil {
		b.Fatalf("error creating work spec: %+v", err)
	}

	counter := make(chan int)
	stopCounter := make(chan struct{})
	go count(counter, stopCounter)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := <-counter
			spec.AddWorkUnit(fmt.Sprintf("u%v", i), map[string]interface{}{}, coordinate.WorkUnitMeta{})
		}
	})
	close(stopCounter)
}

// BenchmarkWorkUnitExecution benchmarks retrieving and completing work
// units.
func BenchmarkWorkUnitExecution(b *testing.B) {
	namespace, err := Coordinate.Namespace("BenchmarkWorkUnitExecution")
	if err != nil {
		b.Fatalf("error creating namespace: %+v", err)
	}
	defer namespace.Destroy()

	// Create the work spec
	spec, err := namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	if err != nil {
		b.Fatalf("error creating work spec: %+v", err)
	}
	createWorkUnits(spec, b.N, b)

	// Do some work
	b.RunParallel(func(pb *testing.PB) {
		worker := createWorker(namespace)
		for pb.Next() {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			if err != nil {
				panic(err)
			}
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				if err != nil {
					panic(err)
				}
			}
		}
	})
}

// BenchmarkMultiAttempts times executing work with multiple attempts
// coming back from one attempt.
func BenchmarkMultiAttempts(b *testing.B) {
	namespace, err := Coordinate.Namespace("BenchmarkMultiAttempts")
	if err != nil {
		b.Fatalf("error creating namespace: %+v", err)
	}
	defer namespace.Destroy()

	// Create the work spec
	spec, err := namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	if err != nil {
		b.Fatalf("error creating work spec: %+v", err)
	}
	createWorkUnits(spec, b.N, b)

	b.RunParallel(func(pb *testing.PB) {
		worker := createWorker(namespace)
		for pb.Next() {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{
				NumberOfWorkUnits: 20,
			})
			if err != nil {
				panic(err)
			}
			// We are required to drain pb.Next() so keep
			// going even if we run out of work...just finish
			// whatever attempts we are given
			for _, attempt := range attempts {
				err = attempt.Finish(nil)
				if err != nil {
					panic(err)
				}
			}
		}
	})
}

// BenchmarkUnitOutput times work unit execution, where a first work spec
// creates work units in a second.
func BenchmarkUnitOutput(b *testing.B) {
	namespace, err := Coordinate.Namespace("BenchmarkUnitOutput")
	if err != nil {
		b.Fatalf("error creating namespace: %+v", err)
	}
	defer namespace.Destroy()

	// Create the work specs
	one, err := namespace.SetWorkSpec(map[string]interface{}{
		"name": "one",
		"then": "two",
	})
	if err != nil {
		b.Fatalf("error creating work spec: %+v", err)
	}
	_, err = namespace.SetWorkSpec(map[string]interface{}{
		"name": "two",
	})
	if err != nil {
		b.Fatalf("error creating work spec: %+v", err)
	}

	createWorkUnits(one, b.N, b)

	b.RunParallel(func(pb *testing.PB) {
		worker := createWorker(namespace)
		for pb.Next() {
			attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{})
			if err != nil {
				panic(err)
			}
			for _, attempt := range attempts {
				unit := attempt.WorkUnit()
				err = attempt.Finish(map[string]interface{}{
					"output": []string{unit.Name()},
				})
				if err != nil {
					panic(err)
				}
			}
		}
	})
}
