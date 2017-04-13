// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package demoworker provides a complete demonstration Coordinate
// application.  This defines two work specs: "generator" runs once
// per 5 seconds, and creates several work units in the "runner" work
// spec, which just prints out the work unit keys.
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/diffeo/go-coordinate/backend"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/worker"
	"github.com/mitchellh/mapstructure"
)

func main() {
	backend := backend.Backend{Implementation: "memory", Address: ""}
	flag.Var(&backend, "backend", "impl[:address] of the storage backend")
	bootstrap := flag.Bool("bootstrap", true, "Create initial work specs")
	nsName := flag.String("namespace", "", "Coordinate namespace name")
	flag.Parse()

	coordinateRoot, err := backend.Coordinate()
	if err != nil {
		panic(err)
	}

	namespace, err := coordinateRoot.Namespace(*nsName)
	if err != nil {
		panic(err)
	}

	if *bootstrap {
		err = createWorkSpecs(namespace)
		if err != nil {
			panic(err)
		}
	}

	tasks := map[string]func(context.Context, []coordinate.Attempt){
		"generator": runGenerator,
		"runner":    runRunner,
	}

	worker := worker.Worker{
		Namespace: namespace,
		Tasks:     tasks,
	}
	worker.Run(context.Background())
}

func createWorkSpecs(namespace coordinate.Namespace) error {
	var err error
	_, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":       "generator",
		"runtime":    "go",
		"task":       "generator",
		"continuous": true,
		"interval":   5,
		"then":       "runner",
	})
	if err != nil {
		return err
	}

	_, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":        "runner",
		"runtime":     "go",
		"task":        "runner",
		"max_getwork": 10,
	})
	if err != nil {
		return err
	}

	return nil
}

func runGenerator(ctx context.Context, attempts []coordinate.Attempt) {
	for _, attempt := range attempts {
		// Trying to stop?
		select {
		case <-ctx.Done():
			_ = attempt.Fail(nil)
			continue
		default:
		}

		// Generate several more work units
		kvps := make([]interface{}, 100)
		for n := range kvps {
			name := fmt.Sprintf("%s_%03d", attempt.WorkUnit().Name(), n)
			data := map[string]interface{}{
				"s": attempt.WorkUnit().Name(),
				"n": n,
			}
			kvps[n] = []interface{}{name, data}
		}
		_ = attempt.Finish(map[string]interface{}{
			"output": kvps,
		})
	}
}

func runRunner(ctx context.Context, attempts []coordinate.Attempt) {
	dead := make(map[int]struct{})

	// We'll check the "done" flag in a couple of places; this is
	// probably, in practice, excessive for the amount of work
	// happening here, but is still good practice
	select {
	case <-ctx.Done():
		for i, attempt := range attempts {
			if _, isDead := dead[i]; !isDead {
				_ = attempt.Fail(nil)
			}
		}
		return
	default:
	}

	var err error
	found := make(map[string][]int)
	for i, attempt := range attempts {
		var data map[string]interface{}
		data, err = attempt.WorkUnit().Data()
		var unit struct {
			S string
			N int
		}
		if err == nil {
			err = mapstructure.Decode(data, &unit)
		}
		if err == nil {
			found[unit.S] = append(found[unit.S], int(unit.N))
		}
		if err != nil {
			fmt.Printf("  %v: %v\n", attempt.WorkUnit().Name(), err.Error())
			_ = attempt.Fail(map[string]interface{}{
				"traceback": err.Error(),
			})
			dead[i] = struct{}{}
			err = nil
		}
	}

	select {
	case <-ctx.Done():
		for i, attempt := range attempts {
			if _, isDead := dead[i]; !isDead {
				_ = attempt.Fail(nil)
			}
		}
		return
	default:
	}

	var lines []string
	lines = append(lines, "Runner found")
	for s, is := range found {
		lines = append(lines, fmt.Sprintf("  %v -> %v", s, is))
	}
	if len(dead) > 0 {
		lines = append(lines, fmt.Sprintf("  rejected %d work units", len(dead)))
	}
	fmt.Printf("%s\n", strings.Join(lines, "\n"))

	select {
	case <-ctx.Done():
		for i, attempt := range attempts {
			if _, isDead := dead[i]; !isDead {
				_ = attempt.Fail(nil)
			}
		}
		return
	default:
	}

	for i, attempt := range attempts {
		if _, isDead := dead[i]; !isDead {
			_ = attempt.Finish(nil)
		}
	}
}
