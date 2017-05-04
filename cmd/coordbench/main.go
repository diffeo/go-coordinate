// Copyright 2016-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package coordbench provides a load-generation tool for Coordinate.
package main

import (
	"github.com/diffeo/go-coordinate/backend"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/satori/go.uuid"
	"github.com/urfave/cli"
	"runtime"
	"sync"
	"time"
)

type benchWork struct {
	Coordinate  coordinate.Coordinate
	Namespace   coordinate.Namespace
	WorkSpec    coordinate.WorkSpec
	Concurrency int
}

func (bench *benchWork) Run(runner func()) {
	wg := sync.WaitGroup{}
	wg.Add(bench.Concurrency)
	for i := 0; i < bench.Concurrency; i++ {
		go func() {
			defer wg.Done()
			runner()
		}()
	}
	wg.Wait()
}

var bench benchWork

var addUnits = cli.Command{
	Name:  "add",
	Usage: "create many work units",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "count",
			Value: 100,
			Usage: "number of work units to create",
		},
	},
	Action: func(c *cli.Context) {
		count := c.Int("count")
		numbers := make(chan int)
		go func() {
			for i := 1; i <= count; i++ {
				numbers <- i
			}
			close(numbers)
		}()
		bench.Run(func() {
			for <-numbers != 0 {
				name := uuid.NewV4().String()
				bench.WorkSpec.AddWorkUnit(name, map[string]interface{}{}, coordinate.WorkUnitMeta{})
			}
		})
	},
}

var doWork = cli.Command{
	Name:  "do",
	Usage: "do work as long as there is more",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "batch",
			Value: 100,
			Usage: "request this many attempts in one batch",
		},
		cli.DurationFlag{
			Name:  "delay",
			Value: 0,
			Usage: "wait this long per work unit before completion",
		},
	},
	Action: func(c *cli.Context) {
		batch := c.Int("batch")
		delay := c.Duration("delay")
		name := uuid.NewV4().String()
		parent, err := bench.Namespace.Worker(name)
		if err != nil {
			return
		}
		bench.Run(func() {
			name := uuid.NewV4().String()
			worker, err := bench.Namespace.Worker(name)
			if err != nil {
				return
			}
			err = worker.SetParent(parent)
			if err != nil {
				return
			}

			for {
				attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{NumberOfWorkUnits: batch})
				if err != nil || len(attempts) == 0 {
					break
				}
				for _, attempt := range attempts {
					time.Sleep(delay)
					_ = attempt.Finish(nil)
				}
			}
			_ = worker.Deactivate()
		})
	},
}

var clear = cli.Command{
	Name:  "clear",
	Usage: "delete all of the work units",
	Action: func(c *cli.Context) {
		bench.WorkSpec.DeleteWorkUnits(coordinate.WorkUnitQuery{})
	},
}

func main() {
	backend := backend.Backend{Implementation: "memory"}
	app := cli.NewApp()
	app.Usage = "benchmark the Coordinate job queue system"
	app.Flags = []cli.Flag{
		cli.GenericFlag{
			Name:  "backend",
			Value: &backend,
			Usage: "impl:[address] of Coordinate backend",
		},
		cli.StringFlag{
			Name:  "namespace",
			Usage: "Coordinate namespace name",
		},
		cli.IntFlag{
			Name:  "concurrency",
			Value: runtime.NumCPU(),
			Usage: "run this many jobs in parallel",
		},
	}
	app.Commands = []cli.Command{
		addUnits,
		doWork,
		clear,
	}
	app.Before = func(c *cli.Context) (err error) {
		bench.Coordinate, err = backend.Coordinate()
		if err != nil {
			return
		}

		bench.Namespace, err = bench.Coordinate.Namespace(c.String("namespace"))
		if err != nil {
			return
		}

		bench.WorkSpec, err = bench.Namespace.SetWorkSpec(map[string]interface{}{
			"name": "spec",
		})
		if err != nil {
			return
		}

		bench.Concurrency = c.Int("concurrency")

		return
	}
	app.RunAndExitOnError()
}
