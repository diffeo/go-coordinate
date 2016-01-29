// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache_test

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/cache"
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"gopkg.in/check.v1"
	"testing"
)

type Suite struct {
	coordinatetest.Suite
}

func (s *Suite) TestTrivialWorkUnitFlow(c *check.C) {
	c.Log("skipping; tests a flow that is explicitly different in cache")
}

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	clock := clock.NewMock()
	backend := memory.NewWithClock(clock)
	backend = cache.New(backend)
	check.Suite(&Suite{
		coordinatetest.Suite{
			Coordinate: backend,
			Clock:      clock,
		},
	})
}
