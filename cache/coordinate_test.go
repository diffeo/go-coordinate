// Copyright 2016-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache_test

import (
	"github.com/diffeo/go-coordinate/cache"
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/stretchr/testify/suite"
	"testing"
)

// Suite runs the generic Coordinate tests with a caching backend.
type Suite struct {
	coordinatetest.Suite
}

// TestTrivialWorkUnitFlow creates a work unit, issues a "delete
// everything" query, and then asserts that fetching the unit returns
// ErrNoSuchWorkUnit.  This implementation's behavior is explicitly
// different for this case.
func (s *Suite) TestTrivialWorkUnitFlow() {}

// SetupSuite does one-time test seup, creating the backend.
func (s *Suite) SetupSuite() {
	s.Suite.SetupSuite()
	backend := memory.NewWithClock(s.Clock)
	backend = cache.New(backend)
	s.Coordinate = backend
}

// TestCoordinate runs the generic Coordinate tests with a caching
// backend.
func TestCoordinate(t *testing.T) {
	suite.Run(t, &Suite{})
}
