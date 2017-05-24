// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory_test

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/stretchr/testify/suite"
	"testing"
)

// Suite runs the generic Coordinate tests with a memory backend.
type Suite struct {
	coordinatetest.Suite
}

// SetupSuite does one-time test setup, creating the memory backend.
func (s *Suite) SetupSuite() {
	s.Suite.SetupSuite()
	s.Coordinate = memory.NewWithClock(s.Clock)
}

// TestCoordinate runs the generic Coordinate tests with a memory backend.
func TestCoordinate(t *testing.T) {
	suite.Run(t, &Suite{})
}
