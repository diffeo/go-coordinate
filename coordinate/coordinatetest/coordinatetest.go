// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package coordinatetest provides generic functional tests for the
// Coordinate interface.  A typical backend test module needs to wrap
// Suite to create its backend:
//
//     package mybackend
//
//     import (
//             "testing"
//             "github.com/diffeo/go-coordinate/coordinate/coordinatetest"
//             "github.com/stretchr/testify/suite"
//     )
//
//     // Suite is the per-backend generic test suite.
//     type Suite struct{
//             coordinatetest.Suite
//     }
//
//     // SetupSuite does global setup for the test suite.
//     func (s *Suite) SetupSuite() {
//             s.Suite.SetupSuite()
//             s.Coordinate = NewWithClock(s.Clock)
//     }
//
//     // TestCoordinate runs the Coordinate generic tests.
//     func TestCoordinate(t *testing.T) {
//             suite.Run(t, &Suite{})
//     }
package coordinatetest

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/suite"
)

// Suite is the generic Coordinate backend test suite.
type Suite struct {
	suite.Suite

	// Clock contains the alternate time source to be used in tests.  It
	// is pre-initialized to a mock clock.
	Clock *clock.Mock

	// Coordinate contains the top-level interface to the backend under
	// test.  It is set by importing packages.
	Coordinate coordinate.Coordinate
}

// SetupSuite does one-time initialization for the test suite.
func (s *Suite) SetupSuite() {
	s.Clock = clock.NewMock()
}
