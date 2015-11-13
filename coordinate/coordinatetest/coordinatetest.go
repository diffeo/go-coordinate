// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package coordinatetest provides generic functional tests for the
// Coordinate interface.  These are implemented via the
// http://labix.org/gocheck support library, so a typical backend test
// module will look like
//
//     package mybackend
//
//     import (
//         "testing"
//         "github.com/diffeo/go-coordinate/coordinate/coordinatetest"
//         "gopkg.in/check.v1"
//     )
//
//     // Test is the top-level entry point to run tests.
//     func Test(t *testing.T) { check.TestingT(t) }
//
//     var _ = check.Suite(&coordinatetest.Suite{Coordinate: New()})
package coordinatetest

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
	"gopkg.in/check.v1"
)

// Suite is a gocheck-compatible test suite for Coordinate
// backends.
type Suite struct {
	// Coordinate contains the top-level interface to the backend
	// under test.
	Coordinate coordinate.Coordinate

	// Namespace contains the namespace object for the current test.
	// It is set up by the gocheck fixture code, and is only valid
	// during a test execution.
	Namespace coordinate.Namespace

	// Clock contains the alternate time source to be used in the
	// test.
	Clock *clock.Mock
}

// SetUpTest does per-test setup; specifically it creates a unique
// namespace per test.
func (s *Suite) SetUpTest(c *check.C) {
	var err error
	s.Namespace, err = s.Coordinate.Namespace(c.TestName())
	if err != nil {
		c.Error(err)
	}
}

// TearDownTest destroys the namespace created in SetUpTest.
func (s *Suite) TearDownTest(c *check.C) {
	err := s.Namespace.Destroy()
	if err != nil {
		c.Error(err)
	}
	s.Namespace = nil
}
