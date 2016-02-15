// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package coordinatetest provides generic functional tests for the
// Coordinate interface.  These need to be copied into individual
// backends' test directories, perhaps by using the cptest support
// program.  A typical backend test module additionally needs to set
// Clock and Coordinate in either an init or TestMain function.
//
//     package mybackend
//
//     import (
//             "github.com/diffeo/go-coordinate/coordinate/coordinatetest"
//     )
//
//     func init() {
//             coordinatetest.Coordinate = NewWithClock(coordinatetest.Clock)
//     }
package coordinatetest

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
)

// Clock contains the alternate time source to be used in tests.  It
// is pre-initialized to a mock clock.
var Clock = clock.NewMock()

// Coordinate contains the top-level interface to the backend under
// test.  It is set by importing packages.
var Coordinate coordinate.Coordinate
