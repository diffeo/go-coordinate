// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory_test

//go:generate cptest --output coordinatetest_test.go --package memory_test github.com/diffeo/go-coordinate/coordinate/coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
)

func init() {
	coordinatetest.Coordinate = memory.NewWithClock(coordinatetest.Clock)
}
