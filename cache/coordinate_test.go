// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache_test

//go:generate cptest --output coordinatetest_test.go --package cache_test --except TestTrivialWorkUnitFlow github.com/diffeo/go-coordinate/coordinate/coordinatetest

// TestTrivialWorkUnitFlow creates a work unit, issues a "delete
// everything" query, and then asserts that fetching the unit returns
// ErrNoSuchWorkUnit.  This implementation's behavior is explicitly
// different for this case.

import (
	"github.com/diffeo/go-coordinate/cache"
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
)

func init() {
	backend := memory.NewWithClock(coordinatetest.Clock)
	backend = cache.New(backend)
	coordinatetest.Coordinate = backend
}
