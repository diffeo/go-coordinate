// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres_test

//go:generate cptest --output coordinatetest_test.go --package postgres_test github.com/diffeo/go-coordinate/coordinate/coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/postgres"
)

func init() {
	c, err := postgres.NewWithClock("", coordinatetest.Clock)
	if err != nil {
		panic(err)
	}
	coordinatetest.Coordinate = c
}
