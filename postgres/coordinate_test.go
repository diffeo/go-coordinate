// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres_test

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/postgres"
	"github.com/stretchr/testify/suite"
	"testing"
)

// Suite runs the generic Coordinate tests with a PostgreSQL backend.
type Suite struct {
	coordinatetest.Suite
}

// SetupSuite does one-time test setup, creating the PostgreSQL backend.
func (s *Suite) SetupSuite() {
	s.Suite.SetupSuite()
	c, err := postgres.NewWithClock("", s.Clock)
	if err != nil {
		panic(err)
	}
	s.Coordinate = c
}

// TestCoordinate runs the generic Coordinate tests with a PostgreSQL
// backend.
func TestCoordinate(t *testing.T) {
	suite.Run(t, &Suite{})
}
