// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient_test

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/diffeo/go-coordinate/restclient"
	"github.com/diffeo/go-coordinate/restserver"
	"github.com/stretchr/testify/suite"
	"net/http/httptest"
	"testing"
)

// Suite runs the generic Coordinate tests with a REST backend.
type Suite struct {
	coordinatetest.Suite
}

// SetupSuite does one-time test setup, creating the REST backend.
func (s *Suite) SetupSuite() {
	s.Suite.SetupSuite()

	// This sets up an object stack where the REST client code talks
	// to the REST server code, which points at an in-memory backend.
	memBackend := memory.NewWithClock(s.Clock)
	router := restserver.NewRouter(memBackend)
	server := httptest.NewServer(router)
	backend, err := restclient.New(server.URL)
	if err != nil {
		panic(err)
	}
	s.Coordinate = backend
}

// TestCoordinate runs the generic Coordinate tests with a memory backend.
func TestCoordinate(t *testing.T) {
	suite.Run(t, &Suite{})
}

func TestEmptyURL(t *testing.T) {
	_, err := restclient.New("")
	if err == nil {
		t.Fatal("Expected error when given empty URL.")
	}
}
