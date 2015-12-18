// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient_test

import (
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/diffeo/go-coordinate/restclient"
	"github.com/diffeo/go-coordinate/restserver"
	"gopkg.in/check.v1"
	"net/http/httptest"
	"testing"
)

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	// This sets up an object stack where the REST client code talks
	// to the REST server code, which points at an in-memory backend.
	clock := clock.NewMock()
	memBackend := memory.NewWithClock(clock)
	router := restserver.NewRouter(memBackend)
	server := httptest.NewServer(router)
	backend, err := restclient.New(server.URL)
	if err != nil {
		panic(err)
	}

	check.Suite(&coordinatetest.Suite{
		Coordinate: backend,
		Clock:      clock,
	})
}

func TestEmptyURL(t *testing.T) {
	_, err := restclient.New("")
	if err == nil {
		t.Fatal("Expected error when given empty URL.")
	}
}
