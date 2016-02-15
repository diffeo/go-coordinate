// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient_test

//go:generate cptest --output coordinatetest_test.go --package restclient_test github.com/diffeo/go-coordinate/coordinate/coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate/coordinatetest"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/diffeo/go-coordinate/restclient"
	"github.com/diffeo/go-coordinate/restserver"
	"net/http/httptest"
	"testing"
)

func init() {
	// This sets up an object stack where the REST client code talks
	// to the REST server code, which points at an in-memory backend.
	memBackend := memory.NewWithClock(coordinatetest.Clock)
	router := restserver.NewRouter(memBackend)
	server := httptest.NewServer(router)
	backend, err := restclient.New(server.URL)
	if err != nil {
		panic(err)
	}
	coordinatetest.Coordinate = backend
}

func TestEmptyURL(t *testing.T) {
	_, err := restclient.New("")
	if err == nil {
		t.Fatal("Expected error when given empty URL.")
	}
}
