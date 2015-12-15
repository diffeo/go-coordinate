// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restserver"
	"net/http"
)

// ServeHTTP runs an HTTP server on the specified local address.  This
// serves connections forever, and probably wants to be run in a
// goroutine.  Panics on any error in the initial setup or in
// accepting connections.
func ServeHTTP(
	coord coordinate.Coordinate,
	laddr string,
) {
	handler := restserver.NewRouter(coord)
	server := &http.Server{
		Addr:    laddr,
		Handler: handler,
	}
	server.ListenAndServe()
}
