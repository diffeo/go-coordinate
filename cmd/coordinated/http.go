// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"net/http"

	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restserver"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP serves HTTP coordinated connections.
type HTTP struct {
	coord coordinate.Coordinate
	laddr string
}

// Serve runs an HTTP server on the specified local address. This serves
// connections forever, and probably wants to be run in a goroutine. Panics on
// any error in the initial setup or in accepting connections.
func (h *HTTP) Serve() {
	r := mux.NewRouter()
	r.PathPrefix("/").Subrouter()
	restserver.PopulateRouter(r, h.coord)
	r.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(h.laddr, r)
}
