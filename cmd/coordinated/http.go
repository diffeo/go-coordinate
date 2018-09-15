// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"net/http"
	"os"

	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restserver"
	"github.com/google/go-cloud/requestlog"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/urfave/negroni"
)

// HTTP serves HTTP coordinated connections.
type HTTP struct {
	coord coordinate.Coordinate
	laddr string
}

// Serve runs an HTTP server on the specified local address. This serves
// connections forever, and probably wants to be run in a goroutine. Panics on
// any error in the initial setup or in accepting connections.
func (h *HTTP) Serve(logRequests bool, logFormat string, logger *logrus.Logger) {
	r := mux.NewRouter()
	r.PathPrefix("/").Subrouter()
	restserver.PopulateRouter(r, h.coord)
	r.Handle("/metrics", promhttp.Handler())

	n := negroni.New()
	n.Use(negroni.NewRecovery())

	// Wrap the root handler in a logger if desired.
	var handler http.Handler = r
	if logRequests {
		handler = logWrapper(logFormat, logger, handler)
	}
	n.UseHandler(handler)

	http.ListenAndServe(h.laddr, n)
}

// logWrapper creates a wrapping logger for the given handler. It is setup this
// way rather than conforming to the negroni paradigm because the API fo the
// requestlog package, which this uses, is not directly compatible.
func logWrapper(logFormat string, logger *logrus.Logger, inner http.Handler) http.Handler {
	var reqLog requestlog.Logger
	// See the following documentation for more information on formats:
	// https://godoc.org/github.com/google/go-cloud/requestlog
	switch logFormat {
	case "ncsa":
		// Combined Log Format, as used by Apache.
		reqLog = requestlog.NewNCSALogger(os.Stderr, func(err error) {
			logger.WithError(err).Error("error writing NCSA log")
		})

	case "stackdriver":
		// As expected by Stackdriver Logging.
		reqLog = requestlog.NewStackdriverLogger(os.Stderr, func(err error) {
			logger.WithError(err).Error("error writing Stackdriver log")
		})

	default:
		logger.WithField("format", logFormat).Fatal("unrecognized log format")
	}

	return requestlog.NewHandler(reqLog, inner)
}
