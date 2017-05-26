// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/prometheus/client_golang/prometheus"
)

var coordinateSummary = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "diffeo",
		Subsystem: "coordinate",
		Name:      "coordinate_summary",
		Help:      "Summary of coordinate work specs",
	},
	[]string{
		"namespace",
		"work_spec",
		"status",
	},
)

func init() {
	prometheus.MustRegister(coordinateSummary)
}

func observe(coord coordinate.Coordinate) {
	for {
		summary, _ := coord.Summarize()
		for _, record := range summary {
			statusBytes, _ := record.Status.MarshalJSON()
			coordinateSummary.With(prometheus.Labels{
				"namespace": record.Namespace,
				"work_spec": record.WorkSpec,
				"status":    string(statusBytes),
			}).Set(float64(record.Count))
		}
	}
}
