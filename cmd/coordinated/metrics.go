// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"context"
	"math"
	"time"

	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	workUnitsNumber = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coordinate",
			Name:      "summary_seconds",
			Help:      "Seconds required to gather coordinate summary",
			Buckets:   prometheus.ExponentialBuckets(math.Pow(2, -5), 2, 12),
		})

	summarySeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "coordinate",
			Name:      "work_units",
			Help:      "Number of coordinate work specs",
		},
		[]string{
			"namespace",
			"work_spec",
			"status",
		})
)

func init() {
	prometheus.MustRegister(summarySeconds)
	prometheus.MustRegister(workUnitsNumber)
}

// Observe repeatedly calls Summarize() on coordinate in an infinite loop, and
// observes each SummaryRecord's fields on a prometheus GaugeVec, and the
// resultant time duration on a prometheus Histogram.
func Observe(
	ctx context.Context,
	coord coordinate.Coordinate,
	period time.Duration,
	log *logrus.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(period):
			t0 := time.Now()
			summary, err := coord.Summarize()
			if err != nil {
				log.Error(err)
				break
			}
			workUnitsNumber.Observe(time.Since(t0).Seconds())
			for _, record := range summary {
				status, err := record.Status.MarshalText()
				if err != nil {
					log.Error(err)
					break
				}
				summarySeconds.With(prometheus.Labels{
					"namespace": record.Namespace,
					"work_spec": record.WorkSpec,
					"status":    string(status),
				}).Set(float64(record.Count))
			}
		}
	}
}
