// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type duration struct {
	Duration time.Duration
	Written  string
	Read     string
}

var someTimes = []duration{
	duration{time.Duration(1 * time.Second), "0 0:0:1.000000", "00:00:01"},
	duration{time.Duration(1 * time.Minute), "0 0:1:0.000000", "00:01:00"},
	duration{time.Duration(1 * time.Hour), "0 1:0:0.000000", "01:00:00"},
	duration{time.Duration(24 * time.Hour), "1 0:0:0.000000", "1 day"},
	duration{time.Duration(25 * time.Hour), "1 1:0:0.000000", "1 day 01:00:00"},
	duration{time.Duration(49 * time.Hour), "2 1:0:0.000000", "2 days 01:00:00"},
}

func TestDurationToSQL(t *testing.T) {
	for _, d := range someTimes {
		actual := string(durationToSQL(d.Duration))
		assert.Equal(t, d.Written, actual)
	}
}

func TestSQLToDuration(t *testing.T) {
	for _, d := range someTimes {
		actual, err := sqlToDuration(d.Read)
		if assert.NoError(t, err, d.Read) {
			assert.Equal(t, d.Duration, actual, d.Read)
		}
	}
}
