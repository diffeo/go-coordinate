// Unit tests for marshal.go.
//
// Copyright 2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/diffeo/go-coordinate/coordinate"
)

type WorkUnitStatusMatrix struct {
	Status      coordinate.WorkUnitStatus
	JSON        string
	EncodeError string
	DecodeError string
}

var workUnitStatuses = []WorkUnitStatusMatrix{
	{coordinate.AvailableUnit, "available", "", ""},
	{coordinate.PendingUnit, "pending", "", ""},
	{coordinate.FinishedUnit, "finished", "", ""},
	{coordinate.FailedUnit, "failed", "", ""},
	{coordinate.DelayedUnit, "delayed", "", ""},
	{coordinate.WorkUnitStatus(17), "seventeen",
		"invalid status (marshal, 17)",
		"invalid status (unmarshal, seventeen)"},
}

func TestWorkUnitToJSON(t *testing.T) {
	for _, w := range workUnitStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			actual, err := json.Marshal(w.Status)
			if w.EncodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, "\""+w.JSON+"\"",
						string(actual))
				}
			} else {
				assert.EqualError(tt, err,
					"json: error calling MarshalText for type coordinate.WorkUnitStatus: "+w.EncodeError)
			}
		})
	}
}

func TestWorkUnitToText(t *testing.T) {
	for _, w := range workUnitStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			actual, err := w.Status.MarshalText()
			if w.EncodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, w.JSON,
						string(actual))
				}
			} else {
				assert.EqualError(tt, err, w.EncodeError)
			}
		})
	}
}

func TestWorkUnitFromJSON(t *testing.T) {
	for _, w := range workUnitStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			var actual coordinate.WorkUnitStatus
			input := []byte("\"" + w.JSON + "\"")
			err := json.Unmarshal(input, &actual)
			if w.DecodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, actual, w.Status)
				}
			} else {
				assert.EqualError(tt, err, w.DecodeError)
			}
		})
	}
}

func TestWorkUnitFromText(t *testing.T) {
	for _, w := range workUnitStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			var actual coordinate.WorkUnitStatus
			input := []byte(w.JSON)
			err := actual.UnmarshalText(input)
			if w.DecodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, actual, w.Status)
				}
			} else {
				assert.EqualError(tt, err, w.DecodeError)
			}
		})
	}
}

type AttemptStatusMatrix struct {
	Status      coordinate.AttemptStatus
	JSON        string
	EncodeError string
	DecodeError string
}

var attemptStatuses = []AttemptStatusMatrix{
	{coordinate.Pending, "pending", "", ""},
	{coordinate.Finished, "finished", "", ""},
	{coordinate.Failed, "failed", "", ""},
	{coordinate.Expired, "expired", "", ""},
	{coordinate.Retryable, "retryable", "", ""},
	{coordinate.AttemptStatus(17), "seventeen",
		"invalid status (marshal, 17)",
		"invalid status (unmarshal, seventeen)"},
}

func TestAttemptToJSON(t *testing.T) {
	for _, w := range attemptStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			actual, err := json.Marshal(w.Status)
			if w.EncodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, "\""+w.JSON+"\"",
						string(actual))
				}
			} else {
				assert.EqualError(tt, err,
					"json: error calling MarshalText for type coordinate.AttemptStatus: "+w.EncodeError)
			}
		})
	}
}

func TestAttemptToText(t *testing.T) {
	for _, w := range attemptStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			actual, err := w.Status.MarshalText()
			if w.EncodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, w.JSON,
						string(actual))
				}
			} else {
				assert.EqualError(tt, err, w.EncodeError)
			}
		})
	}
}

func TestAttemptFromJSON(t *testing.T) {
	for _, w := range attemptStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			var actual coordinate.AttemptStatus
			input := []byte("\"" + w.JSON + "\"")
			err := json.Unmarshal(input, &actual)
			if w.DecodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, actual, w.Status)
				}
			} else {
				assert.EqualError(tt, err, w.DecodeError)
			}
		})
	}
}

func TestAttemptFromText(t *testing.T) {
	for _, w := range attemptStatuses {
		t.Run(w.JSON, func(tt *testing.T) {
			var actual coordinate.AttemptStatus
			input := []byte(w.JSON)
			err := actual.UnmarshalText(input)
			if w.DecodeError == "" {
				if assert.NoError(tt, err) {
					assert.Equal(tt, actual, w.Status)
				}
			} else {
				assert.EqualError(tt, err, w.DecodeError)
			}
		})
	}
}
