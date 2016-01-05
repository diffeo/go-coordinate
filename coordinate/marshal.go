// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"fmt"
)

// MarshalJSON represents a work unit status as a JSON string.
func (status WorkUnitStatus) MarshalJSON() ([]byte, error) {
	switch status {
	case AnyStatus:
		return []byte("\"any\""), nil
	case AvailableUnit:
		return []byte("\"available\""), nil
	case PendingUnit:
		return []byte("\"pending\""), nil
	case FinishedUnit:
		return []byte("\"finished\""), nil
	case FailedUnit:
		return []byte("\"failed\""), nil
	case DelayedUnit:
		return []byte("\"delayed\""), nil
	default:
		return nil, fmt.Errorf("invalid status (marshal, %+v)", status)
	}
}

// UnmarshalJSON populates a work unit status from a JSON string.
func (status *WorkUnitStatus) UnmarshalJSON(json []byte) error {
	switch string(json) {
	case "\"any\"":
		*status = AnyStatus
	case "\"available\"":
		*status = AvailableUnit
	case "\"pending\"":
		*status = PendingUnit
	case "\"finished\"":
		*status = FinishedUnit
	case "\"failed\"":
		*status = FailedUnit
	case "\"delayed\"":
		*status = DelayedUnit
	default:
		return fmt.Errorf("invalid status (unmarshal, %+v)", string(json))
	}
	return nil
}

// MarshalJSON represents an attempt status as a JSON string.
func (status AttemptStatus) MarshalJSON() ([]byte, error) {
	switch status {
	case Pending:
		return []byte("\"pending\""), nil
	case Expired:
		return []byte("\"expired\""), nil
	case Finished:
		return []byte("\"finished\""), nil
	case Failed:
		return []byte("\"failed\""), nil
	case Retryable:
		return []byte("\"retryable\""), nil
	default:
		return nil, fmt.Errorf("invalid status (marshal, %+v)", status)
	}
}

// UnmarshalJSON populates an attempt status from a JSON string.
func (status *AttemptStatus) UnmarshalJSON(json []byte) error {
	switch string(json) {
	case "\"pending\"":
		*status = Pending
	case "\"expired\"":
		*status = Expired
	case "\"finished\"":
		*status = Finished
	case "\"failed\"":
		*status = Failed
	case "\"retryable\"":
		*status = Retryable
	default:
		return fmt.Errorf("invalid status (unmarshal, %+v)", string(json))
	}
	return nil
}
