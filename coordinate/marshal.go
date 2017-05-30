// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"fmt"
)

// MarshalText returns a string representing a work unit status.
func (status WorkUnitStatus) MarshalText() ([]byte, error) {
	switch status {
	case AnyStatus:
		return []byte("any"), nil
	case AvailableUnit:
		return []byte("available"), nil
	case PendingUnit:
		return []byte("pending"), nil
	case FinishedUnit:
		return []byte("finished"), nil
	case FailedUnit:
		return []byte("failed"), nil
	case DelayedUnit:
		return []byte("delayed"), nil
	default:
		return nil, fmt.Errorf("invalid status (marshal, %+v)", status)
	}
}

// UnmarshalText populates a work unit status from a string.
func (status *WorkUnitStatus) UnmarshalText(text []byte) error {
	switch string(text) {
	case "any":
		*status = AnyStatus
	case "available":
		*status = AvailableUnit
	case "pending":
		*status = PendingUnit
	case "finished":
		*status = FinishedUnit
	case "failed":
		*status = FailedUnit
	case "delayed":
		*status = DelayedUnit
	default:
		return fmt.Errorf("invalid status (unmarshal, %+v)", string(text))
	}
	return nil
}

// MarshalText returns a string representing an attempt status.
func (status AttemptStatus) MarshalText() ([]byte, error) {
	switch status {
	case Pending:
		return []byte("pending"), nil
	case Expired:
		return []byte("expired"), nil
	case Finished:
		return []byte("finished"), nil
	case Failed:
		return []byte("failed"), nil
	case Retryable:
		return []byte("retryable"), nil
	default:
		return nil, fmt.Errorf("invalid status (marshal, %+v)", status)
	}
}

// UnmarshalText populates an attempt status from a string.
func (status *AttemptStatus) UnmarshalText(text []byte) error {
	switch string(text) {
	case "pending":
		*status = Pending
	case "expired":
		*status = Expired
	case "finished":
		*status = Finished
	case "failed":
		*status = Failed
	case "retryable":
		*status = Retryable
	default:
		return fmt.Errorf("invalid status (unmarshal, %+v)", string(text))
	}
	return nil
}
