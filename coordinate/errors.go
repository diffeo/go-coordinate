package coordinate

import (
	"errors"
)

var (
	// ErrNoWorkSpecName is returned as an error from functions
	// that create a work spec from a map, but cannot find "name"
	// in the map.
	ErrNoWorkSpecName = errors.New("No 'name' key in work spec")

	// ErrBadWorkSpecName is returned as an error from functions that
	// create a work spec from a map, but find a "name" key that
	// is not a string.
	ErrBadWorkSpecName = errors.New("Work spec 'name' must be a string")

	// ErrLostLease is returned as an error from Attempt.Renew() if
	// this is no longer the active attempt.
	ErrLostLease = errors.New("No longer the active attempt")

	// ErrNotPending is returned as an error from Attempt methods
	// that try to change an Attempt's status if the status is not
	// Pending.
	ErrNotPending = errors.New("Attempt is not pending")
)
