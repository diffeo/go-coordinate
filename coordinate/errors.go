package coordinate

import (
	"errors"
	"fmt"
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

	// ErrChangedName is returned from WorkSpec.SetData() if it
	// tries to change the name of the work spec.
	ErrChangedName = errors.New("Cannot change work spec 'name'")

	// ErrLostLease is returned as an error from Attempt.Renew() if
	// this is no longer the active attempt.
	ErrLostLease = errors.New("No longer the active attempt")

	// ErrNotPending is returned as an error from Attempt methods
	// that try to change an Attempt's status if the status is not
	// Pending.
	ErrNotPending = errors.New("Attempt is not pending")

	// ErrCannotBecomeContinuous is returned as an error from
	// WorkSpec.SetMeta() if the work spec was not defined with
	// the "continuous" flag set.
	ErrCannotBecomeContinuous = errors.New("Cannot set work spec to continuous")
)

// ErrNoSuchWorkSpec is returned by Namespace.WorkSpec() and similar
// functions that want to look up a work spec, but cannot find it.
type ErrNoSuchWorkSpec struct {
	Name string
}

func (err ErrNoSuchWorkSpec) Error() string {
	return fmt.Sprintf("No such work spec %v", err.Name)
}
