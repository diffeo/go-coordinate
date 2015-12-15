// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"errors"
	"fmt"
)

// ErrNoWorkSpecName is returned as an error from functions that
// create a work spec from a map, but cannot find "name" in the map.
var ErrNoWorkSpecName = errors.New("No 'name' key in work spec")

// ErrBadWorkSpecName is returned as an error from functions that
// create a work spec from a map, but find a "name" key that is not a
// string.
var ErrBadWorkSpecName = errors.New("Work spec 'name' must be a string")

// ErrChangedName is returned from WorkSpec.SetData() if it tries to
// change the name of the work spec.
var ErrChangedName = errors.New("Cannot change work spec 'name'")

// ErrLostLease is returned as an error from Attempt.Renew() if this
// is no longer the active attempt.
var ErrLostLease = errors.New("No longer the active attempt")

// ErrNotPending is returned as an error from Attempt methods that try
// to change an Attempt's status if the status is not Pending.
var ErrNotPending = errors.New("Attempt is not pending")

// ErrCannotBecomeContinuous is returned as an error from
// WorkSpec.SetMeta() if the work spec was not defined with the
// "continuous" flag set.
var ErrCannotBecomeContinuous = errors.New("Cannot set work spec to continuous")

// ErrWrongBackend is returned from functions that take two different
// coordinate objects and combine them if the two objects come from
// different backends.  This is impossible in ordinary usage.
var ErrWrongBackend = errors.New("Cannot combine coordinate objects from different backends")

// ErrNoWork is returned from scheduler calls when there is no work to
// do.
var ErrNoWork = errors.New("No work to do")

// ErrWorkUnitNotList is returned from ExtractAddWorkUnitItem if a
// work unit as specified in a work unit's "output" field is not a
// list.
var ErrWorkUnitNotList = errors.New("work unit not a list")

// ErrWorkUnitTooShort is returned from ExtractAddWorkUnitItem if a
// work unit as specified in a work unit's "output" field has fewer
// than 2 items in its list.
var ErrWorkUnitTooShort = errors.New("too few parameters to work unit")

// ErrBadPriority is returned from ExtractAddWorkUnitItem if a
// metadata dictionary is supplied and it has a "priority" key but
// that is not a number.
var ErrBadPriority = errors.New("priority must be a number")

// ErrNoSuchWorkSpec is returned by Namespace.WorkSpec() and similar
// functions that want to look up a work spec, but cannot find it.
type ErrNoSuchWorkSpec struct {
	Name string
}

func (err ErrNoSuchWorkSpec) Error() string {
	return fmt.Sprintf("No such work spec %v", err.Name)
}

// ErrNoSuchWorkUnit is returned by WorkSpec.WorkUnit() and similar
// functions that want to look up a work unit by name, but cannot find
// it.
type ErrNoSuchWorkUnit struct {
	Name string
}

func (err ErrNoSuchWorkUnit) Error() string {
	return fmt.Sprintf("No such work unit %q", err.Name)
}
