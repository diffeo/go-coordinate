// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

import (
	"errors"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
)

type workUnit struct {
	resource
	Representation restdata.WorkUnit
	workSpec       *workSpec
}

func workUnitFromURL(parent *resource, path string, spec *workSpec) (*workUnit, error) {
	unit := workUnit{
		workSpec: spec,
	}
	var err error
	unit.URL, err = parent.Template(path, map[string]interface{}{})
	if err == nil {
		err = unit.Refresh()
	}
	if err == nil && unit.workSpec == nil {
		unit.workSpec, err = workSpecFromURL(&unit.resource, unit.Representation.WorkSpecURL)
	}
	return &unit, err
}

func (unit *workUnit) Refresh() error {
	unit.Representation = restdata.WorkUnit{}
	return unit.Get(&unit.Representation)
}

func (unit *workUnit) Name() string {
	return unit.Representation.Name
}

func (unit *workUnit) Data() (map[string]interface{}, error) {
	err := unit.Refresh()
	if err == nil {
		return unit.Representation.Data, nil
	}
	return nil, err
}

func (unit *workUnit) WorkSpec() coordinate.WorkSpec {
	return unit.workSpec
}

func (unit *workUnit) Status() (coordinate.WorkUnitStatus, error) {
	err := unit.Refresh()
	if err == nil {
		return unit.Representation.Status, nil
	}
	return 0, err
}

func (unit *workUnit) Meta() (meta coordinate.WorkUnitMeta, err error) {
	err = unit.Refresh()
	if err == nil && unit.Representation.Meta == nil {
		err = errors.New("Invalid work unit response")
	}
	if err == nil {
		meta = *unit.Representation.Meta
	}
	return
}

func (unit *workUnit) SetMeta(meta coordinate.WorkUnitMeta) error {
	repr := restdata.WorkUnit{}
	repr.Meta = &meta
	return unit.Put(repr, nil)
}

func (unit *workUnit) Priority() (float64, error) {
	meta, err := unit.Meta()
	return meta.Priority, err
}

func (unit *workUnit) SetPriority(p float64) error {
	// This is a roundabout way to do this; but it is the only
	// entry point to change *only* the priority
	return unit.workSpec.SetWorkUnitPriorities(coordinate.WorkUnitQuery{
		Names: []string{unit.Representation.Name},
	}, p)
}

func (unit *workUnit) ActiveAttempt() (coordinate.Attempt, error) {
	err := unit.Refresh()
	if err == nil {
		aaURL := unit.Representation.ActiveAttemptURL
		if aaURL == "" {
			return nil, nil
		}
		return attemptFromURL(&unit.resource, aaURL, unit, nil)
	}
	return nil, err
}

func (unit *workUnit) ClearActiveAttempt() error {
	repr := restdata.WorkUnit{}
	repr.ActiveAttemptURL = "-"
	return unit.Put(repr, nil)
}

func (unit *workUnit) Attempts() ([]coordinate.Attempt, error) {
	// See also commentary in worker.go returnAttempts().
	// Note that at least most work units have very few attempts,
	// and that every attempt should be for this work unit.
	var repr restdata.AttemptList
	err := unit.GetFrom(unit.Representation.AttemptsURL, map[string]interface{}{}, &repr)
	if err != nil {
		return nil, err
	}
	attempts := make([]coordinate.Attempt, len(repr.Attempts))
	for i, attempt := range repr.Attempts {
		var aUnit *workUnit
		if attempt.WorkUnitURL == unit.Representation.URL {
			aUnit = unit
		}
		attempts[i], err = attemptFromURL(&unit.resource, attempt.URL, aUnit, nil)
		if err != nil {
			return nil, err
		}
	}
	return attempts, nil
}

func (unit *workUnit) NumAttempts() (int, error) {
	var repr restdata.AttemptList
	err := unit.GetFrom(unit.Representation.AttemptsURL, map[string]interface{}{}, &repr)
	if err != nil {
		return 0, err
	}
	return len(repr.Attempts), nil
}
