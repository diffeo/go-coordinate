// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
)

func (api *restAPI) fillWorkUnitShort(namespace coordinate.Namespace, spec coordinate.WorkSpec, name string, short *restdata.WorkUnitShort) error {
	short.Name = name
	return buildURLs(api.Router,
		"namespace", namespace.Name(),
		"spec", spec.Name(),
		"unit", name,
	).URL(&short.URL, "workUnit").Error
}

func (api *restAPI) fillWorkUnit(namespace coordinate.Namespace, spec coordinate.WorkSpec, unit coordinate.WorkUnit, repr *restdata.WorkUnit) error {
	err := api.fillWorkUnitShort(namespace, spec, unit.Name(), &repr.WorkUnitShort)
	if err == nil {
		repr.Data, err = unit.Data()
	}
	if err == nil {
		var priority float64
		priority, err = unit.Priority()
		repr.Priority = &priority
	}
	if err == nil {
		repr.Status, err = unit.Status()
	}
	if err == nil {
		err = buildURLs(api.Router,
			"namespace", namespace.Name(),
			"spec", spec.Name(),
			"unit", unit.Name(),
		).
			URL(&repr.WorkSpecURL, "workSpec").
			URL(&repr.AttemptsURL, "workUnitAttempts").
			Error
	}
	if err == nil {
		var attempt coordinate.Attempt
		attempt, err = unit.ActiveAttempt()
		if err == nil && attempt != nil {
			// This is cheating, a little, but it's probably
			// the easiest way to reuse this code
			var short restdata.AttemptShort
			err = api.fillAttemptShort(namespace, attempt, &short)
			if err == nil {
				repr.ActiveAttemptURL = short.URL
			}
		}
	}
	return err
}

func (api *restAPI) WorkUnitsGet(ctx *context) (interface{}, error) {
	var (
		err   error
		q     coordinate.WorkUnitQuery
		units map[string]coordinate.WorkUnit
		resp  restdata.WorkUnitList
	)
	q, err = ctx.WorkUnitQuery()
	if err == nil {
		units, err = ctx.WorkSpec.WorkUnits(q)
	}
	if err == nil {
		for _, unit := range units {
			var short restdata.WorkUnitShort
			err = api.fillWorkUnitShort(ctx.Namespace, ctx.WorkSpec, unit.Name(), &short)
			if err != nil {
				return nil, err
			}
			resp.WorkUnits = append(resp.WorkUnits, short)
		}
		return resp, nil
	}
	return nil, err
}

func (api *restAPI) WorkUnitsDelete(ctx *context) (interface{}, error) {
	var (
		err  error
		q    coordinate.WorkUnitQuery
		resp restdata.WorkUnitDeleted
	)
	q, err = ctx.WorkUnitQuery()
	if err == nil {
		resp.Deleted, err = ctx.WorkSpec.DeleteWorkUnits(q)
	}
	if err == nil {
		return resp, nil
	}
	return nil, err
}

func (api *restAPI) WorkUnitsPost(ctx *context, in interface{}) (interface{}, error) {
	var (
		err   error
		unit  coordinate.WorkUnit
		short restdata.WorkUnitShort
	)
	repr, valid := in.(restdata.WorkUnit)
	if !valid {
		err = errUnmarshal
	}
	if err == nil {
		var priority float64
		if repr.Priority != nil {
			priority = *repr.Priority
		}
		unit, err = ctx.WorkSpec.AddWorkUnit(repr.Name, repr.Data, priority)
	}
	if err == nil {
		err = api.fillWorkUnitShort(ctx.Namespace, ctx.WorkSpec, unit.Name(), &short)
	}
	if err == nil {
		resp := responseCreated{
			Location: short.URL,
			Body:     short,
		}
		return resp, nil
	}
	return nil, err
}

func (api *restAPI) WorkUnitGet(ctx *context) (interface{}, error) {
	repr := restdata.WorkUnit{}
	err := api.fillWorkUnit(ctx.Namespace, ctx.WorkSpec, ctx.WorkUnit, &repr)
	if err == nil {
		return repr, nil
	}
	return nil, err
}

func (api *restAPI) WorkUnitPut(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.WorkUnit)
	if !valid {
		return nil, errUnmarshal
	}

	// What we do depends on what changed
	var err error

	if err == nil && repr.ActiveAttemptURL == "-" {
		err = ctx.WorkUnit.ClearActiveAttempt()
	}
	if err == nil && repr.Priority != nil {
		err = ctx.WorkUnit.SetPriority(*repr.Priority)
	}

	return nil, err
}

func (api *restAPI) WorkUnitAttempts(ctx *context) (interface{}, error) {
	attempts, err := ctx.WorkUnit.Attempts()
	if err != nil {
		return nil, err
	}
	return api.returnAttempts(ctx, attempts)
}

// PopulateWorkUnit adds routes to a namespace router to manipulate
// work unit.  r should generally be rooted in a subpath like
// /namespace/{}/work_spec/{}.
func (api *restAPI) PopulateWorkUnit(r *mux.Router) {
	r.Path("/work_unit").Name("workUnits").Handler(&resourceHandler{
		Representation: restdata.WorkUnit{},
		Context:        api.Context,
		Get:            api.WorkUnitsGet,
		Delete:         api.WorkUnitsDelete,
		Post:           api.WorkUnitsPost,
	})
	r.Path("/work_unit/{unit}").Name("workUnit").Handler(&resourceHandler{
		Representation: restdata.WorkUnit{},
		Context:        api.Context,
		Get:            api.WorkUnitGet,
		Put:            api.WorkUnitPut,
	})
	r.Path("/work_unit/{unit}/attempts").Name("workUnitAttempts").Handler(&resourceHandler{
		Representation: restdata.AttemptList{},
		Context:        api.Context,
		Get:            api.WorkUnitAttempts,
	})
	sr := r.PathPrefix("/work_unit/{unit}").Subrouter()
	api.PopulateAttempt(sr)
}
