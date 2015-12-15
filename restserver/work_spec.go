// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"errors"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
)

func (api *restAPI) fillWorkSpecShort(namespace coordinate.Namespace, name string, short *restdata.WorkSpecShort) error {
	short.Name = name
	return buildURLs(api.Router,
		"namespace", namespace.Name(),
		"spec", name,
	).URL(&short.URL, "workSpec").Error
}

func (api *restAPI) fillWorkSpec(namespace coordinate.Namespace, name string, repr *restdata.WorkSpec) error {
	err := api.fillWorkSpecShort(namespace, name, &repr.WorkSpecShort)
	if err == nil {
		err = buildURLs(api.Router,
			"namespace", namespace.Name(),
			"spec", name).
			URL(&repr.WorkUnitsURL, "workUnits").
			Template(&repr.WorkUnitURL, "workUnit", "unit").
			URL(&repr.MetaURL, "workSpecMeta").
			URL(&repr.WorkUnitCountsURL, "workSpecCounts").
			URL(&repr.WorkUnitChangeURL, "workSpecChange").
			URL(&repr.WorkUnitAdjustURL, "workSpecAdjust").
			Error
	}
	if err == nil {
		repr.MetaURL += "{?counts}"
		qs := "{?name*,status*,previous,limit}"
		repr.WorkUnitQueryURL = repr.WorkUnitsURL + qs
		repr.WorkUnitChangeURL += qs
		repr.WorkUnitAdjustURL += qs
	}
	return err
}

func (api *restAPI) WorkSpecList(ctx *context) (interface{}, error) {
	workSpecNames, err := ctx.Namespace.WorkSpecNames()
	if err != nil {
		return nil, err
	}
	response := restdata.WorkSpecList{
		WorkSpecs: make([]restdata.WorkSpecShort, len(workSpecNames)),
	}
	for i, name := range workSpecNames {
		err = api.fillWorkSpecShort(ctx.Namespace, name, &response.WorkSpecs[i])
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (api *restAPI) WorkSpecPost(ctx *context, in interface{}) (interface{}, error) {
	req, valid := in.(restdata.WorkSpec)
	if !valid {
		return nil, errUnmarshal
	}
	if req.Data == nil {
		return nil, restdata.ErrBadRequest{Err: errors.New("Missing data")}
	}
	spec, err := ctx.Namespace.SetWorkSpec(req.Data)
	if err == coordinate.ErrNoWorkSpecName || err == coordinate.ErrBadWorkSpecName {
		return nil, restdata.ErrBadRequest{Err: err}
	} else if err != nil {
		return nil, err
	}
	short := restdata.WorkSpecShort{}
	err = api.fillWorkSpecShort(ctx.Namespace, spec.Name(), &short)
	if err != nil {
		return nil, err
	}
	resp := responseCreated{
		Location: short.URL,
		Body:     short,
	}
	return resp, nil
}

func (api *restAPI) WorkSpecGet(ctx *context) (interface{}, error) {
	data, err := ctx.WorkSpec.Data()
	if err != nil {
		return nil, err
	}
	resp := restdata.WorkSpec{Data: data}
	err = api.fillWorkSpec(ctx.Namespace, ctx.WorkSpec.Name(), &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (api *restAPI) WorkSpecPut(ctx *context, in interface{}) (interface{}, error) {
	req, valid := in.(restdata.WorkSpec)
	if !valid {
		return nil, errUnmarshal
	}
	if req.Data != nil {
		err := ctx.WorkSpec.SetData(req.Data)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (api *restAPI) WorkSpecDelete(ctx *context) (interface{}, error) {
	err := ctx.Namespace.DestroyWorkSpec(ctx.WorkSpec.Name())
	return nil, err
}

func (api *restAPI) WorkSpecMetaGet(ctx *context) (interface{}, error) {
	withCounts := ctx.BoolParam("counts", false)
	meta, err := ctx.WorkSpec.Meta(withCounts)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (api *restAPI) WorkSpecMetaPut(ctx *context, in interface{}) (interface{}, error) {
	meta, valid := in.(coordinate.WorkSpecMeta)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.WorkSpec.SetMeta(meta)
	return nil, err
}

func (api *restAPI) WorkSpecCounts(ctx *context) (interface{}, error) {
	counts, err := ctx.WorkSpec.CountWorkUnitStatus()
	return counts, err
}

func (api *restAPI) WorkSpecChange(ctx *context, in interface{}) (interface{}, error) {
	var (
		err   error
		q     coordinate.WorkUnitQuery
		repr  restdata.WorkUnit
		valid bool
	)
	q, err = ctx.WorkUnitQuery()
	if err == nil {
		repr, valid = in.(restdata.WorkUnit)
		if !valid {
			err = errUnmarshal
		}
	}
	if err == nil && repr.Priority != nil {
		err = ctx.WorkSpec.SetWorkUnitPriorities(q, *repr.Priority)
	}
	return nil, err
}

func (api *restAPI) WorkSpecAdjust(ctx *context, in interface{}) (interface{}, error) {
	var (
		err   error
		q     coordinate.WorkUnitQuery
		repr  restdata.WorkUnit
		valid bool
	)
	q, err = ctx.WorkUnitQuery()
	if err == nil {
		repr, valid = in.(restdata.WorkUnit)
		if !valid {
			err = errUnmarshal
		}
	}
	if err == nil && repr.Priority != nil {
		err = ctx.WorkSpec.AdjustWorkUnitPriorities(q, *repr.Priority)
	}
	return nil, err
}

// PopulateWorkSpec adds routes to a namespace router to manipulate
// work specs.  r should generally be rooted in a subpath like
// /namespace/{}.
func (api *restAPI) PopulateWorkSpec(r *mux.Router) {
	r.Path("/work_spec").Name("workSpecs").Handler(&resourceHandler{
		Representation: restdata.WorkSpec{},
		Context:        api.Context,
		Get:            api.WorkSpecList,
		Post:           api.WorkSpecPost,
	})
	r.Path("/work_spec/{spec}").Name("workSpec").Handler(&resourceHandler{
		Representation: restdata.WorkSpec{},
		Context:        api.Context,
		Get:            api.WorkSpecGet,
		Put:            api.WorkSpecPut,
		Delete:         api.WorkSpecDelete,
	})
	r.Path("/work_spec/{spec}/meta").Name("workSpecMeta").Handler(&resourceHandler{
		Representation: coordinate.WorkSpecMeta{},
		Context:        api.Context,
		Get:            api.WorkSpecMetaGet,
		Put:            api.WorkSpecMetaPut,
	})
	r.Path("/work_spec/{spec}/counts").Name("workSpecCounts").Handler(&resourceHandler{
		Representation: make(map[coordinate.WorkUnitStatus]int),
		Context:        api.Context,
		Get:            api.WorkSpecCounts,
	})
	r.Path("/work_spec/{spec}/change").Name("workSpecChange").Handler(&resourceHandler{
		Representation: restdata.WorkUnit{},
		Context:        api.Context,
		Post:           api.WorkSpecChange,
	})
	r.Path("/work_spec/{spec}/adjust").Name("workSpecAdjust").Handler(&resourceHandler{
		Representation: restdata.WorkUnit{},
		Context:        api.Context,
		Post:           api.WorkSpecAdjust,
	})
	sr := r.PathPrefix("/work_spec/{spec}").Subrouter()
	api.PopulateWorkUnit(sr)
}
