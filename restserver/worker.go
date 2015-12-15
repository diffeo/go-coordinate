// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
)

func (api *restAPI) fillWorkerShort(namespace coordinate.Namespace, worker coordinate.Worker, short *restdata.WorkerShort) error {
	short.Name = worker.Name()
	return buildURLs(api.Router,
		"namespace", namespace.Name(),
		"worker", short.Name,
	).
		URL(&short.URL, "worker").
		Error
}

func (api *restAPI) fillWorker(namespace coordinate.Namespace, worker coordinate.Worker, result *restdata.Worker) error {
	err := api.fillWorkerShort(namespace, worker, &result.WorkerShort)
	if err == nil {
		err = buildURLs(api.Router,
			"namespace", namespace.Name(),
			"worker", worker.Name(),
		).
			URL(&result.RequestAttemptsURL, "workerRequestAttempts").
			URL(&result.MakeAttemptURL, "workerMakeAttempt").
			URL(&result.ActiveAttemptsURL, "workerActiveAttempts").
			URL(&result.AllAttemptsURL, "workerAllAttempts").
			URL(&result.ChildAttemptsURL, "workerChildAttempts").
			Error
	}
	var parent coordinate.Worker
	if err == nil {
		parent, err = worker.Parent()
	}
	if err == nil && parent != nil {
		result.Parent = parent.Name()
		err = buildURLs(api.Router,
			"namespace", namespace.Name(),
			"worker", parent.Name(),
		).
			URL(&result.ParentURL, "worker").
			Error
	}
	var children []coordinate.Worker
	if err == nil {
		children, err = worker.Children()
	}
	if err == nil {
		result.ChildURLs = make([]string, len(children))
		for i, child := range children {
			err = buildURLs(api.Router,
				"namespace", namespace.Name(),
				"worker", child.Name(),
			).
				URL(&result.ChildURLs[i], "worker").
				Error
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		result.Active, err = worker.Active()
	}
	if err == nil {
		result.Mode, err = worker.Mode()
	}
	if err == nil {
		result.Data, err = worker.Data()
	}
	if err == nil {
		result.Expiration, err = worker.Expiration()
	}
	if err == nil {
		result.LastUpdate, err = worker.LastUpdate()
	}
	return err
}

func (api *restAPI) WorkerGet(ctx *context) (interface{}, error) {
	repr := restdata.Worker{}
	err := api.fillWorker(ctx.Namespace, ctx.Worker, &repr)
	if err == nil {
		return repr, nil
	}
	return nil, err
}

func (api *restAPI) WorkerPut(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.Worker)
	if !valid {
		return nil, errUnmarshal
	}

	// Did the parent change?
	oldParent, err := ctx.Worker.Parent()
	oldParentName := ""
	if err == nil && oldParent != nil {
		oldParentName = oldParent.Name()
	}
	if err == nil && repr.Parent != oldParentName {
		var parent coordinate.Worker
		if repr.Parent != "" {
			parent, err = ctx.Namespace.Worker(repr.Parent)
		}
		if err == nil {
			err = ctx.Worker.SetParent(parent)
		}
	}

	// Do we need to deactivate ourselves?  Or update?
	var wasActive bool
	if err == nil {
		wasActive, err = ctx.Worker.Active()
	}
	if err == nil && repr.Active {
		// May as well update; checking everything else is
		// a little irritating
		err = ctx.Worker.Update(repr.Data, repr.LastUpdate, repr.Expiration, repr.Mode)
	} else if err == nil && wasActive {
		// was active, not active now (else we hit the previous block)
		err = ctx.Worker.Deactivate()
	}

	return nil, err
}

func (api *restAPI) WorkerRequestAttempts(ctx *context, in interface{}) (interface{}, error) {
	req, valid := in.(coordinate.AttemptRequest)
	if !valid {
		return nil, errUnmarshal
	}
	attempts, err := ctx.Worker.RequestAttempts(req)
	if err != nil {
		return nil, err
	}
	resp := restdata.AttemptResponse{}
	if len(attempts) == 0 {
		return resp, nil
	}
	// All of the attempts' work units are from the same work spec,
	// so record that one URL for basic utility
	spec := attempts[0].WorkUnit().WorkSpec()
	err = buildURLs(api.Router,
		"namespace", ctx.Namespace.Name(),
		"spec", spec.Name(),
	).URL(&resp.WorkSpecURL, "workSpec").Error
	if err != nil {
		return nil, err
	}
	// Now build the attempt data
	resp.Attempts = make([]restdata.Attempt, len(attempts))
	for i, attempt := range attempts {
		err = api.fillAttempt(ctx.Namespace, attempt, &resp.Attempts[i])
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (api *restAPI) WorkerMakeAttempt(ctx *context, in interface{}) (interface{}, error) {
	req, valid := in.(restdata.AttemptSpecific)
	if !valid {
		return nil, errUnmarshal
	}

	// Find the work spec and unit (they are not in context)
	spec, err := ctx.Namespace.WorkSpec(req.WorkSpec)
	if err != nil {
		return nil, err
	}
	unit, err := spec.WorkUnit(req.WorkUnit)
	if err != nil {
		return nil, err
	}

	// Now we can force the attempt
	attempt, err := ctx.Worker.MakeAttempt(unit, req.Lifetime)
	if err != nil {
		return nil, err
	}

	result := restdata.Attempt{}
	err = api.fillAttempt(ctx.Namespace, attempt, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (api *restAPI) WorkerActiveAttempts(ctx *context) (interface{}, error) {
	attempts, err := ctx.Worker.ActiveAttempts()
	if err != nil {
		return nil, err
	}
	return api.returnAttempts(ctx, attempts)
}

func (api *restAPI) WorkerAllAttempts(ctx *context) (interface{}, error) {
	attempts, err := ctx.Worker.AllAttempts()
	if err != nil {
		return nil, err
	}
	return api.returnAttempts(ctx, attempts)
}

func (api *restAPI) WorkerChildAttempts(ctx *context) (interface{}, error) {
	attempts, err := ctx.Worker.ChildAttempts()
	if err != nil {
		return nil, err
	}
	return api.returnAttempts(ctx, attempts)
}

// PopulateWorker adds worker-specific routes to a router.
// r should be rooted at the root of the Coordinate URL tree, e.g. "/".
func (api *restAPI) PopulateWorker(r *mux.Router) {
	r.Path("/worker").Name("workers").Handler(&resourceHandler{
		Representation: restdata.WorkerShort{},
		Context:        api.Context,
	})
	r.Path("/worker/{worker}").Name("worker").Handler(&resourceHandler{
		Representation: restdata.Worker{},
		Context:        api.Context,
		Get:            api.WorkerGet,
		Put:            api.WorkerPut,
	})
	r.Path("/worker/{worker}/request_attempts").Name("workerRequestAttempts").Handler(&resourceHandler{
		Representation: coordinate.AttemptRequest{},
		Context:        api.Context,
		Post:           api.WorkerRequestAttempts,
	})
	r.Path("/worker/{worker}/make_attempt").Name("workerMakeAttempt").Handler(&resourceHandler{
		Representation: restdata.AttemptSpecific{},
		Context:        api.Context,
		Post:           api.WorkerMakeAttempt,
	})
	r.Path("/worker/{worker}/active_attempts").Name("workerActiveAttempts").Handler(&resourceHandler{
		Representation: restdata.AttemptList{},
		Context:        api.Context,
		Get:            api.WorkerActiveAttempts,
	})
	r.Path("/worker/{worker}/all_attempts").Name("workerAllAttempts").Handler(&resourceHandler{
		Representation: restdata.AttemptList{},
		Context:        api.Context,
		Get:            api.WorkerAllAttempts,
	})
	r.Path("/worker/{worker}/child_attempts").Name("workerChildAttempts").Handler(&resourceHandler{
		Representation: restdata.AttemptList{},
		Context:        api.Context,
		Get:            api.WorkerChildAttempts,
	})
}
