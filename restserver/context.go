// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"errors"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// errUnmarshal is returned if the put/post contract is violated and
// a handler function is passed the wrong type.
var errUnmarshal = restdata.ErrBadRequest{
	Err: errors.New("Invalid input format"),
}

// context holds all of the information and objects that can be extracted
// from URL parameters.
type context struct {
	Namespace   coordinate.Namespace
	WorkSpec    coordinate.WorkSpec
	WorkUnit    coordinate.WorkUnit
	Attempt     coordinate.Attempt
	Worker      coordinate.Worker
	QueryParams url.Values
}

func (api *restAPI) Context(req *http.Request) (ctx *context, err error) {
	ctx = &context{}
	ctx.QueryParams = req.URL.Query()
	vars := mux.Vars(req)

	var present bool
	var namespace, spec, unit, worker, start string

	if namespace, present = vars["namespace"]; present && err == nil {
		namespace, err = restdata.MaybeDecodeName(namespace)
		if err == nil {
			ctx.Namespace, err = api.Coordinate.Namespace(namespace)
		}
	}

	if spec, present = vars["spec"]; present && err == nil && ctx.Namespace != nil {
		spec, err = restdata.MaybeDecodeName(spec)
		if err == nil {
			ctx.WorkSpec, err = ctx.Namespace.WorkSpec(spec)
		}
		if _, missing := err.(coordinate.ErrNoSuchWorkSpec); missing {
			err = restdata.ErrNotFound{Err: err}
		}
	}

	if unit, present = vars["unit"]; present && err == nil && ctx.WorkSpec != nil {
		unit, err = restdata.MaybeDecodeName(unit)
		if err == nil {
			ctx.WorkUnit, err = ctx.WorkSpec.WorkUnit(unit)
		}
		// In all cases, if there is a work unit key in the URL
		// and that names an absent work unit, it's a missing
		// URL and we should return 404
		if err == nil && ctx.WorkUnit == nil {
			err = restdata.ErrNotFound{Err: coordinate.ErrNoSuchWorkUnit{Name: unit}}
		}
	}

	if worker, present = vars["worker"]; present && err == nil && ctx.Namespace != nil {
		worker, err = restdata.MaybeDecodeName(worker)
		if err == nil {
			ctx.Worker, err = ctx.Namespace.Worker(worker)
		}
	}

	if start, present = vars["start"]; present && err == nil {
		start, err = restdata.MaybeDecodeName(start)
	}

	if err == nil && ctx.WorkUnit != nil && ctx.Worker != nil && start != "" {
		// This is enough information to try to find a worker.
		// Guess that, of these things, the work unit will have
		// the fewest attempts, and scanning them in linear time
		// is sane.  We won't be able to exactly match times
		// but we can check that their serializations match.
		// (Even this isn't foolproof because of time zones.)
		var attempts []coordinate.Attempt
		attempts, err = ctx.WorkUnit.Attempts()
		if err == nil {
			for _, attempt := range attempts {
				var startTime time.Time
				startTime, err = attempt.StartTime()
				if err != nil {
					break
				}
				myStart := startTime.Format(time.RFC3339)
				if attempt.Worker().Name() == ctx.Worker.Name() && myStart == start {
					ctx.Attempt = attempt
					break
				}
			}
		}
		// If we had all of these things, we clearly were
		// expecting to find an attempt, so fail if we didn't.
		if err == nil && ctx.Attempt == nil {
			err = restdata.ErrNotFound{Err: errors.New("no such attempt")}
		}
	}

	return
}

// BoolParam looks at ctx.QueryParams for a parameter named name.  If
// it has a normally-truthy value (1, on, false, no, ...) then return
// that value.  Otherwise (empty string, foo, ...) return def.
func (ctx *context) BoolParam(name string, def bool) bool {
	switch strings.ToLower(ctx.QueryParams.Get(name)) {
	case "0", "f", "n", "false", "off", "no":
		return false
	case "1", "t", "y", "true", "on", "yes":
		return true
	default:
		return def
	}
}

// Build a work unit query from query parameters.  This can fail (if
// invalid statuses are named, if a non-integer limit is provided)
// so it should only be called if a specific route wants it.
func (ctx *context) WorkUnitQuery() (q coordinate.WorkUnitQuery, err error) {
	q.Names = ctx.QueryParams["name"]
	if len(ctx.QueryParams["status"]) > 0 {
		q.Statuses = make([]coordinate.WorkUnitStatus, len(ctx.QueryParams["status"]))
		for i, status := range ctx.QueryParams["status"] {
			err = q.Statuses[i].UnmarshalText([]byte(status))
			if err != nil {
				return
			}
		}
	}
	q.PreviousName = ctx.QueryParams.Get("previous")
	limit := ctx.QueryParams.Get("limit")
	if limit != "" {
		q.Limit, err = strconv.Atoi(limit)
	}
	return
}
