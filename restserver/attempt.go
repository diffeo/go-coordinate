// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
	"time"
)

func (api *restAPI) attemptURLBuilder(namespace coordinate.Namespace, attempt coordinate.Attempt, startTime time.Time, err error) *urlBuilder {
	var startBytes []byte
	unit := attempt.WorkUnit()
	spec := unit.WorkSpec()
	worker := attempt.Worker()
	if err == nil {
		startBytes, err = startTime.MarshalText()
	}
	if err == nil {
		return buildURLs(api.Router,
			"namespace", namespace.Name(),
			"spec", spec.Name(),
			"unit", unit.Name(),
			"worker", worker.Name(),
			"start", string(startBytes),
		)
	}
	return &urlBuilder{Error: err}
}

func (api *restAPI) fillAttemptShort(namespace coordinate.Namespace, attempt coordinate.Attempt, short *restdata.AttemptShort) error {
	var err error
	short.StartTime, err = attempt.StartTime()
	builder := api.attemptURLBuilder(namespace, attempt, short.StartTime, err)
	builder.URL(&short.URL, "attempt")
	builder.URL(&short.WorkUnitURL, "workUnit")
	builder.URL(&short.WorkerURL, "worker")
	return builder.Error
}

func (api *restAPI) fillAttempt(namespace coordinate.Namespace, attempt coordinate.Attempt, repr *restdata.Attempt) error {
	err := api.fillAttemptShort(namespace, attempt, &repr.AttemptShort)
	if err == nil {
		repr.Status, err = attempt.Status()
	}
	if err == nil {
		repr.Data, err = attempt.Data()
	}
	if err == nil {
		repr.EndTime, err = attempt.EndTime()
	}
	if err == nil {
		repr.ExpirationTime, err = attempt.ExpirationTime()
	}
	builder := api.attemptURLBuilder(namespace, attempt, repr.StartTime, err)
	builder.URL(&repr.RenewURL, "attemptRenew")
	builder.URL(&repr.ExpireURL, "attemptExpire")
	builder.URL(&repr.FinishURL, "attemptFinish")
	builder.URL(&repr.FailURL, "attemptFail")
	builder.URL(&repr.RetryURL, "attemptRetry")
	return builder.Error
}

func (api *restAPI) returnAttempts(ctx *context, attempts []coordinate.Attempt) (interface{}, error) {
	resp := restdata.AttemptList{}
	resp.Attempts = make([]restdata.AttemptShort, len(attempts))
	for i, attempt := range attempts {
		err := api.fillAttemptShort(ctx.Namespace, attempt, &resp.Attempts[i])
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (api *restAPI) AttemptGet(ctx *context) (interface{}, error) {
	repr := restdata.Attempt{}
	err := api.fillAttempt(ctx.Namespace, ctx.Attempt, &repr)
	if err != nil {
		return nil, err
	}
	return repr, nil
}

func (api *restAPI) AttemptRenew(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.AttemptCompletion)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.Attempt.Renew(repr.ExtendDuration, repr.Data)
	return nil, err
}

func (api *restAPI) AttemptExpire(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.AttemptCompletion)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.Attempt.Expire(repr.Data)
	return nil, err
}

func (api *restAPI) AttemptFinish(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.AttemptCompletion)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.Attempt.Finish(repr.Data)
	return nil, err
}

func (api *restAPI) AttemptFail(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.AttemptCompletion)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.Attempt.Fail(repr.Data)
	return nil, err
}

func (api *restAPI) AttemptRetry(ctx *context, in interface{}) (interface{}, error) {
	repr, valid := in.(restdata.AttemptCompletion)
	if !valid {
		return nil, errUnmarshal
	}
	err := ctx.Attempt.Retry(repr.Data)
	return nil, err
}

func (api *restAPI) PopulateAttempt(r *mux.Router) {
	r.Path("/attempt").Name("attempts").Handler(&resourceHandler{
		Representation: restdata.AttemptShort{},
		Context:        api.Context,
	})
	r.Path("/attempt/{worker}/{start}").Name("attempt").Handler(&resourceHandler{
		Representation: restdata.AttemptShort{},
		Context:        api.Context,
		Get:            api.AttemptGet,
	})
	r.Path("/attempt/{worker}/{start}/renew").Name("attemptRenew").Handler(&resourceHandler{
		Representation: restdata.AttemptCompletion{},
		Context:        api.Context,
		Post:           api.AttemptRenew,
	})
	r.Path("/attempt/{worker}/{start}/expire").Name("attemptExpire").Handler(&resourceHandler{
		Representation: restdata.AttemptCompletion{},
		Context:        api.Context,
		Post:           api.AttemptExpire,
	})
	r.Path("/attempt/{worker}/{start}/finish").Name("attemptFinish").Handler(&resourceHandler{
		Representation: restdata.AttemptCompletion{},
		Context:        api.Context,
		Post:           api.AttemptFinish,
	})
	r.Path("/attempt/{worker}/{start}/fail").Name("attemptFail").Handler(&resourceHandler{
		Representation: restdata.AttemptCompletion{},
		Context:        api.Context,
		Post:           api.AttemptFail,
	})
	r.Path("/attempt/{worker}/{start}/retry").Name("attemptRetry").Handler(&resourceHandler{
		Representation: restdata.AttemptCompletion{},
		Context:        api.Context,
		Post:           api.AttemptRetry,
	})
}
