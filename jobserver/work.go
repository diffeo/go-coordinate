// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver

import (
	"errors"
	"fmt"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// GetWorkOptions contains mapped options for the GetWork() call.
type GetWorkOptions struct {
	// AvailableGb contains the amount of memory the worker
	// advertises.  In classic rejester, this limited work specs
	// to needing not more than this min_gb; Python Coordinate
	// ignores this constraint.  If zero, do not enforce this
	// constraint.
	AvailableGb float64 `mapstructure:"available_gb"`

	// LeaseTime specifies the number of seconds to complete the
	// work.  If zero, use a default value of 5 minutes.  Cannot
	// be less than 1 second or more than 1 day.
	LeaseTime int `mapstructure:"lease_time"`

	// MaxJobs indicates the number of jobs requested.  If zero,
	// use 1 instead.  The response to GetWork() is different if
	// this is 1 vs. a greater number.  Fewer jobs may be returned
	// if fewer are available.  All returned work units belong to
	// the same work spec.
	MaxJobs int `mapstructure:"max_jobs"`

	// WorkSpecNames gives a list of work specs to consider.  If
	// not nil, no work specs not in this list will be considered.
	// The list may be further filtered by resource constraints
	// and work unit availability.
	WorkSpecNames []string `mapstructure:"work_spec_names"`
}

// GetWork requests one or more work units to perform.  The work unit
// attempts are associated with workerID, which need not have been
// previously registered.  If there is no work to do, may return
// neither work nor an error.
//
// Each work unit is returned as a cborrpc.PythonTuple holding the
// work spec name, work unit key as a byte slice, and work unit data
// dictionary.  If options does not contain "max_jobs" or if that
// value is 1, returns a tuple or nil, otherwise returns a slice of
// tuples (maybe 1 or none).
func (jobs *JobServer) GetWork(workerID string, options map[string]interface{}) (interface{}, string, error) {
	// This is the Big Kahuna.  The Python Coordinate server tries
	// to be extra clever with its return value, returning None if
	// there is no work, a concrete value if one work unit was
	// requested, and a list if more than one was requested, and
	// this same rule is enforced in the client code.  So, this will
	// return either exactly one PythonTuple or a list of PythonTuple.
	var (
		attempts  []coordinate.Attempt
		err       error
		gwOptions GetWorkOptions
		worker    coordinate.Worker
	)
	err = decode(&gwOptions, options)
	if err == nil {
		worker, err = jobs.Namespace.Worker(workerID)
	}
	if err == nil {
		if gwOptions.MaxJobs < 1 {
			gwOptions.MaxJobs = 1
		}
		req := coordinate.AttemptRequest{
			NumberOfWorkUnits: gwOptions.MaxJobs,
			Runtimes:          []string{""},
			WorkSpecs:         gwOptions.WorkSpecNames,
		}
		attempts, err = worker.RequestAttempts(req)
	}
	if err != nil {
		return nil, "", err
	}
	// successful return
	if gwOptions.MaxJobs == 1 {
		if len(attempts) == 0 {
			tuple := cborrpc.PythonTuple{
				Items: []interface{}{nil, nil, nil},
			}
			return tuple, "", nil
		}
		if len(attempts) == 1 {
			tuple, err := getWorkTuple(attempts[0])
			if err != nil {
				return nil, "", err
			}
			return tuple, "", nil
		}
	}
	result := make([]cborrpc.PythonTuple, len(attempts))
	for i, attempt := range attempts {
		tuple, err := getWorkTuple(attempt)
		if err != nil {
			return nil, "", err
		}
		result[i] = tuple
	}
	return result, "", nil
}

func getWorkTuple(attempt coordinate.Attempt) (cborrpc.PythonTuple, error) {
	data, err := attempt.Data()
	if err != nil {
		return cborrpc.PythonTuple{}, err
	}
	workUnit := attempt.WorkUnit()
	return cborrpc.PythonTuple{Items: []interface{}{
		workUnit.WorkSpec().Name(),
		[]byte(workUnit.Name()),
		data,
	}}, nil
}

// UpdateWorkUnitOptions holds the possible options to the
// UpdateWorkUnit call.
type UpdateWorkUnitOptions struct {
	// LeaseTime specifies the number of additional seconds required
	// to complete the work unit.
	LeaseTime int `mapstructure:"lease_time"`

	// Status specifies the new status of the work unit.
	// Depending on the current status of the work unit, this may
	// start a new attempt or complete an existing attempt.  If
	// zero, make no change in the work unit status, only update
	// the data dictionary and extend an existing attempt's
	// deadline.
	Status WorkUnitStatus

	// Data, if provided, specifies the new data dictionary for
	// the work unit.
	Data map[string]interface{}

	// WorkerID identifies the worker making the request.
	WorkerID string `mapstructure:"worker_id"`
}

// LeaseDuration converts the requested LeaseTime to a duration.
func (opts UpdateWorkUnitOptions) LeaseDuration() time.Duration {
	leaseTime := opts.LeaseTime
	if leaseTime < 1 {
		leaseTime = 300
	}
	return time.Duration(leaseTime) * time.Second
}

// UpdateWorkUnit causes some state change in a work unit.  If the
// work unit is pending, this is the principal interface to complete
// or renew it; if it is already complete this can cause it to be
// retried.
func (jobs *JobServer) UpdateWorkUnit(
	workSpecName string,
	workUnitKey string,
	options map[string]interface{},
) (bool, string, error) {
	// Note that in several corner cases, the behavior of this as
	// written disagrees with Python coordinated's:
	//
	// * If neither "lease_time" nor "status" is specified,
	//   Python coordinated immediately returns False without
	//   checking if workUnitKey is valid
	//
	// * Python coordinated allows arbitrary status changes,
	//   including AVAILABLE -> FINISHED
	//
	// * This openly ignores "worker_id", as distinct from Python
	//   coordinated, which logs an obscure warning and changes it,
	//   but only on a renew
	var (
		attempt    coordinate.Attempt
		changed    bool
		err        error
		status     coordinate.AttemptStatus
		uwuOptions UpdateWorkUnitOptions
		workSpec   coordinate.WorkSpec
		workUnit   coordinate.WorkUnit
	)
	err = decode(&uwuOptions, options)
	if err == nil {
		workSpec, err = jobs.Namespace.WorkSpec(workSpecName)
	}
	if err == nil {
		workUnit, err = workSpec.WorkUnit(workUnitKey)
	}
	if err == nil {
		if workUnit == nil {
			return false, fmt.Sprintf("no such work unit key=%v", workUnitKey), nil
		}
	}
	if err == nil {
		attempt, err = workUnit.ActiveAttempt()
	}
	if err == nil && attempt != nil {
		status, err = attempt.Status()
	}
	if err == nil && attempt != nil {
		if status == coordinate.Expired || status == coordinate.Retryable {
			// The Python Coordinate API sees both of these
			// statuses as "available", and we want to fall
			// into the next block.
			attempt = nil
		}
	}
	if err == nil && attempt == nil {
		// Caller is trying to manipulate an AVAILABLE work
		// unit.  Either they are trying to change the work
		// unit data in place, or they are trying to jump a
		// work unit directly to a completed state.  (The
		// latter is possible during the Python work unit
		// parent cleanup, if the timing is bad.)
		if uwuOptions.Status == Available || uwuOptions.Status == 0 {
			// The only thing we are doing is changing the
			// work unit data.
			if uwuOptions.Data != nil {
				meta, err := workUnit.Meta()
				if err == nil {
					_, err = workSpec.AddWorkUnit(workUnit.Name(), uwuOptions.Data, meta)
				}
				if err == nil {
					changed = true
				}
			}
			return changed && err == nil, "", err
		}
		// Otherwise we are trying to transition to another
		// state; so force-create an attempt.
		worker, err := jobs.Namespace.Worker(uwuOptions.WorkerID)
		if err == nil {
			attempt, err = worker.MakeAttempt(workUnit, uwuOptions.LeaseDuration())
			status = coordinate.Pending
		}
	}
	if err == nil {
		switch status {
		case coordinate.Pending:
			changed = true // or there's an error
			switch uwuOptions.Status {
			case 0, Pending:
				err = attempt.Renew(uwuOptions.LeaseDuration(), uwuOptions.Data)
			case Available:
				err = attempt.Expire(uwuOptions.Data)
			case Finished:
				err = attempt.Finish(uwuOptions.Data)
			case Failed:
				err = attempt.Fail(uwuOptions.Data)
			default:
				err = errors.New("update_work_unit invalid status")
			}
		case coordinate.Expired:
			err = errors.New("update_work_unit logic error, trying to refresh expired unit")
		case coordinate.Finished:
			switch uwuOptions.Status {
			case 0, Finished:
				changed = false // no-op
			case Available:
				err = workUnit.ClearActiveAttempt()
				changed = true
			case Failed:
				changed = false // see below
			default:
				err = errors.New("update_work_unit cannot change finished unit")
			}
		case coordinate.Failed:
			switch uwuOptions.Status {
			case 0, Failed:
				changed = false // no-op
			case Available: // "retry"
				err = workUnit.ClearActiveAttempt()
				changed = true
			case Finished:
				// The Python worker, with two separate
				// processes, has a race wherein there
				// could be 15 seconds to go, the parent
				// kills off the child, and the child
				// finishes successfully, all at the same
				// time.  In that case the successful
				// finish should win.
				err = attempt.Finish(nil)
				changed = true
			default:
				err = errors.New("update_work_unit cannot change failed unit")
			}
		case coordinate.Retryable:
			err = errors.New("update_work_unit logic error, trying to refresh retryable unit")
		default:
			err = fmt.Errorf("update_work_unit invalid attempt status %+v", status)
		}
	}
	return changed && err == nil, "", err
}
