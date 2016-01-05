// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"net/url"
	"time"
)

type attempt struct {
	resource
	Representation restdata.Attempt
	workUnit       *workUnit
	worker         *worker
}

func attemptFromURL(parent *resource, path string, workUnit *workUnit, worker *worker) (a *attempt, err error) {
	a = &attempt{}
	a.URL, err = parent.Template(path, map[string]interface{}{})
	if err == nil {
		err = a.Refresh()
	}
	if err == nil {
		err = a.fillReferences(workUnit, worker)
	}
	if err == nil {
		return a, nil
	}
	return nil, err
}

func (a *attempt) fillReferences(workUnit *workUnit, worker *worker) error {
	var err error
	var url *url.URL

	if err == nil {
		url, err = a.Template(a.Representation.WorkUnitURL, map[string]interface{}{})
	}
	if err == nil && workUnit != nil && workUnit.URL.String() == url.String() {
		a.workUnit = workUnit
	}
	if err == nil && a.workUnit == nil {
		a.workUnit, err = workUnitFromURL(&a.resource, a.Representation.WorkUnitURL, nil)
	}

	if err == nil {
		url, err = a.Template(a.Representation.WorkerURL, map[string]interface{}{})
	}
	if err == nil && worker != nil && worker.URL.String() == url.String() {
		a.worker = worker
	}
	if err == nil && a.worker == nil {
		a.worker, err = workerFromURL(&a.resource, a.Representation.WorkerURL)
	}

	return err
}

func (a *attempt) Refresh() error {
	a.Representation = restdata.Attempt{}
	return a.Get(&a.Representation)
}

func (a *attempt) WorkUnit() coordinate.WorkUnit {
	return a.workUnit
}

func (a *attempt) Worker() coordinate.Worker {
	return a.worker
}

func (a *attempt) Status() (coordinate.AttemptStatus, error) {
	err := a.Refresh()
	if err == nil {
		return a.Representation.Status, nil
	}
	return 0, err
}

func (a *attempt) Data() (map[string]interface{}, error) {
	err := a.Refresh()
	if err == nil {
		return a.Representation.Data, nil
	}
	return nil, err
}

func (a *attempt) StartTime() (time.Time, error) {
	return a.Representation.StartTime, nil
}

func (a *attempt) EndTime() (time.Time, error) {
	err := a.Refresh()
	if err == nil {
		return a.Representation.EndTime, nil
	}
	return time.Time{}, err
}

func (a *attempt) ExpirationTime() (time.Time, error) {
	err := a.Refresh()
	if err == nil {
		return a.Representation.ExpirationTime, nil
	}
	return time.Time{}, err
}

func (a *attempt) Renew(extendDuration time.Duration, data map[string]interface{}) error {
	repr := restdata.AttemptCompletion{
		ExtendDuration: extendDuration,
		Data:           data,
	}
	return a.PostTo(a.Representation.RenewURL, map[string]interface{}{}, repr, nil)
}

func (a *attempt) Expire(data map[string]interface{}) error {
	repr := restdata.AttemptCompletion{Data: data}
	return a.PostTo(a.Representation.ExpireURL, map[string]interface{}{}, repr, nil)
}

func (a *attempt) Finish(data map[string]interface{}) error {
	repr := restdata.AttemptCompletion{Data: data}
	return a.PostTo(a.Representation.FinishURL, map[string]interface{}{}, repr, nil)
}

func (a *attempt) Fail(data map[string]interface{}) error {
	repr := restdata.AttemptCompletion{Data: data}
	return a.PostTo(a.Representation.FailURL, map[string]interface{}{}, repr, nil)
}

func (a *attempt) Retry(data map[string]interface{}, delay time.Duration) error {
	repr := restdata.AttemptCompletion{Data: data, Delay: delay}
	return a.PostTo(a.Representation.RetryURL, map[string]interface{}{}, repr, nil)
}
