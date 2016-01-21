// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"time"
)

type worker struct {
	resource
	Representation restdata.Worker
}

func workerFromURL(parent *resource, path string) (*worker, error) {
	w := &worker{}
	var err error
	w.URL, err = parent.Template(path, map[string]interface{}{})
	if err == nil {
		err = w.Refresh()
	}
	return w, err
}

func (w *worker) Refresh() error {
	w.Representation = restdata.Worker{}
	return w.Get(&w.Representation)
}

func (w *worker) Name() string {
	return w.Representation.Name
}

func (w *worker) Parent() (coordinate.Worker, error) {
	var parent *worker
	err := w.Refresh()
	if err == nil && w.Representation.Parent != nil && *w.Representation.Parent != "" {
		parent, err = workerFromURL(&w.resource, w.Representation.ParentURL)
	}
	if err == nil && parent == nil {
		return nil, nil
	}
	return parent, err
}

func (w *worker) SetParent(parent coordinate.Worker) error {
	parentName := ""
	if parent != nil {
		parentName = parent.Name()
	}
	repr := restdata.Worker{Parent: &parentName}
	return w.Put(repr, nil)
}

func (w *worker) Children() ([]coordinate.Worker, error) {
	var children []coordinate.Worker
	err := w.Refresh()
	if err == nil {
		children = make([]coordinate.Worker, len(w.Representation.ChildURLs))
		for i, url := range w.Representation.ChildURLs {
			children[i], err = workerFromURL(&w.resource, url)
			if err != nil {
				return nil, err
			}
		}
	}
	if err == nil {
		return children, nil
	}
	return nil, err
}

func (w *worker) Active() (bool, error) {
	err := w.Refresh()
	return w.Representation.Active, err
}

func (w *worker) Deactivate() error {
	repr := restdata.Worker{Active: false}
	return w.Put(repr, nil)
}

func (w *worker) Mode() (string, error) {
	err := w.Refresh()
	return w.Representation.Mode, err
}

func (w *worker) Data() (map[string]interface{}, error) {
	err := w.Refresh()
	return w.Representation.Data, err
}

func (w *worker) Expiration() (time.Time, error) {
	err := w.Refresh()
	return w.Representation.Expiration, err
}

func (w *worker) LastUpdate() (time.Time, error) {
	err := w.Refresh()
	return w.Representation.LastUpdate, err
}

func (w *worker) Update(data map[string]interface{}, now, expiration time.Time, mode string) error {
	repr := restdata.Worker{
		Active:     true,
		Data:       data,
		Mode:       mode,
		Expiration: expiration,
		LastUpdate: now,
	}
	return w.Put(repr, nil)
}

func (w *worker) RequestAttempts(req coordinate.AttemptRequest) ([]coordinate.Attempt, error) {
	var resp restdata.AttemptResponse
	err := w.PostTo(w.Representation.RequestAttemptsURL, map[string]interface{}{}, req, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Attempts) == 0 {
		return nil, nil
	}
	spec, err := workSpecFromURL(&w.resource, resp.WorkSpecURL)
	if err != nil {
		return nil, err
	}
	attempts := make([]coordinate.Attempt, len(resp.Attempts))
	for i, attemptRepr := range resp.Attempts {
		unit, err := workUnitFromURL(&w.resource, attemptRepr.WorkUnitURL, spec)
		if err != nil {
			return nil, err
		}
		url, err := w.Template(attemptRepr.URL, map[string]interface{}{})
		if err != nil {
			return nil, err
		}

		attempts[i] = &attempt{
			resource:       resource{URL: url},
			Representation: attemptRepr,
			workUnit:       unit,
			worker:         w,
		}
	}
	return attempts, nil
}

func (w *worker) MakeAttempt(unit coordinate.WorkUnit, lifetime time.Duration) (coordinate.Attempt, error) {
	req := restdata.AttemptSpecific{
		WorkSpec: unit.WorkSpec().Name(),
		WorkUnit: unit.Name(),
		Lifetime: lifetime,
	}
	var a attempt
	err := w.PostTo(w.Representation.MakeAttemptURL, map[string]interface{}{}, req, &a.Representation)
	if err != nil {
		return nil, err
	}

	a.URL, err = w.URL.Parse(a.Representation.URL)
	if err != nil {
		return nil, err
	}
	aUnit, _ := unit.(*workUnit)
	err = a.fillReferences(aUnit, w)
	if err != nil {
		return nil, err
	}

	return &a, nil
}

func (w *worker) returnAttempts(path string) ([]coordinate.Attempt, error) {
	repr := restdata.AttemptList{}
	err := w.GetFrom(path, map[string]interface{}{}, &repr)
	if err != nil {
		return nil, err
	}
	attempts := make([]coordinate.Attempt, len(repr.Attempts))
	for i, attempt := range repr.Attempts {
		// TODO(dmaze): This loop is going to involve a ton of
		// repeated fetching, since we have no caching at all.
		// We will do the single optimization that an attempt
		// whose worker URL is our own URL reuses this worker,
		// which helps ActiveAttempts() and AllAttempts().
		// Another corollary of this is that every return from
		// ChildAttempts() has a distinct worker object, even
		// though there will likely be several for each.
		var aWorker *worker
		if attempt.WorkerURL == w.Representation.URL {
			aWorker = w
		}
		attempts[i], err = attemptFromURL(&w.resource, attempt.URL, nil, aWorker)
		if err != nil {
			return nil, err
		}
	}
	return attempts, nil
}

func (w *worker) ActiveAttempts() ([]coordinate.Attempt, error) {
	return w.returnAttempts(w.Representation.ActiveAttemptsURL)
}

func (w *worker) AllAttempts() ([]coordinate.Attempt, error) {
	return w.returnAttempts(w.Representation.AllAttemptsURL)
}

func (w *worker) ChildAttempts() ([]coordinate.Attempt, error) {
	return w.returnAttempts(w.Representation.ChildAttemptsURL)
}
