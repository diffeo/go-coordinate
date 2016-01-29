// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

type worker struct {
	Worker    coordinate.Worker
	namespace *namespace
}

func newWorker(upstream coordinate.Worker, namespace *namespace) *worker {
	return &worker{
		Worker:    upstream,
		namespace: namespace,
	}
}

// Can workers ever return ErrGone?  There's not an obvious way to
// delete them.  The two answers, really, are, that we will eventually
// implement worker expiry, and that deleting a whole namespace will
// take the workers with it.  In any case, I've gotten good enough at
// copy-and-pasting the refresh/with* code that I may as well do this
// right.

// refresh re-fetches the upstream object if possible.  This should be
// called when code strongly expects the cached object is invalid,
// for instance because a method has returned ErrGone.
//
// On success, worker.Worker points at a newly fetched valid object,
// this object remains the cached worker for its name in the namespace
// cache, and returns nil.  On error returns the error from trying to
// fetch the worker.
func (w *worker) refresh() error {
	name := w.Worker.Name()
	var newWorker coordinate.Worker
	err := w.namespace.withNamespace(func(namespace coordinate.Namespace) (err error) {
		newWorker, err = namespace.Worker(name)
		return
	})
	if err == nil {
		w.Worker = newWorker
		return nil
	}
	w.namespace.invalidateWorker(name)
	return err
}

// withWorker calls some function with the current upstream worker If
// that operation returns ErrGone, tries refreshing this object then
// trying again; it may also refresh the namespace.  Note that if
// there is an error doing the refresh, that error is discarded, and
// the original ErrGone is returned (which will be more meaningful to
// the caller).
func (w *worker) withWorker(f func(coordinate.Worker) error) error {
	for {
		err := f(w.Worker)
		if err != coordinate.ErrGone {
			return err
		}
		err = w.refresh()
		if err != nil {
			return coordinate.ErrGone
		}
	}
}

func (w *worker) Name() string {
	return w.Worker.Name()
}

func (w *worker) Parent() (parent coordinate.Worker, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		parent, err = upstream.Parent()
		return
	})
	if err == nil && parent != nil {
		parent = w.namespace.wrapWorker(parent)
	}
	return
}

func (w *worker) SetParent(parent coordinate.Worker) error {
	if wrapped, isWrapped := parent.(*worker); isWrapped {
		parent = wrapped.Worker
	}
	return w.withWorker(func(upstream coordinate.Worker) error {
		return upstream.SetParent(parent)
	})
}

func (w *worker) Children() (children []coordinate.Worker, err error) {
	var kids []coordinate.Worker
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		kids, err = upstream.Children()
		return
	})
	if err == nil && len(kids) > 0 {
		children = make([]coordinate.Worker, len(kids))
		for i, child := range kids {
			children[i] = w.namespace.wrapWorker(child)
		}
	}
	return
}

func (w *worker) Active() (active bool, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		active, err = upstream.Active()
		return
	})
	return
}

func (w *worker) Deactivate() error {
	return w.withWorker(func(upstream coordinate.Worker) (err error) {
		return upstream.Deactivate()
	})
}

func (w *worker) Mode() (mode string, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		mode, err = upstream.Mode()
		return
	})
	return
}

func (w *worker) Data() (data map[string]interface{}, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		data, err = upstream.Data()
		return
	})
	return
}

func (w *worker) Expiration() (exp time.Time, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		exp, err = upstream.Expiration()
		return
	})
	return
}

func (w *worker) LastUpdate() (update time.Time, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		update, err = upstream.LastUpdate()
		return
	})
	return
}

func (w *worker) Update(data map[string]interface{}, now, expiration time.Time, mode string) error {
	return w.withWorker(func(upstream coordinate.Worker) error {
		return upstream.Update(data, now, expiration, mode)
	})
}

func (w *worker) RequestAttempts(req coordinate.AttemptRequest) (attempts []coordinate.Attempt, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		attempts, err = upstream.RequestAttempts(req)
		return
	})
	return
}

func (w *worker) MakeAttempt(unit coordinate.WorkUnit, length time.Duration) (attempt coordinate.Attempt, err error) {
	if wrapped, isWrapped := unit.(*workUnit); isWrapped {
		unit = wrapped.workUnit
	}
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		attempt, err = upstream.MakeAttempt(unit, length)
		return
	})
	return
}

func (w *worker) ActiveAttempts() (attempts []coordinate.Attempt, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		attempts, err = upstream.ActiveAttempts()
		return
	})
	return
}

func (w *worker) AllAttempts() (attempts []coordinate.Attempt, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		attempts, err = upstream.AllAttempts()
		return
	})
	return
}

func (w *worker) ChildAttempts() (attempts []coordinate.Attempt, err error) {
	err = w.withWorker(func(upstream coordinate.Worker) (err error) {
		attempts, err = upstream.ChildAttempts()
		return
	})
	return
}
