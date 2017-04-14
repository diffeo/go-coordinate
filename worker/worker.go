// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package worker provides a library framework for processes that
// execute Coordinate work units.
package worker

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/satori/go.uuid"
)

// Worker is foo.
type Worker struct {
	// Namespace identifies the Coordinate namespace from which
	// work is performed.  This field is required when creating
	// a Worker.
	Namespace coordinate.Namespace

	// Tasks defines the tasks this Worker is capable of running.
	// Work specs must declare "runtime: go", and also have a
	// "task:" field that names one of the tasks in this map.  If
	// a work spec has no "task:", the work spec name is looked up
	// here instead.
	//
	// The task function is called with a context and a slice of
	// at least one attempt.  The context will be canceled when
	// the worker is stopped or if one of the attempts is nearing
	// is expiration time.  The worker can take any reasonable action
	// in response to being signaled, but generally it should stop
	// doing further work and mark all of the attempts as failed.
	//
	// There is guaranteed to be at least one attempt.  All attempts
	// are for the same worker and for the same work spec.
	Tasks map[string]func(context.Context, []coordinate.Attempt)

	// WorkerID provides the name of the worker as seen through the
	// Coordinate API.  If unset, a worker ID will be generated.
	WorkerID string

	// Concurrency states how many sets of attempts should run in
	// parallel.  If unset, uses runtime.NumCPU().
	Concurrency int

	// PollInterval states how often the worker should try to get
	// more work if the previous attempt(s) returned nothing.  If
	// unset, defaults to 1 second.
	PollInterval time.Duration

	// HeartbeatInterval states how often the worker should report
	// its status in the Coordinate worker data, and check for
	// work units that are about to expire.  If unset, defaults to
	// 15 seconds.
	HeartbeatInterval time.Duration

	// MaxAttempts limits the number of attempts that will be
	// returned; it is exactly the
	// coordinate.AttemptRequest.NumberOfWorkUnits parameter.  If
	// unset, use 100.  Note that individual work specs can also
	// limit the number of attempts by setting a "max_getwork"
	// key.
	MaxAttempts int

	// ErrorHandler is called when an error occurs in the worker
	// main loop.
	ErrorHandler func(error)

	// Clock defines a time source for the worker.  If the
	// Coordinate backend was created with an alternate time
	// source, this should match that time source.  Only test code
	// should need to set this.  If unset, uses a time source
	// backed by real wall-clock time.
	Clock clock.Clock

	// parentWorker is a saved Coordinate worker object with ID
	// WorkerID.
	parentWorker coordinate.Worker

	// childWorkers identifies the child workers.
	childWorkers map[string]coordinate.Worker

	// cancellations maps child worker ID to a cancellation function
	// for that worker's context.  These functions are specified to
	// be idempotent.
	cancellations map[string]func()

	// idleWorkers is an unordered list of child worker IDs that
	// do not have work.
	idleWorkers []string

	// systemIdle is set if an attempt to get work returned
	// nothing.  In this case, there will not be another attempt
	// to get work for PollDuration time.
	systemIdle bool
}

var (
	// expirationWarning is a duration such that, if less than
	// this time is remaining to execute a work unit before it
	// expires, the worker will be signaled.
	expirationWarning = 30 * time.Second

	// expirationAlarm is a duration such that, if less than this
	// time is remaining to execute a work unit before it expires,
	// it will be failed instead.
	expirationAlarm = 15 * time.Second
)

// setDefaults sets default values for any Worker fields that are
// uninitialized.
func (w *Worker) setDefaults() {
	if w.WorkerID == "" {
		// May as well use a UUID here, "it's what we've always done"
		w.WorkerID = uuid.NewV4().String()
	}

	if w.Concurrency == 0 {
		w.Concurrency = runtime.NumCPU()
	}

	if w.PollInterval == time.Duration(0) {
		w.PollInterval = time.Duration(1) * time.Second
	}

	if w.HeartbeatInterval == time.Duration(0) {
		w.HeartbeatInterval = time.Duration(15) * time.Second
	}

	if w.MaxAttempts == 0 {
		w.MaxAttempts = 100
	}

	if w.Clock == nil {
		w.Clock = clock.New()
	}
}

// bootstrap creates the minimum required object set for the worker
// object.
func (w *Worker) bootstrap() error {
	w.childWorkers = make(map[string]coordinate.Worker)
	w.cancellations = make(map[string]func())

	// Get the parent worker
	var err error
	w.parentWorker, err = w.Namespace.Worker(w.WorkerID)
	return err
}

// Run runs work units from Coordinate forever, or until the provided
// context is cancelled.  If it returns, either there was a startup
// error connecting to Coordinate, in which case the corresponding
// error is returned, or execution was cancelled, returning nil.  If
// there is an error while trying to get attempts it is ignored.
func (w *Worker) Run(ctx context.Context) error {
	w.setDefaults()
	if err := w.bootstrap(); err != nil {
		return err
	}

	// This channel is signaled in doWork() after
	// RequestAttempts() returns, with a true value if at least
	// one attempt comes back.  If it does signal true, it
	// triggers another worker if possible.
	gotWork := make(chan bool)

	// This channel is signaled at the end of doWork() with the
	// worker ID.  If the most recent signal in gotWork was true,
	// it triggers another worker if possible.
	finished := make(chan string)

	// This channel, if non-nil, is signaled every second, and
	// triggers a worker.  It has a channel only when gotWork
	// transmits a false value.
	var tick <-chan time.Time
	var ticker *clock.Ticker

	// This channel is signaled to run the heartbeat task.
	var heartbeat <-chan time.Time
	heartbeater := w.Clock.Ticker(w.HeartbeatInterval)

	// We need to (asynchronously) kick off the world by telling
	// ourselves that it's okay to get more work units.
	go func() { gotWork <- true }()

	// TODO(dmaze): check for and signal stale workers
	//
	// The Python parent does both of these together every
	// heartbeat_interval (15 seconds).
	//
	// Also, the children in Python land register on creation and
	// unregister on destruction; but they only run a single work
	// unit or for 10 seconds.  Note that "expired" doesn't carry
	// a whole lot of meaning for workers, yet.

	for {
		select {
		case <-ctx.Done():
			// Shutting down
			// TODO(dmaze): don't return *immediately*; wait
			// for all of the workers to clean up
			if ticker != nil {
				ticker.Stop()
			}
			heartbeater.Stop()
			return nil

		case notIdle := <-gotWork:
			// Some worker came back with its
			// RequestAttempts() result.  If notIdle is
			// false, we expect the finished channel to
			// immediately be signaled as well.

			// If the "idle" bit changed, set/cancel the timer
			if w.systemIdle && notIdle {
				ticker.Stop()
				ticker = nil
				tick = nil
			}
			if (!w.systemIdle) && (!notIdle) {
				ticker = w.Clock.Ticker(w.PollInterval)
				tick = ticker.C
			}
			w.systemIdle = !notIdle
			w.maybeDoWork(ctx, gotWork, finished, false)

		case child := <-finished:
			// Some worker finished.
			w.returnIdleChild(child)
			w.maybeDoWork(ctx, gotWork, finished, false)

		case <-tick:
			// The system is idle, and the clock tick
			// fired.  Forcibly start an idle child at
			// this point.  This will eventually cause
			// gotWork to trigger, which will update the
			// systemIdle flag and might trigger even more
			// work.
			w.maybeDoWork(ctx, gotWork, finished, true)

		case <-heartbeat:
			w.heartbeat()
			w.findStaleUnits()
		}
	}
}

// getIdleChild returns the worker ID of a child worker that is not
// currently doing anything, or an empty string.  If the idle workers
// list is empty but the child workers list could have another worker,
// creates a new child.  Removes the returned worker from the idle
// workers list.
//
// Returns an empty string if there is no idle child and one cannot be
// created, including an error returned by the Coordinate backend.
func (w *Worker) getIdleChild() string {
	// Something in the idle workers list?  Just pick one
	if len(w.idleWorkers) > 0 {
		child := w.idleWorkers[0]
		w.idleWorkers = w.idleWorkers[1:]
		return child
	}

	// Can we support another worker?  Create one
	if len(w.childWorkers) < w.Concurrency {
		id := uuid.NewV4().String()
		child, err := w.Namespace.Worker(id)
		if err == nil {
			err = child.SetParent(w.parentWorker)
		}
		if err == nil {
			w.childWorkers[id] = child
			return id
		}
		if w.ErrorHandler != nil {
			w.ErrorHandler(err)
		}
		return ""
	}

	// Otherwise we're busy
	return ""
}

// returnIdleChild puts a child worker back into the idle children
// list, or if the system is idle, shuts down the worker.
func (w *Worker) returnIdleChild(id string) {
	child := w.childWorkers[id]
	if w.systemIdle {
		delete(w.childWorkers, id)
		delete(w.cancellations, id)
		err := child.Deactivate()
		if err != nil && w.ErrorHandler != nil {
			w.ErrorHandler(err)
		}
	} else {
		w.idleWorkers = append(w.idleWorkers, id)
	}
}

// maybeDoWork spawns a new goroutine to do work, if there is an idle
// child worker.  If the system is idle, a new goroutine is never
// generated unless evenIfIdle is true.
func (w *Worker) maybeDoWork(ctx context.Context, gotWork chan<- bool, finished chan<- string, evenIfIdle bool) {
	if w.systemIdle && !evenIfIdle {
		return
	}
	child := w.getIdleChild()
	if child == "" {
		return
	}
	go w.doWork(child, w.childWorkers[child], ctx, gotWork, finished)
}

// doWork gets attempts and runs them.  It assumes it is running in its
// own goroutine.  It signals gotWork when the call to RequestAttempts
// returns, and signals finished immediately before returning.
func (w *Worker) doWork(id string, worker coordinate.Worker, ctx context.Context, gotWork chan<- bool, finished chan<- string) {
	// When we finish, signal the finished channel with our own ID
	defer func() {
		finished <- id
	}()

	attempts, err := worker.RequestAttempts(coordinate.AttemptRequest{
		Runtimes:          []string{"go"},
		NumberOfWorkUnits: w.MaxAttempts,
	})
	if err != nil {
		// Handle the error if we can, but otherwise act just like
		// we got no attempts back
		if w.ErrorHandler != nil {
			w.ErrorHandler(err)
		}
		gotWork <- false
		return
	}
	if len(attempts) == 0 {
		// Nothing to do
		gotWork <- false
		return
	}
	// Otherwise we have actual work (and at least one attempt).
	gotWork <- true

	// See if we can find a task for the work spec
	spec := attempts[0].WorkUnit().WorkSpec()
	task := spec.Name()
	data, err := spec.Data()
	if err == nil {
		aTask, present := data["task"]
		if present {
			bTask, ok := aTask.(string)
			if ok {
				task = bTask
			}
		}
	}

	// Try to find the task function
	var taskFn func(context.Context, []coordinate.Attempt)
	if err == nil {
		taskFn = w.Tasks[task]
		if taskFn == nil {
			err = fmt.Errorf("No such task function %q", task)
		}
	}

	if err == nil {
		taskCtx, cancellation := context.WithCancel(ctx)
		w.cancellations[id] = cancellation
		taskFn(taskCtx, attempts)
		// It appears to be recommended to call this; calling
		// it multiple times is documented to have no effect
		cancellation()
	} else {
		failure := map[string]interface{}{
			"traceback": err.Error(),
		}
		// Try to fail all the attempts, ignoring errors
		for _, attempt := range attempts {
			_ = attempt.Fail(failure)
		}
	}
}

// heartbeat reports the current status of the parent worker.
func (w *Worker) heartbeat() {
	data := map[string]interface{}{
		"cpus":       runtime.NumCPU(),
		"go":         runtime.Version(),
		"goroutines": runtime.NumGoroutine(),
		"pid":        os.Getpid(),
	}
	hostname, err := os.Hostname()
	if err == nil {
		data["hostname"] = hostname
	}
	interfaces, err := net.Interfaces()
	if err == nil {
		var ipaddrs []string
		for _, interf := range interfaces {
			addrs, err := interf.Addrs()
			if err == nil {
				for _, addr := range addrs {
					ipaddrs = append(ipaddrs, addr.String())
				}
			}
		}
		if len(ipaddrs) > 0 {
			data["ipaddrs"] = ipaddrs
		}
	}
	// The Python worker publishes quite a bit of extra
	// information here: "version" is the version of the
	// coordinate package; "working_set" is the complete pip
	// environment; "memory" is a dictionary with system memory
	// statistics.
	//
	// I think this is rarely examined.  Some of it is hard to get
	// portably.  Even the information that's retrieved above
	// could be uninformative if a worker is run in a Docker
	// container (e.g. the pid will always be 1 and the IP address
	// an uninformative host-local address).

	now := w.Clock.Now()
	then := now.Add(time.Duration(15) * time.Minute)
	err = w.parentWorker.Update(data, now, then, "RUN")
	if err != nil && w.ErrorHandler != nil {
		w.ErrorHandler(err)
	}
}

// findStaleUnits looks for work units assigned to child workers of w
// that are about to run out of time, and cancels them.
func (w *Worker) findStaleUnits() {
	// Scan our childrens' attempts to see if any are expiring
	attempts, err := w.parentWorker.ChildAttempts()
	if err != nil {
		return
	}
	now := w.Clock.Now()
	childrenToCancel := make(map[string]struct{})
	for _, attempt := range attempts {
		exp, err := attempt.ExpirationTime()
		if err == nil {
			remaining := exp.Sub(now)
			if remaining < expirationWarning {
				// TODO(dmaze): if the attempt just
				// has a really short lease time, we
				// should let at least half its
				// possible time elapse
				child := attempt.Worker().Name()
				childrenToCancel[child] = struct{}{}
			}
			if remaining < expirationAlarm {
				// Proactively fail the attempt
				err = attempt.Fail(map[string]interface{}{
					"traceback": "timed out",
				})
			}
		}
	}

	// If anything is expiring (we have a list of child worker IDs,
	// not specific attempts) call their cancellation functions
	for child := range childrenToCancel {
		cancellation, ok := w.cancellations[child]
		// *should* always be there but doesn't hurt to check
		if ok {
			cancellation()
		}
	}
}
