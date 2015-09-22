package memory

import (
	"errors"
	"github.com/dmaze/goordinate/coordinate"
	"time"
)

type worker struct {
	name           string
	parent         *worker
	children       map[string]*worker
	data           map[string]interface{}
	active         bool
	expiration     time.Time
	lastUpdate     time.Time
	mode           int
	activeAttempts []*attempt
	attempts       []*attempt
	namespace      *namespace
}

func newWorker(namespace *namespace, name string) *worker {
	return &worker{
		name:           name,
		children:       make(map[string]*worker),
		activeAttempts: make([]*attempt, 0),
		attempts:       make([]*attempt, 0),
		namespace:      namespace,
	}
}

// coordinate.Worker interface:

func (w *worker) Name() string {
	return w.name
}

func (w *worker) Parent() (coordinate.Worker, error) {
	globalLock(w)
	defer globalUnlock(w)

	if w.parent == nil {
		return nil, nil
	}
	return w.parent, nil
}

func (w *worker) SetParent(parent coordinate.Worker) error {
	oldParent := w.parent
	newParent, ok := parent.(*worker)
	if !ok {
		return errors.New("cannot set parent from a different backend")
	}
	if oldParent == newParent {
		return nil // no-op
	}
	if oldParent != nil {
		delete(oldParent.children, w.name)
	}
	if newParent != nil {
		newParent.children[w.name] = w
	}
	w.parent = newParent
	return nil
}

func (w *worker) Children() ([]coordinate.Worker, error) {
	globalLock(w)
	defer globalUnlock(w)

	var result []coordinate.Worker
	for _, child := range w.children {
		result = append(result, child)
	}
	return result, nil
}

func (w *worker) Active() (bool, error) {
	globalLock(w)
	defer globalUnlock(w)
	return w.active, nil
}

func (w *worker) Deactivate() error {
	globalLock(w)
	defer globalUnlock(w)
	w.active = false
	return nil
}

func (w *worker) Mode() (int, error) {
	globalLock(w)
	defer globalUnlock(w)
	return w.mode, nil
}

func (w *worker) Data() (map[string]interface{}, error) {
	globalLock(w)
	defer globalUnlock(w)
	return w.data, nil
}

func (w *worker) Expiration() (time.Time, error) {
	globalLock(w)
	defer globalUnlock(w)
	return w.expiration, nil
}

func (w *worker) LastUpdate() (time.Time, error) {
	globalLock(w)
	defer globalUnlock(w)
	return w.lastUpdate, nil
}

func (w *worker) Update(data map[string]interface{}, now, expiration time.Time, mode int) error {
	globalLock(w)
	defer globalUnlock(w)
	w.active = true
	w.data = data
	w.lastUpdate = now
	w.expiration = expiration
	w.mode = mode
	return nil
}

func (w *worker) RequestAttempts(req coordinate.AttemptRequest) ([]coordinate.Attempt, error) {
	// This is a stub implementation that returns the first work
	// unit from the first work spec we can find that has any work
	// units at all.  There are some interesting variations, like
	// continuous work specs, that we need to consider, plus we
	// actually want to write a real scheduler.
	//
	// The actual flow here should be:
	//
	// (1) Figure out which work specs can run, because req
	//     satisfies their resource constraints (including maximum
	//     concurrent jobs) and either they have at least one work
	//     unit or are continuous
	//
	// (2) Choose one of these work specs, taking as input the
	//     work specs' stated priority, weight, number of pending
	//     jobs, and continuous flag
	//
	// (3) From the chosen work spec, choose a work unit, based
	//     principally on the work units' stated priorities
	//
	// (4) Create an attempt and change the active attempt from
	//     "none" to the new attempt
	//
	// (5) If (4) fails, start over
	//
	// Note that steps 2 and 3 are largely backend-independent
	// and probably want to be done consistently across backends.
	globalLock(w)
	defer globalUnlock(w)

	var attempts []coordinate.Attempt
	if req.NumberOfWorkUnits < 1 {
		req.NumberOfWorkUnits = 1
	}
	// Get the first work unit, which picks a work spec for the
	// remainder
	attempt := w.getWork()
	if attempt == nil {
		return attempts, nil
	}
	attempts = append(attempts, attempt)
	// Get more work units, but not more than either the number
	// requested or the maximum allowed
	target := attempt.workUnit.workSpec.meta.MaxAttemptsReturned
	if target == 0 || target > req.NumberOfWorkUnits {
		target = req.NumberOfWorkUnits
	}
	for len(attempts) < target {
		attempt := w.getWork()
		if attempt == nil {
			break
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

// getWork finds an available work unit, creates an attempt for it,
// sets that attempt as the active attempt for the work unit, adds
// the attempt to the worker's active and history attempts list, and
// returns it.  If there is no work to be had, returns nil.
func (w *worker) getWork() *attempt {
	for _, workSpec := range w.namespace.workSpecs {
		attempt := w.getWorkFromSpec(workSpec)
		if attempt != nil {
			return attempt
		}
	}
	return nil
}

func (w *worker) getWorkFromSpec(workSpec *workSpec) *attempt {
	// TODO(dmaze): Something, probably this, should also check
	// workSpec.meta.MaxRunning
	if len(workSpec.available) == 0 ||
		workSpec.meta.Paused {
		return nil
	}
	workUnit := workSpec.available.Next()
	return w.makeAttempt(workUnit, time.Duration(0))
}

func (w *worker) MakeAttempt(cUnit coordinate.WorkUnit, duration time.Duration) (coordinate.Attempt, error) {
	globalLock(w)
	defer globalUnlock(w)
	unit, ok := cUnit.(*workUnit)
	if !ok {
		return nil, errors.New("cannot make attempt for unit from a different backend")
	}
	attempt := w.makeAttempt(unit, duration)
	return attempt, nil
}

// makeAttempt creates an attempt and makes it the active attempt.
// This is the implementation for MakeAttempt(), and also is called at
// the bottom of the stack for RequestAttempts().  Assumes the global
// lock and never fails.
func (w *worker) makeAttempt(workUnit *workUnit, duration time.Duration) *attempt {
	start := time.Now()
	if duration == time.Duration(0) {
		duration = time.Duration(15) * time.Minute
	}
	attempt := &attempt{
		workUnit:       workUnit,
		worker:         w,
		status:         coordinate.Pending,
		data:           workUnit.data,
		startTime:      start,
		expirationTime: start.Add(duration),
	}
	workUnit.activeAttempt = attempt
	workUnit.attempts = append(workUnit.attempts, attempt)
	w.addAttempt(attempt)
	return attempt
}

func (w *worker) ActiveAttempts() ([]coordinate.Attempt, error) {
	globalLock(w)
	defer globalUnlock(w)

	result := make([]coordinate.Attempt, len(w.activeAttempts))
	for i, attempt := range w.activeAttempts {
		result[i] = attempt
	}
	return result, nil
}

func (w *worker) AllAttempts() ([]coordinate.Attempt, error) {
	globalLock(w)
	defer globalUnlock(w)

	result := make([]coordinate.Attempt, len(w.activeAttempts))
	for i, attempt := range w.attempts {
		result[i] = attempt
	}
	return result, nil
}

func (w *worker) ChildAttempts() (result []coordinate.Attempt, err error) {
	globalLock(w)
	defer globalUnlock(w)

	for _, child := range w.children {
		for _, attempt := range child.activeAttempts {
			result = append(result, attempt)
		}
	}
	return
}

// addAttempt adds an attempt to both the active and historic attempts
// list.  Does not check for duplicates.  Assumes the global lock.
// Never fails.
func (w *worker) addAttempt(attempt *attempt) {
	w.attempts = append(w.attempts, attempt)
	w.activeAttempts = append(w.activeAttempts, attempt)
}

// removeAttemptFromList removes an attempt from a list of attempts,
// and returns a new attempt slice without that (or, if it is not there,
// the same attempt slice).
func removeAttemptFromList(attempt *attempt, list []*attempt) []*attempt {
	// Find the attempt in the active attempts list
	attemptI := -1
	for i, active := range list {
		if active == attempt {
			attemptI = i
			break
		}
	}
	if attemptI == -1 {
		// not there; just stop
		return list
	}
	// Now make a new attempts list without that
	return append(list[:attemptI], list[attemptI+1:]...)
}

// completeAttempt removes an attempt from the active attempts list,
// if it is there.  Assumes the global lock.  Never fails.
func (w *worker) completeAttempt(attempt *attempt) {
	w.activeAttempts = removeAttemptFromList(attempt, w.activeAttempts)
}

// removeAttempt removes an attempt from the history attempts list,
// if it is there.  Assumes the global lock.  Never fails.
func (w *worker) removeAttempt(attempt *attempt) {
	w.attempts = removeAttemptFromList(attempt, w.attempts)
}

// memory.coordinable interface:

func (w *worker) Coordinate() *memCoordinate {
	return w.namespace.coordinate
}
