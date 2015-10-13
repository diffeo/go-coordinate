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
	now := time.Now()
	expiration := now.Add(time.Duration(15) * time.Minute)
	return &worker{
		name:           name,
		children:       make(map[string]*worker),
		activeAttempts: make([]*attempt, 0),
		attempts:       make([]*attempt, 0),
		namespace:      namespace,
		active:         true,
		lastUpdate:     now,
		expiration:     expiration,
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
	globalLock(w)
	defer globalUnlock(w)

	var attempts []coordinate.Attempt
	if req.NumberOfWorkUnits < 1 {
		req.NumberOfWorkUnits = 1
	}

	// Get the metadata and choose a work spec
	specs, metas := w.namespace.allMetas(true)
	name, err := coordinate.SimplifiedScheduler(metas, req.AvailableGb)
	if err == coordinate.ErrNoWork {
		return attempts, nil
	} else if err != nil {
		return nil, err
	}
	spec := specs[name]
	meta := metas[name]

	// Get more work units, but not more than either the number
	// requested or the maximum allowed
	count := req.NumberOfWorkUnits
	if meta.MaxAttemptsReturned > 0 && count > meta.MaxAttemptsReturned {
		count = meta.MaxAttemptsReturned
	}
	if meta.MaxRunning > 0 && count > meta.PendingCount-meta.MaxRunning {
		count = meta.PendingCount - meta.MaxRunning
	}
	for len(attempts) < count {
		attempt := w.getWorkFromSpec(spec)
		if attempt == nil {
			break
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

// getWorkFromSpec forcibly retrieves a work unit from a work spec.
// It could create a work unit if spec is a continuous spec with no
// available units.  It ignores other constraints, such as whether the
// work spec is paused.
func (w *worker) getWorkFromSpec(spec *workSpec) *attempt {
	if len(spec.available) == 0 {
		return nil
	}
	workUnit := spec.available.Next()
	return w.makeAttempt(workUnit, time.Duration(0))
}

func (w *worker) MakeAttempt(cUnit coordinate.WorkUnit, duration time.Duration) (coordinate.Attempt, error) {
	globalLock(w)
	defer globalUnlock(w)
	unit, ok := cUnit.(*workUnit)
	if !ok {
		return nil, coordinate.ErrWrongBackend
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

	result := make([]coordinate.Attempt, len(w.attempts))
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
