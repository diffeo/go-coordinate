package memory

import (
	"fmt"
	"github.com/dmaze/goordinate/coordinate"
	"time"
)

type worker struct {
	name           string
	parent         *worker
	children       []*worker
	activeAttempts []*attempt
	attempts       []*attempt
	namespace      *namespace
}

func newWorker(namespace *namespace, name string) *worker {
	return &worker{
		name:           name,
		children:       make([]*worker, 0),
		activeAttempts: make([]*attempt, 0),
		attempts:       make([]*attempt, 0),
		namespace:      namespace,
	}
}

// coordinate.Worker interface:

func (worker *worker) Name() string {
	return worker.name
}

func (worker *worker) Parent() (coordinate.Worker, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	if worker.parent == nil {
		return nil, nil
	}
	return worker.parent, nil
}

func (worker *worker) Children() ([]coordinate.Worker, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Worker, len(worker.children))
	for i, child := range worker.children {
		result[i] = child
	}
	return result, nil
}

func (worker *worker) RequestAttempts(req coordinate.AttemptRequest) ([]coordinate.Attempt, error) {
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
	globalLock(worker)
	defer globalUnlock(worker)

	var attempts []coordinate.Attempt
	if req.NumberOfWorkUnits < 1 {
		return attempts, nil
	}
	// Get the first work unit, which picks a work spec for the
	// remainder
	attempt := worker.getWork()
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
	fmt.Printf("target=%v (max_jobs=%v max_getwork=%v)\n",
		target, req.NumberOfWorkUnits, attempt.workUnit.workSpec.meta.MaxAttemptsReturned)
	for len(attempts) < target {
		attempt := worker.getWork()
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
func (worker *worker) getWork() *attempt {
	for _, workSpec := range worker.namespace.workSpecs {
		attempt := worker.getWorkFromSpec(workSpec)
		if attempt != nil {
			return attempt
		}
	}
	return nil
}

func (worker *worker) getWorkFromSpec(workSpec *workSpec) *attempt {
	// TODO(dmaze): Something, probably this, should also check
	// workSpec.meta.MaxRunning
	if len(workSpec.available) == 0 ||
		workSpec.meta.Paused {
		return nil
	}
	workUnit := workSpec.available.Next()
	start := time.Now()
	duration := time.Duration(15) * time.Minute
	attempt := &attempt{
		workUnit:       workUnit,
		worker:         worker,
		status:         coordinate.Pending,
		data:           workUnit.data,
		startTime:      start,
		expirationTime: start.Add(duration),
	}
	workUnit.activeAttempt = attempt
	workUnit.attempts = append(workUnit.attempts, attempt)
	worker.addAttempt(attempt)
	return attempt
}

func (worker *worker) ActiveAttempts() ([]coordinate.Attempt, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Attempt, len(worker.activeAttempts))
	for i, attempt := range worker.activeAttempts {
		result[i] = attempt
	}
	return result, nil
}

func (worker *worker) AllAttempts() ([]coordinate.Attempt, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Attempt, len(worker.activeAttempts))
	for i, attempt := range worker.attempts {
		result[i] = attempt
	}
	return result, nil
}

func (worker *worker) ChildAttempts() (result []coordinate.Attempt, err error) {
	globalLock(worker)
	defer globalUnlock(worker)

	for _, child := range worker.children {
		for _, attempt := range child.activeAttempts {
			result = append(result, attempt)
		}
	}
	return
}

// addAttempt adds an attempt to both the active and historic attempts
// list.  Does not check for duplicates.  Assumes the global lock.
// Never fails.
func (worker *worker) addAttempt(attempt *attempt) {
	worker.attempts = append(worker.attempts, attempt)
	worker.activeAttempts = append(worker.activeAttempts, attempt)
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
func (worker *worker) completeAttempt(attempt *attempt) {
	worker.activeAttempts = removeAttemptFromList(attempt, worker.activeAttempts)
}

// removeAttempt removes an attempt from the history attempts list,
// if it is there.  Assumes the global lock.  Never fails.
func (worker worker) removeAttempt(attempt *attempt) {
	worker.attempts = removeAttemptFromList(attempt, worker.attempts)
}

// memory.coordinable interface:

func (worker *worker) Coordinate() *memCoordinate {
	return worker.namespace.coordinate
}
