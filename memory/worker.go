package memory

import "github.com/dmaze/goordinate/coordinate"
import "time"

type memWorker struct {
	name           string
	parent         *memWorker
	children       []*memWorker
	activeAttempts []*memAttempt
	attempts       []*memAttempt
	namespace      *memNamespace
}

func newWorker(namespace *memNamespace, name string) *memWorker {
	return &memWorker{
		name:           name,
		children:       make([]*memWorker, 0),
		activeAttempts: make([]*memAttempt, 0),
		attempts:       make([]*memAttempt, 0),
		namespace:      namespace,
	}
}

// coordinate.Worker interface:

func (worker *memWorker) Name() string {
	return worker.name
}

func (worker *memWorker) Parent() (coordinate.Worker, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	if worker.parent == nil {
		return nil, nil
	}
	return worker.parent, nil
}

func (worker *memWorker) Children() ([]coordinate.Worker, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Worker, len(worker.children))
	for i, child := range worker.children {
		result[i] = child
	}
	return result, nil
}

func (worker *memWorker) RequestAttempts(req coordinate.AttemptRequest) ([]coordinate.Attempt, error) {
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

	for _, workSpec := range worker.namespace.workSpecs {
		var bestWorkUnit *memWorkUnit
		for _, workUnit := range workSpec.workUnits {
			// We can do this work unit if it does not have
			// an active attempt, or if the active attempt is
			// expired or a retryable failure
			if workUnit.activeAttempt == nil ||
				(workUnit.activeAttempt.status != coordinate.Expired &&
					workUnit.activeAttempt.status != coordinate.Retryable) {
				// This is "better" than the best work unit
				// if its priority is higher (...TODO...)
				// or its name is alphabetically sooner
				if bestWorkUnit == nil || workUnit.name < bestWorkUnit.name {
					bestWorkUnit = workUnit
				}
			}
		}
		if bestWorkUnit != nil {
			attempt := &memAttempt{
				workUnit:  bestWorkUnit,
				worker:    worker,
				status:    coordinate.Pending,
				data:      bestWorkUnit.data,
				startTime: time.Now(),
			}
			attempt.expirationTime = attempt.startTime.Add(time.Duration(15) * time.Minute)
			bestWorkUnit.activeAttempt = attempt
			bestWorkUnit.attempts = append(bestWorkUnit.attempts, attempt)
			worker.addAttempt(attempt)
			return []coordinate.Attempt{attempt}, nil
		}
	}
	return []coordinate.Attempt{}, nil
}

func (worker *memWorker) ActiveAttempts() ([]coordinate.Attempt, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Attempt, len(worker.activeAttempts))
	for i, attempt := range worker.activeAttempts {
		result[i] = attempt
	}
	return result, nil
}

func (worker *memWorker) AllAttempts() ([]coordinate.Attempt, error) {
	globalLock(worker)
	defer globalUnlock(worker)

	result := make([]coordinate.Attempt, len(worker.activeAttempts))
	for i, attempt := range worker.attempts {
		result[i] = attempt
	}
	return result, nil
}

func (worker *memWorker) ChildAttempts() (result []coordinate.Attempt, err error) {
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
func (worker *memWorker) addAttempt(attempt *memAttempt) {
	worker.attempts = append(worker.attempts, attempt)
	worker.activeAttempts = append(worker.activeAttempts, attempt)
}

// removeAttemptFromList removes an attempt from a list of attempts,
// and returns a new attempt slice without that (or, if it is not there,
// the same attempt slice).
func removeAttemptFromList(attempt *memAttempt, list []*memAttempt) []*memAttempt {
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
	newList := make([]*memAttempt, len(list)-1)
	copy(newList[:attemptI], list[:attemptI])
	copy(newList[attemptI:], list[attemptI+1:])
	return newList
}

// completeAttempt removes an attempt from the active attempts list,
// if it is there.  Assumes the global lock.  Never fails.
func (worker *memWorker) completeAttempt(attempt *memAttempt) {
	worker.activeAttempts = removeAttemptFromList(attempt, worker.activeAttempts)
}

// removeAttempt removes an attempt from the history attempts list,
// if it is there.  Assumes the global lock.  Never fails.
func (worker memWorker) removeAttempt(attempt *memAttempt) {
	worker.attempts = removeAttemptFromList(attempt, worker.attempts)
}

// memory.coordinable interface:

func (worker *memWorker) Coordinate() *memCoordinate {
	return worker.namespace.coordinate
}
