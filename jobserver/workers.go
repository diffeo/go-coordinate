package jobserver

import (
	"github.com/dmaze/goordinate/coordinate"
	"time"
)

// WorkerHeartbeat "checks in" with the job server.  It creates a new
// worker record, if required, and marks the worker as active.  It will
// remain active until the next call to WorkerUnregister or until
// expireSeconds have passed.
func (jobs *JobServer) WorkerHeartbeat(
	workerID string,
	mode int,
	expireSeconds float64,
	data map[string]interface{},
	parent string,
) (bool, string, error) {
	worker, err := jobs.Namespace.Worker(workerID)
	if err == nil {
		parentWorker, err := jobs.Namespace.Worker(parent)
		if err == nil {
			err = worker.SetParent(parentWorker)
		}
	}
	if err == nil {
		now := time.Now()
		lifetime := time.Duration(expireSeconds) * time.Second
		expiration := now.Add(lifetime)
		err = worker.Update(data, now, expiration, mode)
	}
	return err == nil, "", err
}

// WorkerUnregister deactivates a specific worker.
func (jobs *JobServer) WorkerUnregister(workerID string) (bool, string, error) {
	worker, err := jobs.Namespace.Worker(workerID)
	if err != nil {
		return false, "", err
	}
	err = worker.Deactivate()
	return err == nil, "", err
}

// ListWorkerModes returns a map where the keys are worker IDs and the
// values are the mode value passed to the most recent call to
// WorkerHeartbeat for that worker..
func (jobs *JobServer) ListWorkerModes() (map[string]int, error) {
	result := make(map[string]int)
	workers, err := jobs.Namespace.Workers()
	if err != nil {
		return nil, err
	}
	for name, worker := range workers {
		mode, err := worker.Mode()
		if err != nil {
			return nil, err
		}
		result[name] = mode
	}
	return result, nil
}

// GetWorkerInfo returns the data dictionary sent with the last
// WorkerHeartbeat call for this worker, plus the key "age_seconds" as
// the time since that last heartbeat.
func (jobs *JobServer) GetWorkerInfo(workerID string) (map[string]interface{}, error) {
	worker, err := jobs.Namespace.Worker(workerID)
	if err != nil {
		return nil, err
	}
	return worker.Data()
}

// ModeCounts counts the number of workers in each reported mode.  It
// is the same as aggregating the result of ListWorkerModes by their
// mode value, and producing a map from mode value to worker count.
func (jobs *JobServer) ModeCounts() (map[int]int, error) {
	result := make(map[int]int)
	workers, err := jobs.Namespace.Workers()
	if err != nil {
		return nil, err
	}
	for _, worker := range workers {
		mode, err := worker.Mode()
		if err != nil {
			return nil, err
		}
		result[mode]++
	}
	return result, nil
}

// WorkerStats retrieves basic statistics on the workers in the
// system.  The returned map has keys "num_workers", the total number
// of active workers; "num_children", the total number of workers with
// parents; and "num_expirable", the same as "num_workers".
func (jobs *JobServer) WorkerStats() (map[string]int, error) {
	var count, children int
	workers, err := jobs.Namespace.Workers()
	if err != nil {
		return nil, err
	}
	for _, worker := range workers {
		count++
		parent, err := worker.Parent()
		if err != nil {
			return nil, err
		}
		if parent != nil {
			children++
		}
	}
	result := map[string]int{
		"num_workers":   count,
		"num_children":  children,
		"num_expirable": count,
	}
	return result, nil
}

// GetChildWorkUnits collects a list of work units being performed by
// immediate children of workerID.  The return value is a map of child
// worker IDs to lists of work unit maps.  Each of the individual work
// unit maps in turn has keys "work_spec_name", "work_unit_key",
// "work_unit_data", "worker_id", and "expires", with the obvious
// meanings.
//
// Note that there is an important layer of indirection here: the
// returned metadata for work units reflects those work units' active
// attempts, which may not be the attempts the worker thinks they are
// doing.  That is, this will report (by a different "worker_id" key)
// that a worker is working on a work unit for which some other worker
// currently owns the active attempt.
func (jobs *JobServer) GetChildWorkUnits(workerID string) (map[string][]map[string]interface{}, string, error) {
	parent, err := jobs.Namespace.Worker(workerID)
	if err != nil {
		return nil, "", err
	}
	children, err := parent.Children()
	if err != nil {
		return nil, "", err
	}
	result := make(map[string][]map[string]interface{})
	for _, child := range children {
		name := child.Name()
		attempts, err := child.ActiveAttempts()
		if err != nil {
			return nil, "", err
		}
		workUnits := make([]map[string]interface{}, len(attempts))
		for i, attempt := range attempts {
			workUnits[i], err = attemptMap(attempt)
			if err != nil {
				return nil, "", err
			}
		}
		result[name] = workUnits
	}
	return result, "", nil
}

// attemptMap turns a single attempt into the map returned by
// GetChildWorkUnits().
func attemptMap(attempt coordinate.Attempt) (map[string]interface{}, error) {
	// First try to swap out attempt for its work unit's actual
	// active attempt.
	workUnit := attempt.WorkUnit()
	activeAttempt, err := workUnit.ActiveAttempt()
	if err != nil {
		return nil, err
	}
	if activeAttempt != nil {
		attempt = activeAttempt
	}

	// Collect extra data we need and build the result
	data, err := attempt.Data()
	if err != nil {
		return nil, err
	}
	expires, err := attempt.ExpirationTime()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	timeLeft := expires.Sub(now)
	result := map[string]interface{}{
		"work_spec_name": workUnit.WorkSpec().Name(),
		"work_unit_key":  workUnit.Name(),
		"work_unit_data": data,
		"worker_id":      attempt.Worker().Name(),
		"expires":        timeLeft.Seconds(),
	}
	return result, nil
}
