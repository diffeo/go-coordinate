package coordinate

import (
	"errors"
	"math/rand"
)

// This file provides two schedulers to choose work for a
// Worker.AttemptRequests() call.
//
// TODO(dmaze): How is the caller supposed to choose?

// SimplifiedScheduler chooses a work spec to do work from a mapping
// of work spec metadata, including counts.  It works as follows:
//
//     * Remove all work specs that have no available work, including
//       continuous work specs that have pending work units already
//     * Find the highest priority score of all remaining work specs,
//       and remove all work specs that do not have the highest priority
//     * Choose one of the remaining work specs randomly, trying to
//       make the number of pending jobs be proportional to the work
//       specs' weights
//
// This means that work spec priority is absolute (higher priority
// always preempts lower priority), and weights affect how often jobs
// will run but are not absolute.  The NextWorkSpec metadata field
// ("then" work spec key) does not affect scheduling.
//
// Returns the name of the selected work spec.  If none of the work
// specs have work (that is, no work specs have available work units,
// and continuous work specs already have jobs pending) returns
// ErrNoWork.
func SimplifiedScheduler(metas map[string]*WorkSpecMeta, availableGb float64) (string, error) {
	var candidates map[string]struct{}
	var highestPriority int

	// Prune the work spec list
	for name, meta := range metas {
		// Filter on core metadata
		if meta.Paused ||
			(meta.MaxRunning > 0 && meta.PendingCount >= meta.MaxRunning) ||
			(!meta.Continuous && meta.AvailableCount == 0) ||
			(meta.Continuous && meta.AvailableCount == 0 && meta.PendingCount > 0) {
			continue
		}
		// Filter on priority
		if candidates == nil {
			// No candidates yet; this is definitionally "best"
			candidates = make(map[string]struct{})
			highestPriority = meta.Priority
		} else if meta.Priority < highestPriority {
			// Lower than the highest priority, uninteresting
			continue
		} else if meta.Priority > highestPriority {
			// Higher priority than existing max; all current
			// candidates should be discarded
			candidates = make(map[string]struct{})
			highestPriority = meta.Priority
		}
		// Or else meta.Priority == highestPriority and it is a
		// candidate
		candidates[name] = struct{}{}
	}
	// If this found no candidates, stop
	if candidates == nil {
		return "", ErrNoWork
	}
	// Choose one of candidates as follows: posit there will be
	// one more pending work unit.  We want the ratio of the pending
	// counts to match the ratio of the weight, so each work spec
	// "wants" (weight / total weight) * (total pending + 1) work
	// units of the new total.  The number of "additional" work units
	// needed, for each work spec, is
	//
	// (weight / total weight) * (total pending + 1) - pending
	//
	// (and the sum of this across all work specs is 1).  Drop all
	// negative scores (there must be at least one positive
	// score).  We choose a candidate work spec with weight
	// proportional to these scores.  The same proportions hold,
	// and you are still in integer space, multiplying by the
	// total weight, so the score is
	//
	// weight * (total pending + 1) - total weight * pending
	scores := make(map[string]int)
	var totalScore, totalWeight, totalPending int
	// Count some totals
	for name := range candidates {
		totalWeight += metas[name].Weight
		totalPending += metas[name].PendingCount
	}
	// Assign some scores
	for name := range candidates {
		score := metas[name].Weight*(totalPending+1) - totalWeight*metas[name].PendingCount
		if score > 0 {
			scores[name] = score
			totalScore += score
		}
	}
	// Now pick one with the correct relative weight
	score := rand.Intn(totalScore)
	for name, myScore := range scores {
		if score < myScore {
			return name, nil
		}
		score -= myScore
	}
	// The preceding loop always should have picked a candidate
	panic(errors.New("SimplifiedScheduler didn't pick a candidate"))
}

// ThenChainScheduler choose a work spec to do work from a mapping of
// work spec metadata, including counts.  It replicates the scheduling
// algorithm in the Python Coordinate daemon.  It works as follows:
//
//     * Analyze all work specs and their NextWorkSpec metadata fields
//       ("then" work spec keys) and build up a graph of work specs
//     * Classify each work spec as a source (the start of a work spec
//       chain), part of a chain, or part of a loop
//     * Choose "sources", "chains", or "loops" with a fixed probability
//     * If "chains", choose the most-downstream work spec in each
//       chain that has work; otherwise choose all work specs
//     * Assign a score to each work spec based on its assigned weight
//       and the number of pending work units it currently has, aiming
//       to make the ratio of pending work units match the ratio of
//       weights
//     * Choose the work spec with the highest score
//
// If work spec "a" sets NextWorkSpec "b" in its metadata, then if "b"
// has work, it will always be done before "a".  Setting
// NextWorkSpecPreempts (or the "then_preempts" work spec key) to
// false breaks this chain; the analysis phase simply does not treat
// "b" as a successor of "a" in this case.
//
// If the number of workers is small, this scheduler can starve
// low-weight work specs.  The Python implementation saved the graph
// of work specs; since practical experience suggests the number of
// work specs is small, this simply repeats it on every call.
// Practical experience also suggests that the pattern of a pipeline
// of work specs, chained by NextWorkSpec, where the system wants to
// emphasize completing work over leaving it mid-pipeline, is rare,
// though accommodating that was the original design goal of this
// scheduler.  Appropriate setting of priorities can make
// SimplifiedScheduler have the same effect in a more comprehensible
// framework.
//
// If none of the work specs have work (that is, no work specs have
// available work units, and continuous work specs already have jobs
// pending) returns ErrNoWork.
func ThenChainScheduler(map[string]*WorkSpecMeta) (string, error) {
	return "", ErrNoWork
}
