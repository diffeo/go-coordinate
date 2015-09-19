package memory

import (
	"github.com/dmaze/goordinate/coordinate"
)

type workUnit struct {
	name           string
	data           map[string]interface{}
	priority       float64
	activeAttempt  *attempt
	attempts       []*attempt
	workSpec       *workSpec
	availableIndex int
}

// coordinate.WorkUnit interface:

func (unit *workUnit) Name() string {
	return unit.name
}

func (unit *workUnit) Data() (map[string]interface{}, error) {
	return unit.data, nil
}

func (unit *workUnit) WorkSpec() coordinate.WorkSpec {
	return unit.workSpec
}

func (unit *workUnit) Status() (coordinate.WorkUnitStatus, error) {
	globalLock(unit)
	defer globalUnlock(unit)
	return unit.status(), nil
}

func (unit *workUnit) status() coordinate.WorkUnitStatus {
	if unit.activeAttempt == nil {
		return coordinate.AvailableUnit
	}
	switch unit.activeAttempt.status {
	case coordinate.Pending:
		return coordinate.PendingUnit
	case coordinate.Expired:
		return coordinate.AvailableUnit
	case coordinate.Finished:
		return coordinate.FinishedUnit
	case coordinate.Failed:
		return coordinate.FailedUnit
	case coordinate.Retryable:
		return coordinate.AvailableUnit
	default:
		panic("invalid attempt status")
	}
}

func (unit *workUnit) Priority() (float64, error) {
	globalLock(unit)
	defer globalUnlock(unit)
	return unit.priority, nil
}

func (unit *workUnit) SetPriority(priority float64) error {
	globalLock(unit)
	defer globalUnlock(unit)
	unit.priority = priority
	unit.workSpec.available.Reprioritize(unit)
	return nil
}

func (unit *workUnit) ActiveAttempt() (coordinate.Attempt, error) {
	globalLock(unit)
	defer globalUnlock(unit)
	// Since this returns an interface type, if we just return
	// unit.activeAttempt, we will get back a nil with a concrete
	// type which is not equal to nil with interface type. Go Go
	// go!
	if unit.activeAttempt == nil {
		return nil, nil
	}
	return unit.activeAttempt, nil
}

// resetAttempt clears the active attempt for a unit and returns it
// to its work spec's available list.  Assumes the global lock.
func (unit *workUnit) resetAttempt() {
	if unit.activeAttempt != nil {
		unit.activeAttempt = nil
		unit.workSpec.available.Add(unit)
	}
}

func (unit *workUnit) ClearActiveAttempt() error {
	globalLock(unit)
	defer globalUnlock(unit)
	unit.resetAttempt()
	return nil
}

func (unit *workUnit) Attempts() ([]coordinate.Attempt, error) {
	globalLock(unit)
	defer globalUnlock(unit)

	result := make([]coordinate.Attempt, len(unit.attempts))
	for i, attempt := range unit.attempts {
		result[i] = attempt
	}
	return result, nil
}

// memory.coordinable interface:

func (unit *workUnit) Coordinate() *memCoordinate {
	return unit.workSpec.namespace.coordinate
}
