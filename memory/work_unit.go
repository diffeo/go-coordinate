package memory

import "github.com/dmaze/goordinate/coordinate"

type memWorkUnit struct {
	name          string
	data          map[string]interface{}
	priority      int
	activeAttempt *memAttempt
	attempts      []*memAttempt
	workSpec      *memWorkSpec
}

// coordinate.WorkUnit interface:

func (unit *memWorkUnit) Name() string {
	return unit.name
}

func (unit *memWorkUnit) Data() (map[string]interface{}, error) {
	return unit.data, nil
}

func (unit *memWorkUnit) WorkSpec() coordinate.WorkSpec {
	return unit.workSpec
}

func (unit *memWorkUnit) Status() (coordinate.WorkUnitStatus, error) {
	globalLock(unit)
	defer globalUnlock(unit)
	return unit.status(), nil
}

func (unit *memWorkUnit) status() coordinate.WorkUnitStatus {
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

func (unit *memWorkUnit) ActiveAttempt() (coordinate.Attempt, error) {
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

func (unit *memWorkUnit) Attempts() ([]coordinate.Attempt, error) {
	globalLock(unit)
	defer globalUnlock(unit)

	result := make([]coordinate.Attempt, len(unit.attempts))
	for i, attempt := range unit.attempts {
		result[i] = attempt
	}
	return result, nil
}

// memory.coordinable interface:

func (unit *memWorkUnit) Coordinate() *memCoordinate {
	return unit.workSpec.namespace.coordinate
}
