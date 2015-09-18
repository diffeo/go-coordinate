package memory

import "container/heap"

// availableUnits is a priority queue of work units.
type availableUnits []*memWorkUnit
// Add a work unit to this queue in the appropriate spot.
func (q *availableUnits) Add(unit *memWorkUnit) {
	heap.Push(q, unit)
}

// Get the next available unit, with the highest priority and lowest name.
func (q *availableUnits) Next() *memWorkUnit {
	return heap.Pop(q).(*memWorkUnit)
}

// Remove a specific work unit.
func (q *availableUnits) Remove(unit *memWorkUnit) {
	heap.Remove(q, unit.availableIndex)
}

// Reprioritize a specific work unit (when its priority changes).
func (q *availableUnits) Reprioritize(unit *memWorkUnit) {
	heap.Fix(q, unit.availableIndex)
}

// sort.Interface

func (q availableUnits) Len() int {
	return len(q)
}

// isUnitHigherPriority returns true if a is more important than b.
func isUnitHigherPriority(a, b *memWorkUnit) bool {
	if a.priority > b.priority {
		return true
	}
	if a.priority < b.priority {
		return false
	}
	return a.name < b.name
}

func (q availableUnits) Less(i, j int) bool {
	// Remember, position 0 is highest priority.  Sorting says
	// that if q.Units[i] < q.Units[j], then i should be before j.
	// This means the highest-priority thing sorts least...or,
	// Less(i, j) is true iff q.Units[i] is higher priority than
	// q.Units[j].
	return isUnitHigherPriority(q[i], q[j])
}

func (q availableUnits) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].availableIndex = i
	q[j].availableIndex = j
}

// collections/heap.Interface

func (q *availableUnits) Push(x interface{}) {
	unit := x.(*memWorkUnit)
	unit.availableIndex = len(*q)
	*q = append(*q, unit)
}

func (q *availableUnits) Pop() interface{} {
	if len(*q) == 0 {
		return nil
	}
	unit := (*q)[len(*q)-1]
	*q = (*q)[:len(*q)-1]
	unit.availableIndex = -1
	return unit
}
