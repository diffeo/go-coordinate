package memory

import "testing"

type rig struct {
	t *testing.T
	name string
	q availableUnits
}

// push adds any number of work units to the priority queue, in the order
// of their parameters.
func (rig *rig) push(units ...*memWorkUnit) {
	for _, unit := range units {
		rig.q.Add(unit)
	}
}

// popSpecific pulls a single unit out of the priority queue and asserts
// that it is exactly u.
func (rig *rig) popSpecific(u *memWorkUnit) {
	if rig.q.Len() == 0 {
		rig.t.Errorf("%v: queue is empty (expected %+v)", rig.name, u)
	} else {
		out := rig.q.Next()
		if out != u {
			rig.t.Errorf("%v: popped wrong unit (got %+v, expected %+v)", rig.name, out, u)
		}
	}
}

// checkEmpty asserts that the priority queue is empty.
func (rig *rig) checkEmpty() {
	if rig.q.Len() > 0 {
		rig.t.Errorf("%v: queue is non-empty (%+v)", rig.name, rig.q)
	}
}

// popAll pops units off the priority queue one at a time, comparing
// them to each of the parameters in turn, and asserts that the queue
// is empty at the end.
func (rig *rig) popAll(units ...*memWorkUnit) {
	for _, unit := range units {
		rig.popSpecific(unit)
	}
	rig.checkEmpty()
}

func TestQueueOfOne(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfOne"}
	unit := &memWorkUnit {name: "unit"}
	rig.push(unit)
	rig.popAll(unit)
}

func TestQueueOfTwoInOrder(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfTwoInOrder"}
	first := &memWorkUnit{name: "first"}
	second := &memWorkUnit{name: "second"}
	rig.push(first, second)
	rig.popAll(first, second)
}

func TestQueueOfTwoInWrongOrder(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfTwoInWrongOrder"}
	first := &memWorkUnit{name: "first"}
	second := &memWorkUnit{name: "second"}
	rig.push(second, first)
	rig.popAll(first, second)
}

func TestQueueOfThreeWithPriorities(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfThreeWithPriorities"}
	first := &memWorkUnit{name: "z", priority: 100}
	second := &memWorkUnit{name: "a"}
	third := &memWorkUnit{name: "m"}
	rig.push(second, third, first)
	rig.popAll(first, second, third)
}

func TestDeleteJustOne(t *testing.T) {
	rig := rig{t: t, name: "TestDeleteJustOne"}
	unit := &memWorkUnit{name: "unit"}
	rig.push(unit)
	rig.q.Remove(unit)
	rig.popAll()
}

func TestDeleteFirstOfThree(t *testing.T) {
	rig := rig{t: t, name: "TestDeleteFirstOfThree"}
	first := &memWorkUnit{name: "a"}
	second := &memWorkUnit{name: "b"}
	third := &memWorkUnit{name: "c"}
	rig.push(first, second, third)
	rig.q.Remove(first)
	rig.popAll(second, third)
}

func TestReprioritizeFirst(t *testing.T) {
	rig := rig{t: t, name: "TestReprioritizeFirst"}
	first := &memWorkUnit{name: "a"}
	second := &memWorkUnit{name: "b"}
	third := &memWorkUnit{name: "c"}
	rig.push(first, second, third)
	first.priority = -1
	rig.q.Reprioritize(first)
	rig.popAll(second, third, first)
}

func TestReprioritizeMiddle(t *testing.T) {
	rig := rig{t: t, name: "TestReprioritizeMiddle"}
	first := &memWorkUnit{name: "a"}
	second := &memWorkUnit{name: "b"}
	third := &memWorkUnit{name: "c"}
	rig.push(first, second, third)
	second.priority = 100
	rig.q.Reprioritize(second)
	rig.popAll(second, first, third)
}
