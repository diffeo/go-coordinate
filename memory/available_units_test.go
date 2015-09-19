package memory

import "testing"

type rig struct {
	t    *testing.T
	name string
	q    availableUnits
}

// push adds any number of work units to the priority queue, in the order
// of their parameters.
func (rig *rig) push(units ...*workUnit) {
	for _, unit := range units {
		rig.q.Add(unit)
	}
}

// popSpecific pulls a single unit out of the priority queue and asserts
// that it is exactly u.
func (rig *rig) popSpecific(u *workUnit) {
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
func (rig *rig) popAll(units ...*workUnit) {
	for _, unit := range units {
		rig.popSpecific(unit)
	}
	rig.checkEmpty()
}

func TestQueueOfOne(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfOne"}
	unit := &workUnit{name: "unit"}
	rig.push(unit)
	rig.popAll(unit)
}

func TestQueueOfTwoInOrder(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfTwoInOrder"}
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	rig.push(first, second)
	rig.popAll(first, second)
}

func TestQueueOfTwoInWrongOrder(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfTwoInWrongOrder"}
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	rig.push(second, first)
	rig.popAll(first, second)
}

func TestQueueOfThreeWithPriorities(t *testing.T) {
	rig := rig{t: t, name: "TestQueueOfThreeWithPriorities"}
	first := &workUnit{name: "z", priority: 100}
	second := &workUnit{name: "a"}
	third := &workUnit{name: "m"}
	rig.push(second, third, first)
	rig.popAll(first, second, third)
}

func TestDeleteJustOne(t *testing.T) {
	rig := rig{t: t, name: "TestDeleteJustOne"}
	unit := &workUnit{name: "unit"}
	rig.push(unit)
	rig.q.Remove(unit)
	rig.popAll()
}

func TestDeleteFirstOfThree(t *testing.T) {
	rig := rig{t: t, name: "TestDeleteFirstOfThree"}
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	rig.push(first, second, third)
	rig.q.Remove(first)
	rig.popAll(second, third)
}

func TestDeleteOther(t *testing.T) {
	rig := rig{t: t, name: "TestDeleteOther"}
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	rig.push(first)
	rig.q.Remove(second)
	rig.popAll(first)
}

func TestReprioritizeFirst(t *testing.T) {
	rig := rig{t: t, name: "TestReprioritizeFirst"}
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	rig.push(first, second, third)
	first.priority = -1
	rig.q.Reprioritize(first)
	rig.popAll(second, third, first)
}

func TestReprioritizeMiddle(t *testing.T) {
	rig := rig{t: t, name: "TestReprioritizeMiddle"}
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	rig.push(first, second, third)
	second.priority = 100
	rig.q.Reprioritize(second)
	rig.popAll(second, first, third)
}
