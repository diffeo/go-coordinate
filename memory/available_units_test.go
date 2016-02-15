// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
)

// push adds any number of work units to the priority queue, in the order
// of their parameters.
func push(q *availableUnits, units ...*workUnit) {
	for _, unit := range units {
		q.Add(unit)
	}
}

// popSpecific pulls a single unit out of the priority queue and asserts
// that it is exactly u.
func popSpecific(t *testing.T, q *availableUnits, u *workUnit) {
	if assert.NotZero(t, q.Len()) {
		out := q.Next()
		assert.Equal(t, u, out)
	}
}

// checkEmpty asserts that the priority queue is empty.
func checkEmpty(t *testing.T, q *availableUnits) {
	assert.Zero(t, q.Len())
}

// popAll pops units off the priority queue one at a time, comparing
// them to each of the parameters in turn, and asserts that the queue
// is empty at the end.
func popAll(t *testing.T, q *availableUnits, units ...*workUnit) {
	for _, unit := range units {
		popSpecific(t, q, unit)
	}
	checkEmpty(t, q)
}

func TestQueueOfOne(t *testing.T) {
	q := new(availableUnits)
	unit := &workUnit{name: "unit"}
	q.Add(unit)
	popAll(t, q, unit)
}

func TestQueueOfTwoInOrder(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	push(q, first, second)
	popAll(t, q, first, second)
}

func TestQueueOfTwoInWrongOrder(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	push(q, second, first)
	popAll(t, q, first, second)
}

func TestQueueOfThreeWithPriorities(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "z", meta: coordinate.WorkUnitMeta{Priority: 100}}
	second := &workUnit{name: "a"}
	third := &workUnit{name: "m"}
	push(q, second, third, first)
	popAll(t, q, first, second, third)
}

func TestDeleteJustOne(t *testing.T) {
	q := new(availableUnits)
	unit := &workUnit{name: "unit"}
	q.Add(unit)
	q.Remove(unit)
	popAll(t, q)
}

func TestDeleteFirstOfThree(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	push(q, first, second, third)
	q.Remove(first)
	popAll(t, q, second, third)
}

func TestDeleteOther(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	q.Add(first)
	q.Remove(second)
	popAll(t, q, first)
}

func TestReprioritizeFirst(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	push(q, first, second, third)
	first.meta.Priority = -1
	q.Reprioritize(first)
	popAll(t, q, second, third, first)
}

func TestReprioritizeMiddle(t *testing.T) {
	q := new(availableUnits)
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	push(q, first, second, third)
	second.meta.Priority = 100
	q.Reprioritize(second)
	popAll(t, q, second, first, third)
}
