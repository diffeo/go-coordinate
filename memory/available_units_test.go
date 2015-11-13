// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package memory

import (
	"gopkg.in/check.v1"
)

type Suite struct {
	q *availableUnits
	c *check.C
}

func (s *Suite) SetUpTest(c *check.C) {
	s.q = new(availableUnits)
	s.c = c
}

// push adds any number of work units to the priority queue, in the order
// of their parameters.
func (s *Suite) push(units ...*workUnit) {
	for _, unit := range units {
		s.q.Add(unit)
	}
}

// popSpecific pulls a single unit out of the priority queue and asserts
// that it is exactly u.
func (s *Suite) popSpecific(u *workUnit) {
	s.c.Assert(s.q.Len(), check.Not(check.Equals), 0)
	out := s.q.Next()
	s.c.Check(out, check.DeepEquals, u)
}

// checkEmpty asserts that the priority queue is empty.
func (s *Suite) checkEmpty() {
	s.c.Assert(s.q.Len(), check.Equals, 0)
}

// popAll pops units off the priority queue one at a time, comparing
// them to each of the parameters in turn, and asserts that the queue
// is empty at the end.
func (s *Suite) popAll(units ...*workUnit) {
	for _, unit := range units {
		s.popSpecific(unit)
	}
	s.checkEmpty()
}

func (s *Suite) TestQueueOfOne(c *check.C) {
	unit := &workUnit{name: "unit"}
	s.push(unit)
	s.popAll(unit)
}

func (s *Suite) TestQueueOfTwoInOrder(c *check.C) {
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	s.push(first, second)
	s.popAll(first, second)
}

func (s *Suite) TestQueueOfTwoInWrongOrder(c *check.C) {
	first := &workUnit{name: "first"}
	second := &workUnit{name: "second"}
	s.push(second, first)
	s.popAll(first, second)
}

func (s *Suite) TestQueueOfThreeWithPriorities(c *check.C) {
	first := &workUnit{name: "z", priority: 100}
	second := &workUnit{name: "a"}
	third := &workUnit{name: "m"}
	s.push(second, third, first)
	s.popAll(first, second, third)
}

func (s *Suite) TestDeleteJustOne(c *check.C) {
	unit := &workUnit{name: "unit"}
	s.push(unit)
	s.q.Remove(unit)
	s.popAll()
}

func (s *Suite) TestDeleteFirstOfThree(c *check.C) {
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	s.push(first, second, third)
	s.q.Remove(first)
	s.popAll(second, third)
}

func (s *Suite) TestDeleteOther(c *check.C) {
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	s.push(first)
	s.q.Remove(second)
	s.popAll(first)
}

func (s *Suite) TestReprioritizeFirst(c *check.C) {
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	s.push(first, second, third)
	first.priority = -1
	s.q.Reprioritize(first)
	s.popAll(second, third, first)
}

func (s *Suite) TestReprioritizeMiddle(c *check.C) {
	first := &workUnit{name: "a"}
	second := &workUnit{name: "b"}
	third := &workUnit{name: "c"}
	s.push(first, second, third)
	second.priority = 100
	s.q.Reprioritize(second)
	s.popAll(second, first, third)
}

func init() {
	check.Suite(&Suite{})
}
