package jobserver_test

import (
	"github.com/dmaze/goordinate/cborrpc"
	"gopkg.in/check.v1"
)

func (s *PythonSuite) TestBasic(c *check.C) {
	foo := cborrpc.PythonTuple{Items: []interface{}{"foo"}}
	barbaz := cborrpc.PythonTuple{Items: []interface{}{"bar", "baz"}}
	bar := cborrpc.PythonTuple{Items: []interface{}{"bar"}}

	ok, msg, err := s.JobServer.Lock("id", 0, []interface{}{foo, barbaz})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	lockid, err := s.JobServer.Readlock([]interface{}{foo})
	c.Assert(err, check.IsNil)
	c.Check(lockid, check.DeepEquals, []interface{}{"id"})

	lockid, err = s.JobServer.Readlock([]interface{}{bar})
	c.Assert(err, check.IsNil)
	c.Check(lockid, check.DeepEquals, []interface{}{nil})

	lockid, err = s.JobServer.Readlock([]interface{}{barbaz, foo})
	c.Assert(err, check.IsNil)
	c.Check(lockid, check.DeepEquals, []interface{}{"id", "id"})

	ok, msg, err = s.JobServer.Unlock("id", []interface{}{foo, barbaz})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	lockid, err = s.JobServer.Readlock([]interface{}{foo, bar, barbaz})
	c.Assert(err, check.IsNil)
	c.Check(lockid, check.DeepEquals, []interface{}{nil, nil, nil})
}

func (s *PythonSuite) TestConflict(c *check.C) {
	ok, msg, err := s.JobServer.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// should not be able to lock foo.bar when foo is held
	ok, msg, err = s.JobServer.Lock("id", 0,
		[]interface{}{[]interface{}{"foo", "bar"}})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(msg, check.Not(check.Equals), "")
}

func (s *PythonSuite) TestConflict2(c *check.C) {
	ok, msg, err := s.JobServer.Lock("id", 0,
		[]interface{}{[]interface{}{"foo", "bar"}})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// should not be able to lock foo when foo.bar is held
	ok, msg, err = s.JobServer.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(msg, check.Not(check.Equals), "")
}

func (s *PythonSuite) TestLocksome(c *check.C) {
	ok, msg, err := s.JobServer.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	locked, msg, err := s.JobServer.Locksome("id", 0, []interface{}{
		[]interface{}{"foo"},
		[]interface{}{"bar"},
		[]interface{}{"baz"},
	})
	c.Assert(err, check.IsNil)
	c.Check(msg, check.Equals, "")
	c.Check(locked, check.DeepEquals, [][]interface{}{
		nil,
		[]interface{}{"bar"},
		[]interface{}{"baz"},
	})
}

func (s *PythonSuite) TestUnlockSanity(c *check.C) {
	keys := []interface{}{[]interface{}{"foo"}, []interface{}{"bar"}}

	ok, msg, err := s.JobServer.Lock("id", 0, keys)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")

	// Should not be able to unlock something a different locker locked
	ok, msg, err = s.JobServer.Unlock("id2", keys)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(msg, check.Not(check.Equals), "")

	// Should be able to read original lock
	lockid, err := s.JobServer.Readlock(keys)
	c.Assert(err, check.IsNil)
	c.Check(lockid, check.DeepEquals, []interface{}{"id", "id"})

	ok, msg, err = s.JobServer.Unlock("id", keys)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(msg, check.Equals, "")
}
