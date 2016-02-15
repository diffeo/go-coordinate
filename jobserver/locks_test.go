// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver_test

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasic(t *testing.T) {
	j := setUpTest(t, "TestBasic")
	defer tearDownTest(t, j)

	foo := cborrpc.PythonTuple{Items: []interface{}{"foo"}}
	barbaz := cborrpc.PythonTuple{Items: []interface{}{"bar", "baz"}}
	bar := cborrpc.PythonTuple{Items: []interface{}{"bar"}}

	ok, msg, err := j.Lock("id", 0, []interface{}{foo, barbaz})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	lockid, err := j.Readlock([]interface{}{foo})
	if assert.NoError(t, err) {
		assert.Equal(t, []interface{}{"id"}, lockid)
	}

	lockid, err = j.Readlock([]interface{}{bar})
	if assert.NoError(t, err) {
		assert.Equal(t, []interface{}{nil}, lockid)
	}

	lockid, err = j.Readlock([]interface{}{barbaz, foo})
	if assert.NoError(t, err) {
		assert.Equal(t, []interface{}{"id", "id"}, lockid)
	}

	ok, msg, err = j.Unlock("id", []interface{}{foo, barbaz})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	lockid, err = j.Readlock([]interface{}{foo, bar, barbaz})
	if assert.NoError(t, err) {
		assert.Equal(t, []interface{}{nil, nil, nil}, lockid)
	}
}

func TestConflict(t *testing.T) {
	j := setUpTest(t, "TestConflict")
	defer tearDownTest(t, j)

	ok, msg, err := j.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// should not be able to lock foo.bar when foo is held
	ok, msg, err = j.Lock("id", 0,
		[]interface{}{[]interface{}{"foo", "bar"}})
	if assert.NoError(t, err) {
		assert.False(t, ok)
		assert.NotEmpty(t, msg)
	}
}

func TestConflict2(t *testing.T) {
	j := setUpTest(t, "TestConflict2")
	defer tearDownTest(t, j)

	ok, msg, err := j.Lock("id", 0,
		[]interface{}{[]interface{}{"foo", "bar"}})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// should not be able to lock foo when foo.bar is held
	ok, msg, err = j.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	if assert.NoError(t, err) {
		assert.False(t, ok)
		assert.NotEmpty(t, msg)
	}
}

func TestLocksome(t *testing.T) {
	j := setUpTest(t, "TestLocksome")
	defer tearDownTest(t, j)

	ok, msg, err := j.Lock("id", 0,
		[]interface{}{[]interface{}{"foo"}})
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	locked, msg, err := j.Locksome("id", 0, []interface{}{
		[]interface{}{"foo"},
		[]interface{}{"bar"},
		[]interface{}{"baz"},
	})
	if assert.NoError(t, err) {
		assert.Empty(t, msg)
		assert.Equal(t, [][]interface{}{
			nil,
			[]interface{}{"bar"},
			[]interface{}{"baz"},
		}, locked)
	}
}

func TestUnlockSanity(t *testing.T) {
	j := setUpTest(t, "TestUnlockSanity")
	defer tearDownTest(t, j)

	keys := []interface{}{[]interface{}{"foo"}, []interface{}{"bar"}}

	ok, msg, err := j.Lock("id", 0, keys)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}

	// Should not be able to unlock something a different locker locked
	ok, msg, err = j.Unlock("id2", keys)
	if assert.NoError(t, err) {
		assert.False(t, ok)
		assert.NotEmpty(t, msg)
	}

	// Should be able to read original lock
	lockid, err := j.Readlock(keys)
	if assert.NoError(t, err) {
		assert.Equal(t, []interface{}{"id", "id"}, lockid)
	}

	ok, msg, err = j.Unlock("id", keys)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Empty(t, msg)
	}
}

func TestReadlockNotNil(t *testing.T) {
	j := setUpTest(t, "TestReadlockNotNil")
	defer tearDownTest(t, j)

	locks, err := j.Readlock([]interface{}{})
	if assert.NoError(t, err) {
		assert.NotNil(t, locks)
		assert.Len(t, locks, 0)
	}
}

func TestUnlockNotLocked(t *testing.T) {
	j := setUpTest(t, "TestUnlockNotLocked")
	defer tearDownTest(t, j)

	keys := []interface{}{[]interface{}{"foo"}, []interface{}{"bar"}}
	ok, _, err := j.Unlock("id", keys)
	if assert.NoError(t, err) {
		assert.False(t, ok)
	}
}
