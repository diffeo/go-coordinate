// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type AName struct {
	IAm string
}

func (a AName) Name() string {
	return a.IAm
}

func Make(name string) (named, error) {
	return AName{IAm: name}, nil
}

func DoNotMake(name string) (named, error) {
	return nil, assert.AnError
}

type LRUAssertions struct {
	*assert.Assertions
	LRU *lru
}

func NewLRUAssertions(t assert.TestingT, size int) *LRUAssertions {
	return &LRUAssertions{
		assert.New(t),
		newLRU(size),
	}
}

// PutName adds an item with name to the cache.
func (a *LRUAssertions) PutName(name string) {
	item := AName{IAm: name}
	a.LRU.Put(item)
}

// GetName fetches an item with name from the cache; if not present, it
// is added.
func (a *LRUAssertions) GetName(name string) {
	item, err := a.LRU.Get(name, Make)
	if a.NoError(err) && a.IsType(AName{}, item) {
		aName := item.(AName)
		a.Equal(aName.Name(), name)
	}
}

// GetPresent fetches an item with name from the cache; if not present,
// it should produce an assertion error.
func (a *LRUAssertions) GetPresent(name string) {
	item, err := a.LRU.Get(name, DoNotMake)
	if a.NoError(err) && a.IsType(AName{}, item) {
		aName := item.(AName)
		a.Equal(aName.Name(), name)
	}
}

// GetError tries to fetch an item from the cache, but it should not
// exist, and the resulting error will be caught.
func (a *LRUAssertions) GetError(name string) {
	_, err := a.LRU.Get(name, DoNotMake)
	a.Error(err)
}

// LRUHas asserts that an item with name is in the cache.
func (a *LRUAssertions) LRUHas(name string) {
	item := a.LRU.Peek(name)
	if a.NotNil(item) {
		a.Equal(name, item.Name())
	}
}

// LRUDoesNotHave asserts that no item with name is in the cache.
func (a *LRUAssertions) LRUDoesNotHave(name string) {
	item := a.LRU.Peek(name)
	a.Nil(item)
}

// TestLRUSimple tests minimal object presence.
func TestLRUSimple(t *testing.T) {
	a := NewLRUAssertions(t, 2)
	a.PutName("Sam")

	a.LRUHas("Sam")
	a.LRUDoesNotHave("Horton")
}

// TestLRUAutoInsert tests lru.Get() adding absent items.
func TestLRUAutoInsert(t *testing.T) {
	a := NewLRUAssertions(t, 2)

	// Get (and insert) two names
	a.GetName("Marvin")
	a.GetName("Horton")

	// At this point "Marvin" and "Horton" should both be present
	a.LRUHas("Marvin")
	a.LRUHas("Horton")

	// Now add one more name; since it is a third one, the oldest
	// (Marvin) should be evicted
	a.GetName("Sam")
	a.LRUDoesNotHave("Marvin")
	a.LRUHas("Horton")
	a.LRUHas("Sam")
}

func TestLRUInsertError(t *testing.T) {
	a := NewLRUAssertions(t, 2)

	// As before
	a.GetName("Marvin")
	a.GetName("Horton")
	a.LRUHas("Marvin")
	a.LRUHas("Horton")

	// Now try to add "Sam", but the add function will return an error
	a.GetError("Sam")
	// Since no item was added, nothing will be evicted
	a.LRUHas("Marvin")
	a.LRUHas("Horton")
	a.LRUDoesNotHave("Sam")

	// We can call the erroring version of Get() but since the item
	// is present it will not fail
	a.GetPresent("Marvin")
	a.GetPresent("Horton")
}

// TestLRUOrder tests that getting an item causes it to not get evicted.
func TestLRUOrder(t *testing.T) {
	a := NewLRUAssertions(t, 2)

	a.GetName("Marvin")
	a.GetName("Horton")
	a.LRUHas("Marvin")
	a.LRUHas("Horton")

	// Do an *additional* get for Marvin, so he is more-recently-used
	a.GetName("Marvin")

	// Now when we add Sam, Horton gets pushed out
	a.GetName("Sam")
	a.LRUHas("Marvin")
	a.LRUDoesNotHave("Horton")
	a.LRUHas("Sam")
}

// TestLRURemoval does simple tests on the Remove call.
func TestLRURemoval(t *testing.T) {
	a := NewLRUAssertions(t, 2)

	// Obvious thing #1:
	a.GetName("Marvin")
	a.LRUHas("Marvin")
	a.LRU.Remove("Marvin")
	a.LRUDoesNotHave("Marvin")

	// Obvious thing #2:
	a.LRU.Remove("Sam")
	a.LRUDoesNotHave("Sam")

	// Also if we remove a more-recent thing, the
	// older-but-present thing shouldn't get evicted
	a.GetName("Marvin")
	a.GetName("Horton")
	a.LRU.Remove("Horton")
	a.GetName("Sam")
	a.LRUHas("Marvin")
	a.LRUDoesNotHave("Horton")
	a.LRUHas("Sam")
}
