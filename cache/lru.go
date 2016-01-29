// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package cache

// This file provides a simple LRU cache.  I know of at least two
// other implementations, though it is a pretty simple concept; I'm
// dissatisfied with the one I've looked at in several small ways.
// Some design notes have suggested adding a per-object unique
// identifier to Coordinate, and lookup-by-name vs. lookup-by-ID would
// be a distinct difference from the existing implementations.

import (
	"container/list"
	"sync"
)

// named describes things with names, like most Coordinate objects.
type named interface {
	Name() string
}

// lru is a least-recently-used cache with a fixed capacity.  The cache
// can be safely accessed from multiple goroutines.
type lru struct {
	size      int
	lock      sync.RWMutex
	evictList *list.List
	index     map[string]*list.Element
}

func newLRU(size int) *lru {
	return &lru{
		size:      size,
		evictList: list.New(),
		index:     make(map[string]*list.Element),
	}
}

// Get retrieves an item from the cache.  If it is not present, calls
// the fetch function, and if that returns non-null, saves the item
// and returns it.  This should return an error only if the item is
// not present and the fetch function returns an error.
func (lru *lru) Get(name string, fetch func(string) (named, error)) (named, error) {
	// This sadly happens under a writer lock, since we need to move
	// the item to the front of the list if it is present
	lru.lock.Lock()
	defer lru.lock.Unlock()

	// Is it there?
	if element, present := lru.index[name]; present {
		lru.evictList.MoveToBack(element)
		return element.Value.(named), nil
	}

	// Otherwise call the fetch function
	item, err := fetch(name)
	if err != nil {
		return item, err
	}
	lru.add(item)
	return item, nil
}

// Peek looks for an item in the cache and returns it if present, or
// returns nil if absent.  This runs under a reader lock, and so can
// run concurrently with itself but not calls to Put or Get.  This
// does not affect the recency of the item.
func (lru *lru) Peek(name string) named {
	lru.lock.RLock()
	defer lru.lock.RUnlock()

	if element, present := lru.index[name]; present {
		return element.Value.(named)
	}
	return nil
}

// Put adds an item to the LRU cache, possibly evicting something.
func (lru *lru) Put(item named) {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	// Are we just updating an existing item?
	if element, present := lru.index[item.Name()]; present {
		element.Value = item
		lru.evictList.MoveToBack(element)
		return
	}

	// Otherwise add it
	lru.add(item)
}

// Remove takes an item out of the cache.  It does nothing if that
// name does not exist.
func (lru *lru) Remove(name string) {
	lru.lock.Lock()
	defer lru.lock.Unlock()

	if element, present := lru.index[name]; present {
		delete(lru.index, name)
		lru.evictList.Remove(element)
	}
}

// add is an internal helper, running under the write lock, that adds a
// new item to the cache.  The item is known to not already exist.
func (lru *lru) add(item named) {
	element := lru.evictList.PushBack(item)
	lru.index[item.Name()] = element

	// If this caused the cache to go over size, start evicting items
	for len(lru.index) > lru.size {
		head := lru.evictList.Front()
		item := head.Value.(named)
		delete(lru.index, item.Name())
		lru.evictList.Remove(head)
	}
}
