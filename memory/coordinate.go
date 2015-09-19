// Package memory provides an in-process, in-memory implementation of
// Coordinate.  There is no persistence on this job queue, nor is
// there any automatic sharing.  The entire system is behind a single
// global semaphore to protect against concurrent updates; in some
// cases this can limit performance in the name of correctness.
//
// This is mostly intended as a simple reference implementation of
// Coordinate that can be used for testing, including in-process
// testing of higher-level components.  It is generally tuned for
// correctness, not performance or scalability.
package memory

import (
	"github.com/dmaze/goordinate/coordinate"
	"sync"
)

// This is the only external entry point to this package:

// New creates a new Coordinate interface that operates purely in
// memory.
func New() coordinate.Coordinate {
	c := new(memCoordinate)
	c.namespaces = make(map[string]*namespace)
	return c
}

// coordinable is a common interface for objects that need to take the
// global lock on the Coordinate state.
type coordinable interface {
	// Coordinate returns a pointer to the coordinate object
	// at the root of this object tree.
	Coordinate() *memCoordinate
}

// globalLock locks the coordinate object at the root of the object
// tree.  Pair this with globalUnlock, as
//
//     globalLock(self)
//     defer globalUnlock(self)
func globalLock(c coordinable) {
	c.Coordinate().sem.Lock()
}

// globalUnlock unlocks the coordinate object at the root of the
// object tree.
func globalUnlock(c coordinable) {
	c.Coordinate().sem.Unlock()
}

// Coordinate wrapper type:

type memCoordinate struct {
	namespaces map[string]*namespace
	sem        sync.Mutex
}

func (c *memCoordinate) Namespace(namespace string) (coordinate.Namespace, error) {
	globalLock(c)
	defer globalUnlock(c)

	ns := c.namespaces[namespace]
	if ns == nil {
		ns = newNamespace(c, namespace)
		c.namespaces[namespace] = ns
	}
	return ns, nil
}

func (c *memCoordinate) Coordinate() *memCoordinate {
	return c
}
