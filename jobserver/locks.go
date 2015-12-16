// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver

import (
	"fmt"
	"github.com/diffeo/go-coordinate/cborrpc"
	"reflect"
	"time"
)

// lockNode is a single node in a hierarchical tree of locks.
type lockNode struct {
	// Label is an arbitrary label for this node.  It may be a string,
	// a cborrpc.PythonTuple, or another object that can be compared
	// via reflect.DeepEquals.
	Label interface{}

	// Owner is the caller-provided identifier of the owner of a lock
	// on the node, or empty string if unlocked.
	Owner string

	// Deadline is the expiration time of the lock, or zero if
	// unlocked.
	Deadline time.Time

	// Children is an unordered list of the children of this node.
	Children []*lockNode
}

// Child returns a child of l with a specified label, or nil if no
// such child exists.
func (l *lockNode) Child(label interface{}) *lockNode {
	for _, node := range l.Children {
		if reflect.DeepEqual(label, node.Label) {
			return node
		}
	}
	return nil
}

// RemoveChild removes a specific child from l.  It is a no-op if
// child is not in l's children list.
func (l *lockNode) RemoveChild(child *lockNode) {
	var children []*lockNode
	for _, c := range l.Children {
		if c != child {
			children = append(children, c)
		}
	}
	l.Children = children
}

// PruneChildren removes all children of l that are unlocked and
// have no children themselves.  It is not recursive beyond this.
func (l *lockNode) PruneChildren() {
	var children []*lockNode
	for _, child := range l.Children {
		if !(len(child.Children) == 0 && child.Deadline.IsZero()) {
			children = append(children, child)
		}
	}
	l.Children = children
}

// CanLock determines whether an arbitrary path can be locked.
func (l *lockNode) CanLock(key []interface{}) bool {
	// If this node is locked, then either it is key or a prefix
	// thereof, and we cannot lock it
	if !l.Deadline.IsZero() {
		return false
	}
	// If this node does not have children and is not itself locked,
	// then the lock can succeed.  (Either it is key itself and so
	// we've proven it is unlocked, or the full path to key doesn't
	// exist and no prefix of it is locked.)
	if len(l.Children) == 0 {
		return true
	}
	// If key is empty, then we are pointing at an unlocked parent
	// (or ancestor) of some locked node, and cannot lock.
	if len(key) == 0 {
		return false
	}
	// Otherwise find the child
	head := key[0]
	tail := key[1:]
	child := l.Child(head)
	// If there is no child with that name then we will succeed
	if child == nil {
		return true
	}
	// Otherwise search recursively
	return child.CanLock(tail)
}

// Path finds a lock node at an arbitrary path.  If there is no such
// path, returns nil.  This can help determine if a specific path is
// locked, but even if it isn't, a path may be unlockable because a
// prefix of the path is locked.
//
// If create is true, then this never returns nil; instead, it will
// create new child nodes as required.
func (l *lockNode) Path(key []interface{}, create bool) *lockNode {
	if len(key) == 0 {
		return l
	}
	child := l.Child(key[0])
	if child == nil {
		if create {
			child = &lockNode{Label: key[0]}
			l.Children = append(l.Children, child)
		} else {
			return nil
		}
	}
	return child.Path(key[1:], create)
}

// Lock claims a lock on an arbitrary path.  It assumes that CanLock
// has already been checked.
func (l *lockNode) Lock(owner string, deadline time.Time, key []interface{}) {
	leaf := l.Path(key, true)
	leaf.Owner = owner
	leaf.Deadline = deadline
}

// Unlock releases a lock on an arbitrary path.  It may delete its
// child node if it is unlocked and has no children.
func (l *lockNode) Unlock(key []interface{}) {
	if len(key) == 0 {
		l.Owner = ""
		l.Deadline = time.Time{}
	} else {
		child := l.Child(key[0])
		// Do nothing if there is no matching child
		if child != nil {
			child.Unlock(key[1:])
			l.PruneChildren()
		}
	}
}

func (l *lockNode) Expire(now time.Time) {
	// First expire all of our children
	for _, child := range l.Children {
		child.Expire(now)
	}
	l.PruneChildren()
	// If we ourselves are expired, release our lock
	if now.After(l.Deadline) {
		l.Owner = ""
		l.Deadline = time.Time{}
	}
	// Our parent will prune is if required
}

// lockDeadline finds the expiration time from a timeout parameter passed
// in an RPC request.
func lockDeadline(now time.Time, timeout int) time.Time {
	if timeout == 0 {
		timeout = 60
	}
	if timeout > 1000000 {
		timeout = 1000000
	}
	return now.Add(time.Duration(timeout) * time.Second)
}

// lockKeys unmarshals an arbitrary object into a list of lists of
// objects, flattening tuples into lists, or fails.
func lockKeys(obj interface{}) ([][]interface{}, bool) {
	list, ok := cborrpc.Detuplify(obj)
	if !ok {
		return nil, ok
	}
	list2 := make([][]interface{}, len(list))
	for i, item := range list {
		item2, ok := cborrpc.Detuplify(item)
		if !ok {
			return nil, ok
		}
		list2[i] = item2
	}
	return list2, ok
}

func (jobs *JobServer) doLock(keys interface{}, f func(time.Time, [][]interface{}) error) error {
	newKeys, ok := lockKeys(keys)
	if !ok {
		return fmt.Errorf("cannot decode key paths")
	}
	jobs.lockLock.Lock()
	defer jobs.lockLock.Unlock()
	now := time.Now()
	jobs.locks.Expire(now)
	return f(now, newKeys)
}

// Lock claims a lock on a set of hierarchical keys.  Locks are global
// to this specific instance of the job server, and will not survive a
// restart.  The key space is hierarchical, so the keys parameter is a
// list of key paths, e.g. [][]interface{}{{"usr", "bin"}, {"usr", "share"}}.
// If the timeout is 0, a (60-second) default is used instead.
//
// This tries to lock all of the keys.  Returns true if all could be
// locked, or false if nothing is locked at all.
func (jobs *JobServer) Lock(lockerID string, timeout int, keys interface{}) (ok bool, msg string, err error) {
	err = jobs.doLock(keys, func(now time.Time, keys [][]interface{}) error {
		deadline := lockDeadline(now, timeout)

		for _, key := range keys {
			if !jobs.locks.CanLock(key) {
				msg = fmt.Sprintf("%v is not lockable", key)
				return nil
			}
		}

		for _, key := range keys {
			jobs.locks.Lock(lockerID, deadline, key)
		}
		ok = true
		return nil
	})
	return
}

// Locksome claims locks on as many of a set of hierarchical keys as
// possible.  The actual semantics of locking are the same as Lock().
// The return value is an ordered list of key paths, in the same order
// as the keys parameter, where each item is either the matching key
// path if locked or nil if unsuccessful.
func (jobs *JobServer) Locksome(lockerID string, timeout int, keys interface{}) (result [][]interface{}, msg string, err error) {
	err = jobs.doLock(keys, func(now time.Time, keys [][]interface{}) error {
		deadline := lockDeadline(now, timeout)
		for _, key := range keys {
			if jobs.locks.CanLock(key) {
				jobs.locks.Lock(lockerID, deadline, key)
				result = append(result, key)
			} else {
				result = append(result, nil)
			}
		}
		return nil
	})
	return
}

// Renew attempts to renew all of a list of key paths.  If any are not
// locked by the given locker ID, fails; otherwise extends their locks by
// the new timeout.
func (jobs *JobServer) Renew(lockerID string, timeout int, keys interface{}) (ok bool, msg string, err error) {
	err = jobs.doLock(keys, func(now time.Time, keys [][]interface{}) error {
		deadline := lockDeadline(now, timeout)
		for _, key := range keys {
			node := jobs.locks.Path(key, false)
			if node == nil {
				msg = fmt.Sprintf("%v is held by None", key)
				return nil
			}
			if node.Owner != lockerID {
				msg = fmt.Sprintf("%v is held by %v", key, node.Owner)
				return nil
			}
		}

		for _, key := range keys {
			node := jobs.locks.Path(key, false)
			node.Deadline = deadline
		}
		ok = true
		return nil
	})
	return
}

// Readlock determines the current owner, if any, of a list of key paths.
// The return value is a list of either string owner names or nil.
func (jobs *JobServer) Readlock(keys interface{}) (result []interface{}, err error) {
	result = make([]interface{}, 0)
	err = jobs.doLock(keys, func(_ time.Time, keys [][]interface{}) error {
		for _, key := range keys {
			node := jobs.locks.Path(key, false)
			if node == nil || node.Deadline.IsZero() {
				result = append(result, nil)
			} else {
				result = append(result, node.Owner)
			}
		}
		return nil
	})
	return
}

// Unlock releases locks on all of a set of key paths.  The nodes must
// be currently locked by lockerID.
func (jobs *JobServer) Unlock(lockerID string, keys interface{}) (ok bool, msg string, err error) {
	err = jobs.doLock(keys, func(_ time.Time, keys [][]interface{}) error {
		for _, key := range keys {
			node := jobs.locks.Path(key, false)
			if node == nil {
				msg = fmt.Sprintf("%v is not held", key)
				return nil
			} else if node.Deadline.IsZero() || node.Owner != lockerID {
				msg = fmt.Sprintf("%v is held by %v", key, node.Owner)
				return nil
			}
		}
		for _, key := range keys {
			jobs.locks.Unlock(key)
		}
		ok = true
		return nil
	})
	return
}

// DeleteNamespace deletes all locks in the lock system whose first
// key part is prefix.
func (jobs *JobServer) DeleteNamespace(prefix interface{}) (int, error) {
	jobs.lockLock.Lock()
	defer jobs.lockLock.Unlock()

	child := jobs.locks.Child(prefix)
	if child != nil {
		jobs.locks.RemoveChild(child)
	}
	return 0, nil
}
