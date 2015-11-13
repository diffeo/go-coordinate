// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package backend provides a standard way to construct a coordinate
// interface based on command-line flags.
package backend

import (
	"errors"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/diffeo/go-coordinate/postgres"
	"strings"
)

// Backend describes user-visible parameters to store coordinate data.
// This implements the flag.Value interface, and so a typical use is
//
//     func main() {
//         backend := backend.Backend{"memory", ""}
//         flag.Var(&backend, "backend", "impl:address of coordinate storage")
//         flag.Parse()
//         coordinate := backend.Coordinate()
//     }
type Backend struct {
	// Implementation holds the name of the implementation; for
	// instance, "memory".
	Implementation string

	// Address holds some backend-specific address, such as a
	// database connect string.
	Address string
}

// Coordinate creates a new coordinate interface.  This generally should be
// only called once.  If the backend has in-process state, such as a
// database connection pool or an in-memory story, calling this multiple
// times will create multiple copies of that state.  In particular, if
// b.Implementation is "memory", multiple calls to this will create
// multiple independent coordinate "worlds".
//
// If b.Implementation does not match a known implementation, returns
// an error.  It is assumed that Set() will validate at least the
// implementation.  The choice of implementation can also produce
// errors (invalid connection string, etc.)
func (b *Backend) Coordinate() (coordinate.Coordinate, error) {
	switch b.Implementation {
	case "memory":
		return memory.New(), nil
	case "postgres":
		return postgres.New(b.Address)
	default:
		return nil, errors.New("unknown coordinate backend " + b.Implementation)
	}
}

// String renders a backend description as a string.
func (b *Backend) String() string {
	if b.Address == "" {
		return b.Implementation
	}
	return b.Implementation + ":" + b.Address
}

// Set parses a string into an existing backend description.  The
// string should be of the form "implementation:address", where
// address can be any string.  Set checks to see if the provided
// implementation is any of the known implementations, and returns an
// appropriate error if not.
//
// This is part of the flag.Value interface.  If Set returns a nil
// error then Coordinate() will return successfully.  Note that
// neither function attempts to validate the b.Address part of the
// string or attempts to actually make a connection.
func (b *Backend) Set(param string) (err error) {
	parts := strings.SplitN(param, ":", 2)
	switch len(parts) {
	case 0:
		err = errors.New("must specify a backend type")
	case 1:
		b.Implementation = parts[0]
		b.Address = ""
	case 2:
		b.Implementation = parts[0]
		b.Address = parts[1]
	default:
		err = errors.New("strings.SplitN did something odd")
	}
	return
}
