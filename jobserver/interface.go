// Package jobserver provides a CBOR-RPC interface compatible with
// the Python coordinate module.  This should be merged in some form
// with the goordinated package.
//
// The Python coordinated operates with an extremely irregular
// RPC-like interface.  Many methods, but not all, take a dictionary
// of additional options.  Many methods, but not all, return an
// in-band string error message, plus the underlying RPC layer allows
// an exception string to be returned.  Some methods specifically
// require a Python tuple return, even though the only way to achieve
// this across the wire is through an extension tag in CBOR.
//
// As such, JobServer provides an interface that can be made compatible
// with the Python coordinate library, but it is unlikely to be useful
// to native Go code or other client interfaces.
package jobserver

import "github.com/dmaze/goordinate/coordinate"
import "sync"

// JobServer is a network-accessible interface to Coordinate.  Its
// methods are the Python coordinated RPC methods, with more normalized
// parameters and Go-style CamelCase names.
type JobServer struct {
	// Namespace is the Coordinate Namespace interface this works
	// against.
	Namespace coordinate.Namespace

	// GlobalConfig is the configuration that is returned by the
	// GetConfig RPC call.
	GlobalConfig map[string]interface{}

	// locks is the root of the tree for the hierarchical lock
	// subsystem.
	locks lockNode

	// lockLock is a global mutex over the locks tree.
	lockLock sync.Mutex
}
