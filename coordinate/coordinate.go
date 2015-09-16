// Package coordinate defines an abstract API to Coordinate.
//
// In most cases, applications will know of specific implementations of
// this API and will get an implementation of Coordinate or Namespace
// from that implementation.
//
// In general, objects here have a small amount of immutable data
// (a WorkUnit.Name() never changes, for instance) and the accessors
// of these return the value directly.  Accessors to mutable data return
// the value and an error.
package coordinate

import "time"

// Coordinate is the principal interface to the Coordinate system.
// Implementations of this interface provide a specific database backend,
// RPC system, or other way to interact with Coordinate.
type Coordinate interface {
	// Namespace retrieves a Namespace object for some name.  If
	// no namespace already exists with that name, creates one.
	// Coordinate implementations such as the Python one that do
	// not use namespaces pass an empty string here.
	Namespace(namespace string) (Namespace, error)
}

// Namespace is a single application's state within Coordinate.  A
// namespace has an immutable name, and a collection of work specs.  A
// namespace is tied to a single Coordinate backend.  Most
// applications will only need to interact with a single namespace.
type Namespace interface {
	// Name returns the name of this namespace.  This may be an
	// empty string.
	Name() string

	// Destroy destroys this namespace and all state associated
	// with it.  There is no recovery from this.  There is no
	// confirmation in the API.  This generally should not be
	// called outside of test code.
	//
	// If other goroutines or processes are using this namespace
	// or an equivalent object, operations on it will likely fail
	// (or, depending on database constraints, this operation may
	// itself fail).  You probably don't want to be in this state.
	Destroy() error

	// SetWorkSpec creates or updates a work spec.  The map may
	// have any string keys and any values, except that it must
	// contain a key "name" with a string value.  You cannot
	// rename an existing work spec, but changing any other keys
	// will change an existing work spec.  On success returns the
	// created (or modified) WorkSpec object.
	SetWorkSpec(workSpec map[string]interface{}) (WorkSpec, error)

	// WorkSpec retrieves a work spec by its name.  If no work
	// spec exists with that name, GetWorkSpec will return a nil
	// WorkSpec and a nil error.
	WorkSpec(name string) (WorkSpec, error)

	// WorkSpecNames returns the names of all of the work specs in
	// this namespace.  This may be an empty slice if there are no
	// work specs.  Unless one of the work specs is destroyed,
	// calling GetWorkSpec on one of these names will retrieve the
	// corresponding WorkSpec object.
	WorkSpecNames() ([]string, error)

	// Worker retrieves or creates a Worker object by its name.
	// Every Worker in this Namespace has a nominally unique but
	// client-provided name.  If no Worker exists yet with the
	// requested name, returns a new one with no parent.
	Worker(name string) (Worker, error)
}

// A WorkSpec defines a collection of related jobs.  For instance, a
// work spec could define a specific function to call, and its work units
// give parameters to that function.  A work spec has a string-keyed
// metadata map, where some keys (e.g., "name") have well-known types
// (string) and meanings.  A work spec also has any number of WorkUnit
// associated with it.
type WorkSpec interface {
	// Name returns the name of this work spec.
	Name() string

	// Data returns the definition of this work spec.
	Data() (map[string]interface{}, error)

	// AddWorkUnit adds a single work unit to this work spec.  If
	// a work unit already exists with the specified name, it is
	// overridden.
	AddWorkUnit(name string, data map[string]interface{}, priority int) (WorkUnit, error)

	// WorkUnit retrieves a single work unit by name.  If it does
	// not exist, return nil (not an error).
	WorkUnit(name string) (WorkUnit, error)

	// WorkUnits retrieves any number of work units by name.  Work
	// unit keys that do not exist are silently omitted from the
	// result.
	WorkUnits(names []string) (map[string]WorkUnit, error)

	// TODO: define what "status" means

	// AllWorkUnits retrieves some number of the work units
	// associated with this work spec.  If no work units are added
	// or destroyed, then consecutive calls incrementing the
	// "start" parameter will eventually retrieve all of the work
	// units.  The exact ordering of the work units is
	// unspecified.
	AllWorkUnits(start uint, limit uint) (map[string]WorkUnit, error)
}

// A WorkUnit is a single job to perform.  It is associated with a
// specific WorkSpec.  It could be a map entry, and has a name (key)
// and a data map.
type WorkUnit interface {
	// Name returns the name (key) of this work unit.
	Name() string

	// Data returns the data map of this work unit.
	Data() (map[string]interface{}, error)

	// WorkSpec returns the associated work spec.
	WorkSpec() WorkSpec

	// ActiveAttempt returns the current Attempt for this work
	// unit, if any.  If the work unit is completed, either
	// successfully or unsuccessfully, this is the Attempt that
	// finished it.  This may be an expired Attempt if no other
	// worker has started it yet.  If no Worker is currently
	// working on this work unit, returns nil.
	ActiveAttempt() (Attempt, error)

	// Attempts returns all current and past Attempts for this
	// work unit, if any.  This includes the attempt reported by
	// ActiveAttempt().
	Attempts() ([]Attempt, error)
}

// AttemptRequest describes parameters to Worker.RequestAttempts().
// Its zero value provides reasonable defaults, returning a single
// work unit from any work spec ignoring resource constraints if
// possible.
type AttemptRequest struct {
	// AvailableGb is the amount of memory that can be dedicated
	// to the returned work unit.  If zero, ignore this
	// constraint.  This is compared with the "min_gb" field in
	// the work spec.
	AvailableGb float64

	// Lifetime is the minimum requested time to perform this
	// attempt; it must be completed or renewed by this deadline.
	// If zero, use a system-provided default, generally 15
	// minutes.
	Lifetime time.Duration

	// NumberOfWorkUnits is the number of work units requested.
	// If zero, actually use one.  All of the returned attempts
	// will be for work units in the same work spec.  Fewer work
	// units, maybe as few as zero, can be returned if they are
	// not available.
	NumberOfWorkUnits int

	// TODO: limit to work specs
}

// A Worker is a process that is doing work.  Workers may be
// hierarchical, for instance with a parent Worker that does not do
// work itself but supervises its children.  A Worker chooses its own
// name, often a UUID.  It may be performing some number of Attempts;
// typically none if it is only a parent, exactly one if it runs work
// units serially, or multiple if it requests multiple work units in one
// shot or can actively run work units in parallel.
type Worker interface {
	// Name returns the worker-chosen name of the worker.
	Name() string

	// Parent returns the parent of this worker, if any.  If this
	// worker does not have a parent, nil is returned; this is not
	// an error.
	Parent() (Worker, error)

	// Children returns the children of this worker, if any.
	Children() ([]Worker, error)

	// RequestAttempts tries to allocate new work to this worker.
	// With a zero-valued AttemptRequest, this will return at most
	// one new Attempt with a default expiration from any work
	// spec with no resource constraints.  This may return fewer
	// attempts than were requested, maybe even none, if work is
	// not available.
	//
	// Any Attempts returned from this method will also be
	// returned from AllAttempts(), and will be returned from
	// ActiveAttempts() until they are completed or expired.
	RequestAttempts(req AttemptRequest) ([]Attempt, error)

	// ActiveAttempts returns all Attempts this worker is
	// currently performing, or an empty slice if this worker is
	// idle.
	ActiveAttempts() ([]Attempt, error)

	// AllAttempts returns all Attempts this worker has ever
	// performed, including those returned in ActiveAttempts().
	AllAttempts() ([]Attempt, error)

	// ChildAttempts returns any attempts this worker's
	// children are performing.  It is similar to calling
	// ActiveAttempt on each of Children, but is atomic.
	ChildAttempts() ([]Attempt, error)
}

// AttemptStatus is a brief representation of the current status of
// an Attempt.
type AttemptStatus int

const (
	// Pending attempts are not in any other state, and their
	// workers are still working on it.
	Pending AttemptStatus = iota

	// Expired attempts' expiration times have passed.  These
	// attempts should not be the active attempts for their work
	// units, but this constraint is not enforced anywhere.
	Expired

	// Finished attempts have been successfully completed by their
	// workers.
	Finished

	// Failed attempts have been unsuccessfully completed by their
	// workers.
	Failed

	// Retryable attempts have been unsuccessfully completed by
	// their workers, but the failures are identified as transient
	// such that later attempts at redoing the same work would
	// succeed.
	Retryable
)

// An Attempt is a persistent record that some worker is attempting to
// complete some specific work unit.  It has its own copy of the work
// unit data.
type Attempt interface {
	// WorkUnit returns the work unit that is being attempted.
	WorkUnit() WorkUnit

	// Worker returns the worker that is attempting the work.
	Worker() Worker

	// Status returns a high-level status of this Attempt.
	Status() (AttemptStatus, error)

	// Data returns the data map of this work unit, as seen
	// within this attempt.
	Data() (map[string]interface{}, error)

	// StartTime returns the time this attempt began.
	StartTime() (time.Time, error)

	// EndTime returns the time this attempt completed.  If
	// this attempt is not yet complete, returns a zero time.
	EndTime() (time.Time, error)

	// ExpirationTime returns the time by which the worker must
	// complete the work unit.  If this deadline passes, this
	// attempt may expire, and another worker can begin the work
	// unit.
	ExpirationTime() (time.Time, error)

	// Renew attempts to extend the time this worker has to
	// complete the attempt.  You must request a specific
	// duration, with time.Duration(15) * time.Minute being a
	// reasonable default.  Selecting 0 or a negative duration
	// will generally cause this Attempt's status to change to
	// Expired, though it is implementation-dependent whether that
	// change happens before or after this call actuall returns.
	// If data is non-nil, replaces the data stored in this
	// Attempt with a new map.
	//
	// This Attempt must be the active attempt for Renew() to have
	// any affect.  If it is not, the Attempt data will still be
	// updated, but Renew() will return ErrLostLease.
	//
	// The Status() of this Attempt must be Pending for Renew()
	// to have any affect.  If it is Expired but still is the
	// active Attempt, it can also be Renew()ed.  Otherwise, do
	// not update anything and return ErrNotPending.
	Renew(extendDuration time.Duration, data map[string]interface{}) error

	// Expire explicitly transitions an Attempt from Pending to
	// Expired status.  If data is non-nil, also updates the work
	// unit data.  If Status() is already Expired, has no effect.
	//
	// This method is intended to be called by a parent worker to
	// record the fact that it killed off a long-running work unit
	// that was about to expire.  As such it is possible that the
	// parent and child can both be trying to update the same
	// Attempt, resulting in conflicts in the data map.
	//
	// If the Status() of this Attempt is not Pending or Expired,
	// does nothing and returns ErrNotPending.
	Expire(data map[string]interface{}) error

	// Finish transitions an Attempt from Pending to Finished
	// status.  If data is non-nil, also updates the work unit
	// data.
	//
	// If the Status() of this attempt is not Pending, or if it
	// is not both Expired and the current active Attempt, returns
	// ErrNotPending and has no effect.
	Finish(data map[string]interface{}) error

	// Fail transitions an Attempt from Pending to Failed status.
	// If data is non-nil, also updates the work unit data.
	//
	// If the Status() of this attempt is not Pending, or if it
	// is not both Expired and the current active Attempt, returns
	// ErrNotPending and has no effect.
	Fail(data map[string]interface{}) error

	// Retry transitions an Attempt from Pending to Retryable
	// status.  If data is non-nil, also updates the work unit
	// data.
	//
	// TODO: This method is likely to gain a time.Duration
	// parameter to set the earliest time for a retry.
	//
	// If the Status() of this attempt is not Pending, or if it
	// is not both Expired and the current active Attempt, returns
	// ErrNotPending and has no effect.
	Retry(data map[string]interface{}) error
}
