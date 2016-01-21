// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package restdata defines common data structures shared between the
// restserver and restclient packages.  Generally JSON encodings of
// these are passed across the wire as the
// application/vnd.diffeo.coordinate.v1+json MIME type.
//
// In spite of the "v1" label this representation is not considered
// fully stable yet.
//
// API Usage
//
// HTTP GET the root document at its specified URL.  This will return
// a JSON serialization of the RootData object.  That serialization
// has links to other resources; follow these links, possibly filling
// in template values, to get to other resources.
//
// Many of the URL fields are actually RFC 6570 URI templates.
// This is a fancy way of saying that they are URL strings with a
// {parameter} in curly braces (or, in some cases, {?p1*,p2} to
// describe query strings).  For instance, if the system is rooted at
// /, a JSON serialization of RootData will look like
//
//     {
//         "namespaces_url": "/namespaces",
//         "namespace_url": "/namespace/{namespace}"
//     }
//
// While the URL structure is predictable and formulaic, it is not
// actually part of the API contract.  The only specific guarantee is
// that retrieving the Coordinate root resource will return a
// serialization of RootData.
//
// Encoding Considerations
//
// A name that appears in a URL string must be made of ASCII
// characters that can be represented unescaped.  Other names are
// escaped by encoding their byte representations using the base64
// URL-safe encoding with no padding, and prepending a hyphen to the
// name.  Names that would be otherwise safe and begin with hyphens
// are also encoded.
//
// The URL path
//
//     /namespace/-/work_spec/foo/work_unit/-LQ
//
// refers to the empty namespace, the work spec named "foo" within
// that, and its work unit named "-".
//
// Most Coordinate objects have a corresponding "data" field.  These
// can be conveyed as either a JSON object or a string.  If a string,
// it is a base64 encoded CBOR encoding of the data object, using
// standard base64 alphabet and padding rules.  The CBOR encoding is
// required to preserve some data types that cannot be conveyed in
// JSON, most notably the PythonTuple type from the Coordinate cborrpc
// package and UUIDs.
//
// Timestamps, when they appear, are represented in JSON as RFC 3339
// strings, "2012-03-04T05:06:07.890Z".  Durations, when they appear,
// represented in JSON as a number of nanoseconds.
//
// HTTP Considerations
//
// Each URL reference notes the applicable HTTP verbs.  In most cases
// simple resource references support GET, PUT, and DELETE, and
// actions support POST and possibly GET.  Any resource that supports
// GET also supports HEAD.
//
// When a representation is PUT, any non-null field is updated.
// Fields that are null or absent in the uploaded data remain
// unchanged.  Usually a corresponding GET request will return a
// complete representation.
//
// Object names never change after their creation.  In most cases, URL
// template links included in a representation will not change either.
//
// The current server implementation matching this makes minimal use
// of HTTP status codes, but will usually correctly return 200 OK, 204
// No Content, 400 Bad Request, 404 Not Found, and 415 Unsupported
// Media Type when these are correct.
//
// Errors
//
// Most errors should be returned as encodings of the ErrorResponse
// type.  This can round-trip all of the coordinate package's errors
// but may return most other errors as plain strings that are not
// the same objects as other standard errors.
//
// If Go server code panics, this should be captured and returned as
// an ErrorResponse with error code "panic".
//
// Errors should be returned as failing HTTP statuses, but some
// application-level errors may be returned as 500 Internal Server
// Error even in correct operation.
//
// Other Notes
//
// The coordinate Attempt type does not provide any sort of unique
// identifier.  Implementations may assume that the triple of a work
// unit, a worker, and its start time (in whole seconds) is enough to
// identify an attempt.  Only test code is especially likely to run
// into trouble with this, and it should address it with a mock time
// source.
package restdata

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// V1JSONMediaType is the preferred, most specific MIME type for the
// JSON representation of this content.
const V1JSONMediaType = "application/vnd.diffeo.coordinate.v1+json"

// JSONMediaType requests the most recent version of the JSON
// representation of this content.
const JSONMediaType = "application/vnd.diffeo.coordinate+json"

// DataDict is an arbitrary user-provided data dictionary.  Many
// objects have these, generally in a field named Data.  If any of the
// values have (possibly further embedded) a cborrpc.PythonTuple or
// uuid.UUID value, this is encoded as a base64-encoded CBOR string;
// otherwise this is encoded as a normal JSON dictionary.
type DataDict map[string]interface{}

// Resource is a base type for all resources in this module.
type Resource struct {
	// URL points at this resource.  If this record is a "short"
	// record, the contents of this URL are the full record.  This
	// field does not need to be provided when posting data (and
	// indeed for HTTP PUT requests you need to know the URL to
	// post at all).
	URL string `json:"url"`
}

// NamedResource is a resource with a name.  Most of the Coordinate
// objects have names.
type NamedResource struct {
	Resource

	// Name holds the name of this resource.  This is generally
	// immutable.  This field does not need to be provided when
	// posting data.
	Name string `json:"name"`
}

// RootData is returned by the root path.
type RootData struct {
	Resource

	// NamespacesURL points at the namespace list.  This endpoint
	// supports HTTP GET to return a NamespaceList.  This endpoint
	// also supports HTTP POST to submit a new Namespace,
	// returning a NamespaceShort pointing at the result a
	// NamespaceList.
	NamespacesURL string `json:"namespaces_url"`

	// NamespaceURL points at the representation of a single
	// namespace.  This endpoint supports HTTP GET, PUT, and
	// DELETE, and its representation is a Namespace.  HTTP GET
	// will create a new namespace without specially notifying the
	// caller.  This field is a URI template with a single
	// parameter, "namespace", which should be substituted for the
	// (possibly escaped) name of the namespace.
	NamespaceURL string `json:"namespace_url"`
}

// NamespaceShort provides minimal data to identify a single namespace.
type NamespaceShort struct {
	NamedResource
}

// NamespaceList is a list of NamespaceShort.
type NamespaceList struct {
	// Namespaces is a list of the namespaces available in the system.
	Namespaces []NamespaceShort `json:"namespaces"`
}

// Namespace provides pointers to associated data about a namespace.
type Namespace struct {
	NamespaceShort

	// WorkSpecsURL points at the list of work specs in this
	// namespace.  This endpoint supports HTTP GET, returning a
	// WorkSpecList, and HTTP POST, to submit a WorkSpec and
	// return a WorkSpecShort.
	WorkSpecsURL string `json:"work_specs_url"`

	// WorkSpecURL points at the representation of a single work
	// spec.  This endpoint supports HTTP GET, PUT, and DELETE,
	// and its representation is a WorkSpec.  This is a URI
	// template with a single parameter, "spec", which should be
	// substituted for the (possibly escaped) name of the work
	// spec.
	WorkSpecURL string `json:"work_spec_url"`

	// WorkersURL points at the list of workers in this namespace.
	// This endpoint supports HTTP GET, returning a WorkersList,
	// and HTTP POST, to submit a Worker and return a WorkerShort.
	//
	// The semantics of HTTP GET of this URL are likely to change
	// in the future.
	WorkersURL string `json:"workers_url"`

	// WorkerURL points at the representation of a single worker.
	// This endpoint supports HTTP GET and PUT, and its
	// representation is a Worker.  This is a URI template with a
	// single parameter, "worker", which should be substituted for
	// the (possibly escaped) name of the worker.
	//
	// The Coordinate API defines three basic changes to workers,
	// deactivating them, updating (and reactivating) them, and
	// changing their parents.  All of these are performed by HTTP
	// PUT to this endpoint.
	WorkerURL string `json:"worker_url"`
}

// WorkSpecShort provides data that identifies a work spec, but no more.
type WorkSpecShort struct {
	NamedResource
}

// WorkSpecList is a list of WorkSpecShort.
type WorkSpecList struct {
	// WorkSpecs contains the embedded list of work specs.
	WorkSpecs []WorkSpecShort `json:"work_specs"`
}

// WorkSpec contains all of the details for a single work spec.  When
// submitting, only "data" is required, and it must itself have a
// "name" field.
type WorkSpec struct {
	WorkSpecShort

	// Data is the user-provided data dictionary.  In JSON it may
	// be either an object or a string; if a string, it is a
	// base64-encoded CBOR encoding of a map.
	Data DataDict `json:"data"`

	// WorkUnitsURL points at the list of work units in this work
	// spec.  This endpoint supports HTTP GET, returning a
	// WorkUnitList, and HTTP POST, submitting a WorkUnit and
	// returning a WorkUnitShort to create a new work unit.  The
	// HTTP GET response includes every work unit in this work
	// spec; WorkUnitQueryURL is more flexible.
	WorkUnitsURL string `json:"work_units_url"`

	// WorkUnitQueryURL retrieves a subset of the work units for
	// this work spec.  This endpoint supports HTTP GET, returning
	// a WorkUnitList, and HTTP DELETE, returning a count via a
	// WorkUnitDeleted object. This is a URI template with
	// parameters "name", "status", "previous", and "limit",
	// matching the fields in the WorkUnitQuery object.
	WorkUnitQueryURL string `json:"work_unit_query_url"`

	// WorkUnitURL points at a single work unit by name.  This
	// endpoint supports HTTP GET, PUT, and DELETE, and its
	// representation is a WorkUnit.  This is a template URI with
	// a single parameter, "unit", that should be substituted for
	// the (possibly escaped) name of the work unit.
	//
	// HTTP PUT to this endpoint is limited.  If the Priority
	// field is provided, it changes the priority of this work
	// unit to that value.  If ActiveAttemptURL is provided and
	// set to "-", it clears the active attempt; this is an
	// exception to the general rule that URLs cannot be
	// resubmitted.  No other changes are allowed, and if other
	// fields are provided (including Data) they are ignored.
	WorkUnitURL string `json:"work_unit_url"`

	// WorkUnitCountsURL points at summary data about how many
	// work units are in this work spec.  This endpoint only
	// supports HTTP GET, and returns a
	// map[coordinate.WorkUnitStatus]int; in JSON, this is an
	// object whose keys are strings matching the work unit
	// statuses, and whose values are numbers.
	WorkUnitCountsURL string `json:"work_unit_counts_url"`

	// WorkUnitChangeURL points at an endpoint to make bulk
	// changes to work units.  This endpoint only supports HTTP
	// POST, submitting a WorkUnit and returning nothing.  This is
	// a URI template with parameters "name", "status",
	// "previous", and "limit", matching the fields in the
	// WorkUnitQuery object.
	//
	// The only supported operation is to change the priority of
	// the matched work units by setting it to the Priority of the
	// posted data.  All other fields are ignored.
	WorkUnitChangeURL string `json:"work_unit_change_url"`

	// WorkUnitAdjustURL points at an endpoint to apply deltas to
	// several work units.  This endpoint only supports HTTP POST,
	// submitting a WorkUnit and returning nothing.  This is a URI
	// template with parameters "name", "status", "previous", and
	// "limit", matching the fields in the WorkUnitQuery object.
	//
	// The only supported operation is to adjust the priority of
	// the matched work units by adding the Priority of the posted
	// data to their current priorities.  All other fields are
	// ignored.
	WorkUnitAdjustURL string `json:"work_unit_adjust_url"`

	// MetaURL points at control metadata for this work spec.
	// This endpoint supports HTTP GET and PUT, and its
	// representation is a coordinate.WorkSpecMeta.  This is a
	// template URI with a single parameter, "counts", that
	// indicates whether counts of work units should be filled
	// in.
	//
	// Many of these fields are derived from the work spec data,
	// but can be set independently, for instance to pause or
	// resume a work spec.  Some fields cannot be set.  The
	// entire structure must be provided for HTTP PUT; otherwise
	// values will be reset to false or zero.
	MetaURL string `json:"meta"`
}

// WorkUnitShort provides minimal identifying information for a work
// unit.
type WorkUnitShort struct {
	NamedResource
}

// WorkUnitList is a list of WorkUnitShort.
type WorkUnitList struct {
	WorkUnits []WorkUnitShort `json:"work_units"`
}

// WorkUnit provides complete static data for a work unit.  (Coordinate
// 0.3.0 removes a "priority" field and replaces it with "meta".)
type WorkUnit struct {
	WorkUnitShort

	// Data is the user-provided work unit data.
	Data DataDict `json:"data,omitempty"`

	// Meta describes additional control information for this
	// work unit, such as its scheduling priority.
	Meta *coordinate.WorkUnitMeta `json:"meta"`

	// Status describes the overall status of this work unit,
	// which is a function of its active attempt.  This cannot
	// be directly changed.
	Status coordinate.WorkUnitStatus `json:"status"`

	// WorkSpecURL points to the work spec containing this unit.
	// See Namespace for further details.
	WorkSpecURL string `json:"work_spec_url"`

	// ActiveAttemptURL, if present, points to the current attempt
	// to complete this work unit.  This endpoint supports HTTP
	// GET and PUT, and its representation is an Attempt.
	//
	// As a special case, an HTTP PUT of a work unit with this
	// field set to "-" clears (and abandons) the active attempt.
	ActiveAttemptURL string `json:"active_attempt_url,omitempty"`

	// AttemptsURL points to an endpoint that retrieves all of the
	// attempts, past and current, for this work unit.  It only
	// supports HTTP GET, and its representation is an
	// AttemptList.
	AttemptsURL string `json:"attempts_url"`
}

// WorkUnitDeleted is the response to a batch delete request.
type WorkUnitDeleted struct {
	// Deleted has the number of work units actually deleted.
	Deleted int
}

// WorkerShort includes minimal data to identify a worker.
type WorkerShort struct {
	NamedResource
}

// Worker contains details for a single worker.
type Worker struct {
	WorkerShort

	// Parent gives the name of the parent worker.  If empty
	// string, this worker has no parent.  Setting this in a PUT
	// request changes the worker's parent.
	Parent *string `json:"parent,omitempty"`

	// ParentURL points at the parent worker object, if any.  If
	// absent or null, this worker has no parent.
	ParentURL string `json:"parent_url,omitempty"`

	// ChildURLs points at this worker's children, if any.  Their
	// content are Workers.  If absent, null, or an empty list,
	// this worker has no children.  This list cannot be directly
	// manipulated, but a child can set its parent to something
	// else.
	ChildURLs []string `json:"child_urls,omitempty"`

	// Active is a flag indicating whether this worker is still
	// alive.
	//
	// If this resource is PUT with Active set to false, and the
	// worker was previously active, it will be deactivated,
	// and all other fields will be ignored.  Otherwise, the worker
	// will update using other fields.
	Active bool `json:"active"`

	// Mode is intended to be the last observed mode of the
	// coordinate system as a whole; in practice it will usually
	// be "RUN".
	Mode string `json:"mode"`

	// Data is arbitrary worker-provided data.  This is intended
	// to be diagnostic data to more completely identify the
	// worker and its runtime environment.
	Data DataDict `json:"data"`

	// Expiration is a deadline by which the worker must update
	// itself, or automatically become inactive.  An update must
	// provide an expected expiration time, typically 15 minutes
	// in the future.
	Expiration time.Time `json:"expiration"`

	// LastUpdate records the last time the worker checked in.
	// An update must provide the current time in this field.
	LastUpdate time.Time `json:"last_update"`

	// RequestAttemptsURL points at an endpoint to request more
	// attempts.  This endpoint only supports HTTP POST, accepting
	// a coordinate.AttemptRequest structure and returning an
	// AttemptResponse.
	RequestAttemptsURL string `json:"request_attempts_url"`

	// MakeAttemptURL points at an endpoint to create a specific
	// attempt.  Generally RequestAttemptsURL is a better way to
	// get work to do.  This endpoint only supports HTTP POST,
	// accepting an AttemptSpecific and returning an Attempt.
	MakeAttemptURL string `json:"make_attempt_url"`

	// ActiveAttemptsURL, AllAttemptsURL, and ChildAttemptsURL
	// point at endpoints that return sets of attempts associated
	// with this worker.  These are attempts that this worker is
	// currently doing, all attempts that this worker has ever
	// done, and this worker's children's active attempts,
	// respectively.  These endpoints all only support HTTP GET
	// and return AttemptList.
	ActiveAttemptsURL string `json:"active_attempts_url"`
	AllAttemptsURL    string `json:"all_attempts_url"`
	ChildAttemptsURL  string `json:"child_attempts_url"`
}

// AttemptSpecific names a specific work unit to attempt.  This is the
// input parameter to the Worker.MakeAttemptURL endpoint.
type AttemptSpecific struct {
	// WorkSpec holds the name of the work spec.
	WorkSpec string `json:"work_spec"`

	// WorkUnit holds the name of the work unit.
	WorkUnit string `json:"work_unit"`

	// Lifetime is the minimum requested time to perform this
	// attempt; it must be completed or renewed by this deadline.
	// If zero, use a system-provided default, generally 15
	// minutes.
	Lifetime time.Duration `json:"lifetime"`
}

// AttemptResponse contains the response to the
// Worker.RequestAttemptsURL endpoint.
type AttemptResponse struct {
	// WorkSpecURL points at the work spec for all of the work
	// units, if any are returned.  Its representation is a
	// WorkSpec.
	WorkSpecURL string `json:"work_spec_url,omitempty"`

	// Attempts contains a list of Attempt.  This includes full data
	// on the attempt, in particular including its action URLs.
	Attempts []Attempt `json:"attempts"`
}

// AttemptShort contains minimum information to identify an attempt.
// Note that attempts do not have names or unique identifiers.  This
// treats an attempt by a specific worker to do a specific work unit
// at a specific start time as unique.
type AttemptShort struct {
	Resource

	// WorkUnitURL points at the work unit being performed.  Its
	// representation is a WorkUnit.
	WorkUnitURL string `json:"work_unit_url"`

	// WorkerURL points at the worker doing the work.  Its
	// representation is a Worker.
	WorkerURL string `json:"worker_url"`

	// StartTime contains the time the attempt was created.  This
	// is in RFC 3339 format, e.g. "2012-03-04T05:06:07.890Z".
	StartTime time.Time `json:"start_time"`
}

// AttemptList holds a list of AttemptShort.
type AttemptList struct {
	// Attempts contains the actual attempts in this representation.
	Attempts []AttemptShort `json:"attempts"`
}

// Attempt contains complete current information about an attempt.
type Attempt struct {
	AttemptShort

	// Status has the current status of this attempt.
	Status coordinate.AttemptStatus `json:"status"`

	// Data holds the updated work unit data for this attempt.  If
	// this field is null or absent then the attempt has not
	// updated the data, and the original work unit data prevails.
	Data DataDict `json:"data,omitempty"`

	// EndTime contains the time the attempt completed.  If this
	// field is absent then the attempt is not yet completed.
	// This is in RFC 3339 format,
	// e.g. "2012-03-04T05:06:07.890Z".
	EndTime time.Time `json:"end_time,omitempty"`

	// ExpirationTime contains the time at which the attempt will
	// become available to other workers again.  If this field is
	// absent then the attempt is not completed.  This is in RFC
	// 3339 format, e.g. "2012-03-04T05:06:07.890Z".
	ExpirationTime time.Time `json:"expiration_time"`

	// RenewURL, ExpireURL, FinishURL, FailURL, and RetryURL each
	// point to endpoints to change the state of this attempt.
	// These endpoints only support HTTP POST, accepting an
	// AttemptCompletion and returning nothing.
	RenewURL  string `json:"renew_url"`
	ExpireURL string `json:"expire_url"`
	FinishURL string `json:"finish_url"`
	FailURL   string `json:"fail_url"`
	RetryURL  string `json:"retry_url"`
}

// AttemptCompletion contains data submitted as part of one of the
// requests to complete or renew an attempt.
type AttemptCompletion struct {
	// Data holds updated data for the attempt.  If absent the
	// attempt (and thus, derived work unit) data is not updated.
	Data DataDict `json:"data,omitempty"`

	// ExtendDuration holds the further length of time to extend
	// the attempt, if this is a renew request.  This is a number
	// in nanoseconds.
	ExtendDuration time.Duration `json:"extend_duration"`

	// Delay holds the length of time to wait before retrying the
	// work unit, if this is a retry request.  This is a number in
	// nanoseconds.  (Added in Coordinate 0.3.0)
	Delay time.Duration `json:"delay"`
}

// ErrorResponse can be a response to any method, generally accompanied
// by a failing HTTP status code.
type ErrorResponse struct {
	// Error is a short description of the failure.  This may be
	// the name or type of a coordinate API error, the string
	// "panic", or the string "error" for some other kind of
	// error.
	Error string `json:"error"`

	// Message is a human-readable description of the failure.
	Message string `json:"message"`

	// Value is an extra parameter to the error if applicable.
	Value string `json:"value,omitempty"`

	// Stack holds a formatted backtrace, if the method failed
	// due to a panic.
	Stack string `json:"stack,omitempty"`
}
