Concurrency and Deletion
========================

Imagine code that gets a `coordinate.WorkUnit` object and is preparing
to do some work with it.  Meanwhile, another process blindly deletes
all of the work units.  When happens to the first process, that has a
work unit object, and then tries to use it?  What if the other process
kindly recreates a new work unit with the same name in the meantime?

Prior to Coordinate 0.3.0, this story was extremely backend-dependent.
The `memory` backend would calmly allow you to use the stale work unit
as though nothing changed, and the new work unit with a different name
would be a different object.  The `postgres` backend in many cases
would pass through `sql.ErrNoRows`.  The `restclient` backend always
does name-based lookup, and would pass through an HTTP 404 error if
the unit was gone and silently use the new work unit if it was
recreated.

Coordinate 0.3.0 adds a new error, `coordinate.ErrGone`, though its
use is still not totally consistent.  `memory` will return `ErrGone`
from all operations on a deleted work unit, work spec, or namespace,
or a descendant of one of those.  `postgres` will return `ErrGone`
when it can definitively determine that this is the right answer,
which usually means that functions that return lists or maps of things
will return empty lists if an object has been deleted but functions
that get or set a single property will return `ErrGone`.  `restclient`
and `restserver` treat `ErrGone` as a standard error, but will still
do name-based lookup, and so could return more specific errors like
`ErrNoSuchWorkUnit`, and if a work unit is recreated, the object in
the calling process will continue to silently refer to the new record.

0.3.0's behavior is motivated by the need, for performance reasons, to
cache Coordinate objects: if `restserver` always does name-based
lookups and HTTP requests are stateless, and it is PostgreSQL-backed,
then every request needs to find a namespace object, a work spec
object, a work unit object, and an attempt object, and these are
frequently objects that a caller has already been using.  `ErrGone`
gives a backend-neutral way to tell the cache layer that an object no
longer exists and it should be removed, and even if it's not used
totally consistently, having it is important (and having it is far
better than passing through `sql.ErrNoRows`).

Really truly solving this issue involves adding a notion of object
identity to Coordinate.  There are two parts to this: every object
gains an `ID() int` method that returns a globally unique (per type)
integer identifier, and the top-level `Coordinate` interface has a
family of `WorkSpecWithID(int) (WorkSpec, error)` and similar
functions.  The implications of this are all in the REST API: instead
of doing name-based lookup, it does ID-based lookup, and URLs change
from `/namespace/-/work_spec/foo/work_unit/bar` to just
`/work_unit/17`.  This solves the problem of recreating an object with
the same name, makes URLs much shorter, interfaces better with REST
libraries, and makes it possible to directly address attempts rather
than indirectly referencing them by their attributes.  Currently the
only two "concrete" backends in Coordinate, `memory` and `postgres`,
could easily add this; are there other obvious backends where a
globally unique integer ID is hard to construct, maybe because they
speak only in terms of UUIDs?

Having the globally unique ID and well-defined semantics for `ErrGone`
provides one more possibility.  Say `WorkSpecWithID` does not return
an error, but the object it returns also does not have useful links to
its parent or a name.  In the `postgres` case, it creates a `workSpec`
object with a caller-provided ID but no other data, and there is no
lookup at the time it is created.  Now operations on it can return
`ErrGone` in the unusual cases where that happens, without having to
do an extra database operation to fetch it where you don't need it.
The benefits of this may be limited (you frequently need to know a
work unit's work spec or an attempt's work unit).
