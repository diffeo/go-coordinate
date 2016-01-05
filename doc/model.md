Data Model
==========

The core purpose of Coordinate is to store and track a sequenc of
jobs, or _work units_.  These work units are grouped together by
related tasks, or _work specs_; for instance, you could define a work
spec that extracted text from a PDF file, and then define work units
that were individual files to process.

Namespaces
----------

The Coordinate system manages a set of logically separate namespaces.
Each namespace is defined by its name.  No state is shared between
namespaces.  A namespace has any number of work specs and any number
of workers.

Work specs
----------

A work spec defines a family of related tasks, generally running
near-identical code over a number of different inputs.  The
Python-based worker embeds a Python module and function name in the
work spec data, for instance, and calls this function every time a
work unit is executed.  A work unit is defined by a JSON object
(Python dictionary, Go `map[string]interface{}`) which is required to
have a key `name` with a string value.

There are several [special work spec keys](work_specs.md).  Work specs
can also be controlled in several ways via a metadata object; for
instance a work spec can be paused so that no new work will be
returned from it.

Work units
----------

A work unit defines a single task to perform within the context of a
single work spec; for instance a single file or database record.  A
work unit is defined by its name (in some contexts also called a key)
and an arbitrary data dictionary.  Individual work units have their
own metadata objects, though with many fewer options than the work
spec metadata.

Workers
-------

A worker is a process (in the Unix sense) that executes work units.  A
worker is defined by its name.  For diagnostic purposes, workers
should periodically "check in" and upload an environment dictionary.
Workers are arranged hierarchically to support a setup where one
parent worker manages a family of child worker processes, one per
core.

Attempts
--------

An attempt is a record that a specific worker is attempting (or has
attempted) a specific work unit.  The attempt has a status, which may
be "pending" or may record a completed (or incomplete-but-terminated)
attempt.  Attempts also record an updated data dictionary for their
work unit.

A work unit has at most one _active_ attempt.  If a work unit does
have an active attempt, its status should be "pending", "finished", or
"failed", but not "expired" or "retryable".  Work units also can
retrieve all of their past attempts.

Workers similarly keep lists of active and past attempts.  If an
attempt is on a worker's active list, that means the worker is
spending cycles on it, though if that attempt is no longer the active
attempt for its work unit it may be a waste of computation.

Data Objects
------------

Work specs, work units, and workers all have data objects, and
attempts keep updated data objects for their work unit.  These are
generally treated as JSON objects, and usually have a Go type of
`map[string]interface{}`.  Treatment as JSON means that in some cases
exact data types may not be preserved: map types may be converted to
`map[string]interface{}`, array types to `[]interface{}`, and numeric
types to `float64`.

Additional type information stored within the system also preserves
values of type `uuid.UUID` from `github.com/satori/go.uuid`, and a
`PythonTuple` type native to this package.  If Python code uploads
data using the `uuid.UUID` type from the standard library or native
Python tuples, these will be preserved.
