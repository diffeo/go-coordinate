(Re)Design of Coordinated
=========================

Diffeo `coordinated` was a redesign of
[rejester](http://github.com/diffeo/rejester), and the merged
[Coordinate](http://github.com/diffeo/coordinate) follows the
`coordinated` design and code base.

Principles
----------

**Distributed and stateless.** The Coordinate daemon should store all
of its state somewhere persistent and remote, like a central external
database.  While limited caching may be appropriate, state should be
in the database first.  This allows multiple Coordinate daemons to be
running in a cluster, which provides some degree of redundancy and
helps migrations.  If code relies on the database to provide
concurrency protection, then this minimizes the amount of troublesome
code within the daemon itself to manage intra-server concurrency.

**Abstract API.** There are at least three useful implementations of
the Python Coordinate `TaskMaster` object: the rejester version, the
standard Coordinate version, and a unit-test version that hard-wires
the Coordinate client to a Coordinate job server object.  These should
share a common API.

Object Model
------------

**Namespace.** The Coordinate server tracks any number of namespaces.
A namespace is an unordered collection of work specs.  In the future
it may gain an access control policy or other system-wide data.

**Work spec.** A work spec is a definition of what to do for a set of
units.  It might define a specific Python function to call, for
instance, where a work unit gives a set of parameters.  A work spec
belongs to a single namespace.  There are typically at most dozens of
work specs in a namespace.  The definition of a work spec is described
as a JSON object with a number of well-known keys, such as `name`.

**Work unit.** A work unit is a single thing to do for a given work
spec.  It might correspond to a single input document in the system or
other job.  If it is convenient to batch together smaller units of
work, a single Coordinate work unit might be tuned to take about one
minute to execute.  A work unit has a key and a data JSON object,
along with an overall status.

**Worker.** A worker is a process that executes work units.  A worker
may also be a parent worker that is responsible for child worker
processes.  A worker chooses a unique ID for itself, and should
periodically report its status and system environment to the
Coordinate server.

**Attempt.** An attempt is a record that a specific worker is working
on a specific work unit.  A work unit has at most one active attempt.
Workers also have lists of active attempts, but they are not strictly
limited to doing one at a time.  The attempt includes a new data
dictionary for the work unit, which may include any generated outputs
or failure information.  An attempt must be completed by some deadline
(which the worker may extend) or else it ceases to be the active
attempt.

Changes from Python Coordinate
------------------------------

**Reintroduction of namespaces.** Rejester had the concept of a
"namespace", allowing there to be multiple sets of work specs on a
single (Redis) server.  While this was most useful for testing, it
also supported a shared server if users and tests could come up with
reasonably unique namespace names.  We add back in the namespace
concept.

**Attempt tracking.** Both rejester and Coordinate had a basic work
flow for work units: they would move from "available" to "pending",
then either "finished" or "failed".  If the assigned worker failed to
complete the work unit promptly, it could silently move back from
"pending" to "available".  We add the concept of an "attempt" that
keeps track of who is doing work and what the result of that attempt
was.

**Work unit data lifespan.** In Python Coordinate (and rejester)
changing a work unit's data changes it globally, even if the work unit
does not complete successfully.  This implementation attaches the
changed data to an attempt.  In practice this is only a significant
change if a work unit fails and is retried.
