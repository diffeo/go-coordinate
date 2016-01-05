Work Specs: Additional Details
===============================

Special keys
------------

A work spec is defined as a JSON object, as described in the
[data model](model.md).  Many keys in the object are recognized as
special by the system.

`name`: Gives the name of the work spec.  Its value must be a string,
and it cannot be changed after initial creation.  This field is
required.

`min_gb`: Gives the minimum amount of memory this work spec needs.
Its value is a number.  The
[Python coordinated](https://github.com/diffeo/coordinate) and the
older [rejester](https://github.com/diffeo/rejester) required this
field, but actual enforcement or connection to real memory resources
has always been spotty.  For compatibility it is better to specify
this field, but it is not used in the Go coordinated implementation.

`disabled`: Indicates that the work spec will start paused.  Its value
is a boolean, and it defaults to false.  It is the boolean negation of
the "paused" field in the work spec metadata.

`continuous`: Indicates that the work spec expects to get a continuous
sequence of artificial work units.  Its value is a boolean, and it
defaults to false.  If set, the scheduler will still schedule this
work spec even if there are no available work units, and in this case
a work unit will be created with the current Unix time as its name.
This matches a corresponding "continous" field in the work spec
metadata, which can be used to disable continuous job creation, but if
this flag was not set when the work spec was created, it cannot be
later enabled.

`interval`: If the work spec gets continuous work units, gives a
minimum interval, in seconds, between consecutive work units.  Its
value is a number, and it defaults to zero (new continuous work units
can be created immediately).  This matches a corresponding "interval"
field in the work spec metadata.

`priority`: Gives an absolute priority for this work spec.  Its value
is a number, and it defaults to 0.  If two work specs both have
available work units (or are marked continuous) and one has higher
priority, the higher-priority one will always be selected.  This
matches a corresponding "priority" field in the work spec metadata.

`weight`: Gives a relative weight for this work spec.  Its value is a
number, and it defaults to 20.  Among work specs with the same
priority, the scheduler will try to ensure that the ratio of the
number of pending work units each has matches the ratio of their
weights.  If work spec "a" has a weight of 1 and "b" has a weight of
2, the scheduler will try to ensure that twice as many "b" jobs as "a"
are always running.  This matches a corresponding "weight" field in
the work spec metadata.

`nice`: An alternate way of setting `weight`.  Its value is a number.
It is ignored if `weight` is specified.  If `weight` is not specified
but `nice` is, the weight is set to 20 - nice.

`max_running`: Sets a hard global limit on the number of work units
allowed to be running at once for this work spec.  Its value is a
number, and it defaults to 0 (unlimited).  If non-zero, then no more
than this many attempts will be returned for a single request, and if
this many attempts are already pending, its work spec will not be
considered.  This matches a corresponding "max running" field in the
work spec metadata.

`max_getwork`: Limits the number of attempts that will be returned
from a single request.  Its value is a number, and it defaults to 0
(unlimited).  If non-zero, a single call to `Worker.RequestAttempts()`
will return at most the lower of this number and the number of
attempts requested.  This matches a corresponding "max attempts
returned" field in the work spec metadata.

`then`: Gives the name of another work spec to run after this one.
Its value is a string.  If this names another valid work spec and work
units complete with an `output` key in their work unit data, more work
units will be created.  This matches a corresponding "next work spec
name" field in the work spec metadata.  Read more about
[work unit chaining](chaining.md).

`then_preempts`: Controls the scheduler in the Python coordinate
daemon, not used in this implementation.  Its value is a boolean, and
it defaults to true.  In the Python coordinate daemon, by default,
work units in the `then` work spec take absolute priority over this
one, with the intent that a chain of work specs would create a flow or
pipeline of work where intermediate progress should be minimized.
Practical experience suggested this feature was not useful, and work
specs with a `then` field often also have `"then_preempts": false`.
This field is ignored by the Go coordinate daemon.

`runtime`: Gives the name of a language runtime.  Its value is a
string, and it defaults to an empty string.  This work spec will only
be considered in a call to `Worker.RequestAttempts` if this exact
string is present in the attempt request.  This matches a
corresponding "runtime" field in the work spec metadata.  Read more
about [runtimes](runtime.md).

`module`: Names a Python module holding the code for this work spec.
Its value is a string.  Only used by the Python worker, which in turn
is only used if `runtime` is empty, but required then.

`run_function`: Names a Python function visible in `module`.  Its
value is a string.  The corresponding function is called with a
`coordinate.WorkUnit` object.  Only used by the Python worker, which
in turn is only used if `runtime` is empty, but required then.

`terminate_function`: Names a Python function visible in `module`.
Its value is a string.  The corresponding function is called with a
`coordinate.WorkUnit` object.  Dates back to
[rejester](https://github.com/diffeo/rejester), with a vague
expectation that it could be called in response to `SIGTERM`; it was
never well-specified when it could be called or what action the task
should take.  The intent was to have `terminate_function` kill
long-running subprocesses.  The current Python coordinate worker never
calls this, but may be present in some work specs nevertheless.

`config`: Defines configuration specific to this work spec.  Its value
is an object.  This is not formally defined by Coordinate, but it is
widely used by Python-based tasks, which expect it to be a complete
system [yakonfig](https://github.com/diffeo/yakonfig) configuration.

Metadata
--------

Work specs have an associated metadata object.  Reloading the work
spec will reset its metadata object to values derived from the work
spec.  The metadata object may also be controlled separately.

Almost all of the metadata fields have corresponding work spec data
fields, and these are noted above.  Notable fields include:

`Paused`: if a work spec is paused, the scheduler will not consider it
and no new attempts will be created for its work units.  This is the
opposite of the `disabled` flag in the work spec data.

`Continuous`, `CanBeContinuous`: the "can be continuous" flag holds
the `continuous` flag from the work spec data, and cannot be changed.
"Continuous" can only be enabled if "can be continuous" is true.

`NextContinuous`: the time the last work unit was generated for a
continuous work spec, plus `Interval`.  Defaults to zero
(immediately), which means that reloading a work spec could cause a
new continuous work unit to be generated immediately even if the
interval hasn't passed yet.

`MaxAttemptsReturned`: matches the `max_getwork` data field.

`NextWorkSpecName`: matches the `then` data field.  Ignored if it does
not match the name of another work spec or if the completed work unit
data does not have an `output` key.  Cannot be set without reloading
the work spec.

`AvailableCount`, `PendingCount`: must be explicitly requested.
"Pending count" gives the actual number of work units with active
attempts in "pending" status, and is needed for the scheduler.
"Available count" only needs to be 0 or 1 for the scheduler's benefit,
or it may reflect the actual number of available work units (with no
active attempt and either without a not-before time or whose
not-before time has passed).  Cannot be set, but may change as work
proceeds.

`Runtime`: matches the `runtime` data field.  Cannot be set without
reloading the work spec.

Scheduling
----------

Calls to `Worker.RequestAttempts` use a common scheduling system
across all backends.  (This is different from the scheduler in the
[Python coordinated](https://github.com/diffeo/coordinate); see the
"Scheduling" section of this package's
[Python documentation](python.md) for details.)

**Filtering work specs:** Consider all of the work specs, but:

* Do not consider any work spec that is paused or has a negative weight.
* Do not consider any work spec whose runtime is not one of those
  listed in the attempt request, if the attempt request listed
  runtimes.
* Do not consider any work spec that is not one of those listed in the
  attempt request, if the attempt request listed specific work spec
  names.
* Do not consider any work spec that has a max-running value and
  already has that many pending work units.
* Do not consider any work spec that does not have available work
  units, unless it can execute continuous work units, it has no work
  units currently pending, and the minimum interval (if any) has
  passed.

If this filtering discards all work specs, the system has no work to
do and the worker will be idle.

**Priorities:** Find the highest priority of all remaining work specs.
Discard all work specs that have a lower priority.

**Weights:** Look at the weight and pending count of all remaining
work specs.  Assume one more work unit will become pending;
probabilistically choose a work spec so that the ratio of the
pending counts becomes closer to the ratio of the weights.

**Picking work units:** Look at all of the available work units in the
selected work spec.  Choose the best work units, not more than the
number requested in the attempt request and not more than the
max-attempts-returned value in the work spec metadata.  "Best" means
those with the highest priority values, and of those with equal
priority values, those with alphabetically earlier work unit names.

**Continuous work units:** If the work unit selection chose a work
spec with no available work units but with the "continuous" flag set
in its metadata, create a new work unit whose name is the Unix
timestamp of its creation (seconds since midnight 1 Jan 1970 UTC) with
an empty data dictionary and priority 0, and run it.  Regardless of
how many units were requested, only create and return one continuous
unit.
