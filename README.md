Go Coordinate Daemon
====================

This package provides a reimplementation of the Diffeo Coordinate
(https://github.com/diffeo/coordinate) daemon.  It does not
reimplement the worker process, and it does not (yet) have any sort of
client code to talk back to itself.  As such, it is only useful with
existing Python Coordinate code.

Overview
--------

Coordinate is a job queue system.  It is designed for repetitive tasks
with large numbers of inputs, where the inputs and outputs will be
stored externally and do not need to be passed directly through the
system, and where no particular action needs to be taken when a job
finishes.

Coordinate-based applications can define _work specs_, JSON or YAML
dictionary objects that define specific work to do.  A typical work
spec would name a specific Python function to call with a YAML
configuration dictionary, and the Python Coordinate package contains a
worker process that can run these work specs.  Each work spec has a
list of _work units_, individual tasks to perform, where each work
unit has a name or key and an additional data dictionary.  In typical
operation a work unit key is a filename or database key and the data
is used only to record outputs.

The general expectation is that there will be, at most, dozens of work
specs, but each work spec could have millions of work units.  It is
definitely expected that many worker processes will connect to a
single Coordinate daemon, and past data loads have involved 800 or
more workers talking to one server.

Installation
------------

    go get github.com/dmaze/goordinate/goordinated

Usage
-----

Run the `goordinated` binary.  With default options, it will use
in-memory storage and start a network server listening on port 5932.
This is the default TCP port for the Python Coordinate daemon, and
application configurations that normally point at that daemon should
work against this one as well.

```sh
pip install coordinate
go get github.com/dmaze/goordinate/goordinated
cat >config.yaml <<EOF
coordinate:
  addresses: ['localhost:5932']
EOF
coordinate -c config.yaml summary
```

To run the Python tests against this daemon, edit
`coordinate/tests/test_job_client.py`, rename the existing
`task_master` fixture, and add instead

```python
@pytest.yield_fixture
def task_master():
    tm = TaskMaster({'address': '127.0.0.1:5932'})
    yield tm
    tm.clear()
```

`test_job_client.py`, `test_job_flow.py`, and `test_task_master.py`
all use this fixture and will run against the `goordinated` server.
Many of these tests have been extracted into Go tests in the
"jobserver" package.

Differences from Python Coordinate
----------------------------------

Most Python Coordinate applications should run successfully against
this server.  Known issues are noted in the "status" section below.

Work spec names must be valid Unicode strings.  Work spec definitions
and work unit data must be Unicode-string-keyed maps.  Work unit keys,
however, can be arbitrary byte strings.  Python (especially Python 2)
is sloppy about byte vs. character strings and it is easy to inject
the wrong type; if you do create a work spec with a non-UTF-8 byte
string name, the server will eventually return it as an invalid
Unicode-tagged string.

The work spec scheduler is much simpler than in the Python
coordinated.  The scheduler only considers work specs' `priority` and
`weight` fields.  Work specs with no work are discarded; then work
specs not of the highest priority are discarded; then the scheduler
randomly chooses a work spec to try to make the ratio of the number of
pending work units match the weights.  Work specs' successors, as
identified by the `then` field, do not factor into the scheduler, and
the `then_preempts` field is ignored.

For example, given work specs:

```yaml
flows:
  a:
    weight: 1000
    then: b
  b:
    weight: 1
``` 

Add two work units to "a", and have an implementation that copies
those work units to the work unit data `output` field.

```python
def run_function(work_unit):
  work_unit.data["output"] = {work_unit.key: {}}
```

The Python coordinated will run a work unit from "a", producing one in
"b"; then its rule that later work specs take precedence applies, and
it will run the work unit in "b"; then "a", then "b".  This
implementation does not have that precedence rule, and so the second
request for work will (very probably) get the second work unit from
"a" in accordance with the specified weights.

If you need a later work spec to preempt an earlier one, set a
`priority` key on the later work spec.

Packages
--------

`goordinated` is the main process, providing the network service.
`jobserver` provides the RPC calls compatible with the Python
Coordinate system.  `cborrpc` provides the underlying wire transport.

`coordinate` describes an abstract API to Coordinate.  This API is
slightly different from the Python Coordinate API; in particular, an
Attempt object records a single worker working on a single work unit,
allowing the history of workers and individual work units to be
tracked.  `memory` is the in-memory implementation of this API, and
`postgres` uses PostgreSQL.  `backend` provides a command-line option
to choose a backend.

Status
------

(As of 13 Oct 2015)

The overall system is probably usable for some applications, though I
have not done any particular real-world testing yet.  With the
exception of continuous jobs, the entire system should work as
expected, including passing all of the Python tests (and their ported
equivalents here).  The PostgreSQL backend needs performance tuning.

This implementation has no way to make outbound calls to a Coordinate
server from a Go program.  This will likely involve creating a new
network service, possibly a REST API mirroring the Coordinate API.
