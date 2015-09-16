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
@pytest.fixture
def task_master():
  return TaskMaster({'address': '127.0.0.1:5932'})
```

Packages
--------

`goordinated` is the main process, providing the network service.
`jobserver` provides the RPC calls compatible with the Python
Coordinate system.  `cborrpc` provides the underlying wire transport.

`coordinate` describes an abstract API to Coordinate.  This API is
slightly different from the Python Coordinate API; in particular, an
Attempt object records a single worker working on a single work unit,
allowing the history of workers and individual work units to be
tracked.  `memory` is the in-memory implementation of this API.
`backend` provides a command-line option to choose a backend.

Status
------

(As of 15 Sep 2015)

The `coordinate summary` command, and the most basic flows that create
work specs, add work units, get work, and complete it work.  This
passes 3 tests in `test_job_client.py`.

About a dozen Coordinate RPC calls are unimplemented.  There is
currently no work scheduler (the alphabetically first work unit from
any work spec is returned).  Neither job chaining (work spec `then`
key) nor continuous jobs are implemented.

Currently the only storage backend is in-memory, but the abstract API
is intended to support other storage systems.  In particular,
PostgreSQL support is planned.

This implementation also has no way to make outbound calls to a
Coordinate server from a Go program.  This will likely involve
creating a new network service, possibly a REST API mirroring the
Coordinate API.
