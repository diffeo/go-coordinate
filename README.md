Go Coordinate Daemon
====================

[![CircleCI](https://circleci.com/gh/diffeo/go-coordinate.svg?style=svg)](https://circleci.com/gh/diffeo/go-coordinate)
[![Docker Hub Repository](https://img.shields.io/docker/pulls/diffeo/coordinated.svg "Docker Hub Repository")](https://hub.docker.com/r/diffeo/coordinated/)

This package provides a reimplementation of the Diffeo Coordinate
(https://github.com/diffeo/coordinate) daemon.  It is fully compatible
with existing Python Coordinate code, and provides a REST interface
for Go and other languages.

* [Documentation index](doc/index.md)
* [Change history](doc/changes.md)

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
is used only to record outputs.  Read more about the
[data model](doc/model.md).

The general expectation is that there will be, at most, dozens of work
specs, but each work spec could have millions of work units.  It is
definitely expected that many worker processes will connect to a
single Coordinate daemon, and past data loads have involved 800 or
more workers talking to one server.

Installation
------------

From source:

    go get github.com/diffeo/go-coordinate/cmd/coordinated

Usage
-----

Run the `coordinated` binary.  With default options, it will use
in-memory storage and start a network server listening on ports 5932
and 5980.  Port 5980 provides the REST interface.

Go code should use the `backend` package to provide a command-line
flag to get a backend object, which will implement the generic
interface in the `coordinate` package.  Test code can directly create
a `memory` backend.  Most applications will expect to use the
`restclient` backend to communicate with a central Coordinate daemon.

5932 is the default TCP port for the Python Coordinate daemon, and
application configurations that normally point at that daemon should
work against this one as well.  Read more about
[Python compatibility](doc/python.md).

```sh
pip install coordinate
go get github.com/diffeo/go-coordinate/cmd/coordinated
$GOPATH/bin/coordinated &
cat >config.yaml <<EOF
coordinate:
  addresses: ['localhost:5932']
EOF
coordinate -c config.yaml summary
```

Docker
------

A [Coordinate daemon server image](https://hub.docker.com/r/diffeo/coordinated/)
is on Docker Hub:

```sh
docker run -p 5932:5932 -p 5980:5980 diffeo/coordinated
```

This is a single-binary image that only runs the daemon.  If you need
any additional command-line options, such as a persistent backend,
specify them directly after the image name.

```sh
docker run -d -p 5432:5432 postgres:9.5
docker run -d -p 5932:5932 -p 5980:5980 diffeo/coordinated \
    -backend postgres://172.17.0.1 -log-requests
```

The current CI setup has the Docker `latest` tag pointing at a
`master` commit from this repository.  You may want to specify a
specific version tag.  The earliest version tag in Docker Hub is
`diffeo/coordinated:0.4.2`.

Packages
--------

`cmd/coordinated` is the main process, providing the network service.
`jobserver` provides the RPC calls compatible with the Python
Coordinate system.  `cborrpc` provides the underlying wire transport.

`coordinate` describes an abstract API to Coordinate.  This API is
slightly different from the Python Coordinate API; in particular, an
Attempt object records a single worker working on a single work unit,
allowing the history of workers and individual work units to be
tracked.  `memory` is the in-memory implementation of this API,
`postgres` uses PostgreSQL, and `restclient` talks to a REST server.
`backend` provides a command-line option to choose a backend.

Future
------

The `Namespace.Workers()` call simply iterates all known workers, but
the implementation of the Python Coordinate worker will generate an
extremely large number of these.  This call is subject to unspecified
future change.

Go Coordinate version 0.3.0 adds a generic WorkUnitMeta structure,
which defines both the work unit priority and the earliest time it can
execute.  This replaces the priority field in a couple of contexts.  A
future version of Go Coordinate not before 0.4.0 will delete
`WorkUnit.Priority()` and `WorkUnit.SetPriority()`.
