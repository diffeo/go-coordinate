Runtimes
========

This version of coordinated supports the concept of workers running
multiple language runtimes.  The Python worker in the
[coordinate package](https://github.com/diffeo/coordinate) can't run
Go code, for instance.  This feature was added in Go Coordinate 0.2.0.

Work Spec
---------

If a work spec contains a key `runtime`, it is the name of a language
runtime that is required to run that work spec.  This is generally a
short description such as `python_2`, `go`, or `java_1.7`.  In the Go
API, the runtime can be retrieved from the immutable
`WorkSpecMeta.Runtime` field.

For backwards compatibility, an empty runtime string should generally
be interpreted as equivalent to `python_2`.

Attempt Requests
----------------

`AttemptRequest.Runtimes` is a list of strings that are runtimes this
worker is capable of handling.  Work specs with runtimes that do not
exactly match one of these strings are ignored.  If the runtime list
is empty, any runtime is considered acceptable.

A new Go-based worker could call

```go
attempts := worker.RequestAttempts(coordinate.AttemptRequest{
        NumberOfWorkUnits: 20,
        Runtimes: []string{"go"},
})
```

The Python-compatible interface passes a runtime list containing of a
single empty string.

Example
-------

For a mixed Python/Go system, create a YAML file containing:

```yaml
flows:
  a_python_spec:
    module: python.module
    run_function: coordinate_run

  a_go_spec:
    runtime: go
    task: task_name
```

Using the `coordinate` tool from the Python coordinate package, run
`coordinate flow flow.yaml`, pointing at the file above.

The Python-based `coordinate_worker` will only retrieve and run work
units from `a_python_spec`.  A new Go-based worker, using the
`RequestAttempts` call shown above, will only retrieve and run work
units from `a_go_spec`.
