Python Coordinate
================

Coordinate's previous life was as a
[pure Python package](https://github.com/diffeo/coordinate).  This
package aims to maintain wire compatibility with the Python
`coordinate` client class; it cannot establish outbound connections to
the Python `coordinated` daemon.

Configuration
-------------

Run the `coordinated` binary from this package.  In your Python YAML
configuration, set a pointer to this server:

```yaml
coordinate:
  addresses: ['localhost:5932']
```

If the daemon is running on a different host, or you specified a
different port using the `-cborrpc` command-line option, change this
setting accordingly.

If your application depends on getting a system-global configuration
back from coordinated, start the Go daemon with a `-config`
command-line option pointing at a YAML file.  This file will be passed
back without interpretation beyond parsing to clients that request it.

Running Python tests
--------------------

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
all use this fixture and will run against the Go `coordinated` server.
Many of these tests have been extracted into Go tests in the
"jobserver" package.

Behavioral differences
----------------------

### Scheduling ###

The work spec scheduler is much simpler than in the Python
coordinated.  See the "Scheduling" section in the
[extended work spec discussion](work_specs.md) for details.

The Go scheduler only considers work specs' `priority` and `weight`
fields.  Work specs' successors, as identified by the `then` field, do
not factor into the scheduler, and the `then_preempts` field is
ignored.  The Python scheduler would try to give successor work specs
priority over predecessors (unless `then_preempts: false` was in the
work spec data), and would deterministically pick a work unit based on
weights.  This could cause low-weight work specs to never get work if
there were relatively fewer workers.

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

Enhancements
------------

Python programs that are aware they are talking to this implementation
of the Coordinate daemon can take advantage of some enhancements in
it.

### Delayed work units ###

When a work unit is created, calling code can request that it not
execute for some length of time:

```python
task_master.add_work_units('spec', [
  ('unit', {'key': 'value'}, {'delay': 90})
])
```

If `add_work_units()` is passed a list of tuples, each tuple contains
the work unit name, data dictionary, and (optionally) a metadata
dictionary.  Native Python coordinated already supports a key
`priority` to set the work unit priority at creation time; Go
coordinated adds the `delay` key giving an initial delay in seconds.

Delays for work units created using the `output` key for
[chained work specs](chaining.md) also work as described.

In both cases, running this code against Python coordinated will
ignore the `delay` key, and the added work unit(s) will run
immediately.

Other notes
-----------

Most Python Coordinate applications should run successfully against
this server.  This server has been tested against both the Python
Coordinate provided unit tests and some real-world data.

Work spec names must be valid Unicode strings.  Work spec definitions
and work unit data must be Unicode-string-keyed maps.  Work unit keys,
however, can be arbitrary byte strings.  Python (especially Python 2)
is sloppy about byte vs. character strings and it is easy to inject
the wrong type; if you do create a work spec with a non-UTF-8 byte
string name, the server will eventually return it as an invalid
Unicode-tagged string.  Data such as work spec names can be submitted
as byte strings but may be returned as Unicode strings.
