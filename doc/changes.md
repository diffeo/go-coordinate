Major Changes
=============

0.3.0 (unreleased)
------------------

* Adds the ability for work units to declare an earliest time they can
  run.  Changes `WorkSpec.AddWorkUnit()` to take a new `WorkUnitMeta`
  structure instead of the existing `priority` parameter, and adds a
  `time.Duration` parameter to `Attempt.Retry()`.  Generated `output`
  work units in chained work units can also declare a `delay` before
  running the next unit.  Makes corresponding (incompatible) changes
  to the REST API.

0.2.0 (4 Jan 2016)
------------------

* Adds a REST server interface: start `coordinated -http :5980`.
* Adds a REST client backend: start `coordinated -backend
  http://localhost:5980/`.
* Adds a `runtime` key to work specs to allow workers for multiple
  language runtimes to coexist.
* Turns off detailed logging of every CBOR-RPC request and response.

0.1.2 (1 Dec 2015)
------------------

* Build changes to support getting a working static Docker container out.

0.1.1 (20 Nov 2015)
-------------------

* Work around a code change in the
  [codec](https://github.com/ugorji/go/codec) library.

0.1.0 (18 Nov 2015)
-------------------

* Initial release.
