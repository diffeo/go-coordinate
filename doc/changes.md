Major Changes
=============

0.4.12 (7 Sep 2018)
-------------------

* Added `.NumAttempts()` method as a faster way to do `len(w.Attempts())`

0.4.10 (2 Jan 2018)
-------------------

* Fix CircleCI build.
  ([#18](https://github.com/diffeo/go-coordinate/pull/18))

0.4.9 (2 Jan 2018)
------------------

* Switch to CircleCI 2.0.
  ([#17](https://github.com/diffeo/go-coordinate/pull/17))
* Switch to dep dependency management tool.
  ([#17](https://github.com/diffeo/go-coordinate/pull/17))

0.4.8 (21 Dec 2017)
-------------------

* Fix a data race.
  ([#16](https://github.com/diffeo/go-coordinate/pull/16))

0.4.7 (29 Aug 2017)
-------------------

* Switch docker image repository to Docker Hub.
  ([#15](https://github.com/diffeo/go-coordinate/pull/15))

0.4.6 (7 July 2017)
-------------------

* Bug fix for panic on nil logger.
  ([#14](https://github.com/diffeo/go-coordinate/pull/14))

0.4.5 (2 Jun 2017)
------------------

* Performance fix for the PostgreSQL backend when `max_retries` is used.
  ([#12](https://github.com/diffeo/go-coordinate/pulls/12))
* If `max_retries` causes some work units to get preemptively failed,
  `RequestAttempts` will return fewer than the `max_getwork` number of
  attempts, rather than attempting to get more work, on all backends.

0.4.4 (30 May 2017)
-------------------

* In the published Prometheus metrics, don't double-double-quote the
  "status" value.
  ([#11](https://github.com/diffeo/go-coordinate/pulls/11))

0.4.3 (30 May 2017)
-------------------

* Add a `max_retries` work spec parameter to limit work unit retries
  ([#8](https://github.com/diffeo/go-coordinate/pulls/8))
* Port the generic test suite to use
  `github.com/stretchr/testify/suite`, rather than the home-grown
  `cptest` program in this package.
  ([#9](https://github.com/diffeo/go-coordinate/pulls/9))
* Publish `coordinate summary` style metrics
  to [Promotheus](https://prometheus.io/).
  ([#10](https://github.com/diffeo/go-coordinate/pulls/10))

0.4.2 (4 May 2017)
------------------

* Requires Go 1.7 or later, and its standard `context` package
  ([#4](https://github.com/diffeo/go-coordinate/pulls/4))
* Add CircleCI integration, Quay reference Docker image
  ([#1](https://github.com/diffeo/go-coordinate/pulls/1),
  [#2](https://github.com/diffeo/pulls/2))
* PostgreSQL backend performance improvement for long-running systems
  with many old workers
  ([#3](https://github.com/diffeo/go-coordinate/pulls/3))
* The Go worker framework can take a list of runtime strings as
  parameters, it is not limited to `"go"`
  ([#5](https://github.com/diffeo/go-coordinate/pulls/5))

0.4.1 (15 Aug 2016)
-------------------

* Bug fix in Go decoding of CBOR-RPC responses.
* Bug fix adding work units in the PostgreSQL backend,
  where adding a work unit that already existed would affect all work
  specs with performance implications

0.4.0 (29 Jun 2016)
-------------------

* Bug fix relating to multiple concurrent Go workers and concurrent
  map access
* Bug fixes for continuous work units on the PostgreSQL backend,
  including fixes for intervals longer than 3600 seconds, intervals
  longer than 86400 seconds, and concurrent attempts to execute
  continuous work units
* Don't ignore a requested work spec name from Python
  `coordinate run_one --from-work-spec`

The `github.com/satori/go.uuid` library made its UUID parsing stricter
in this timeframe as well, so UUID values passed in work unit data or
elsewhere may be rejected if key bits do not conform to RFC 4122.

0.3.0 (8 Apr 2016)
------------------

* Adds the ability for work units to declare an earliest time they can
  run.  Changes `WorkSpec.AddWorkUnit()` to take a new `WorkUnitMeta`
  structure instead of the existing `priority` parameter, and adds a
  `time.Duration` parameter to `Attempt.Retry()`.  Generated `output`
  work units in chained work units can also declare a `delay` before
  running the next unit.  Makes corresponding (incompatible) changes
  to the REST API.
* Adds a worker framework so that Coordinate work specs can run task
  implementations written in Go.
* Add a `coordbench` tool to measure the performance of the Coordinate
  system.
* Several performance improvements for the PostgreSQL backend, aimed
  at the "millions of work units" scale.
* Switch tests from `gocheck` to `testify`.

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
