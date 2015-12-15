Coordinate REST Client
======================

This package defines a REST client for the Coordinate system.  It matches
the REST server in the [../restserver](restserver) package.

Usage
-----

Run a [../cmd/coordinated](coordinated) process with an HTTP server, e.g.

```sh
go install github.com/diffeo/go-coordinate/cmd/coordinated
coordinated -backend memory -http :5980
```

Then run your client code pointing at that service.  To run a
coordinated proxy, for instance,

```sh
coordinated -backend http://localhost:5980/ -http :5981
```

Limitations
-----------

In most cases names of objects are embedded directly into URLs.  If
they are not printable, the URL scheme can pass base64-encoded keys as
well; see the [../restserver/doc.go](REST API documentation) for
details.

There is no single unique identifier for attempts.  The URL scheme
assumes that at most one attempt will be created for a specific work
unit, for a specific worker, in a single millisecond.  This is not a
hard guarantee but it would be unusual for it to break: it either
implies duplicate attempts are being issued, or attempts are being
forced for specific work units, or an attempt is requested, marked
retryable, and requested again all within 1 ms.
