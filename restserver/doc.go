// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package restserver publishes a Coordinate interface as a REST service.
// The restclient package is a matching client.
//
// The complete REST API is defined in the restdata package.  In
// particular, note that the URLs described here are not actually part
// of the API.
//
// HTTP Considerations
//
// HTTP GET requests will default to returning an HTML representation of
// the resource.  Clients should use the standard HTTP Accept: header to
// request a different format.  See "MIME Types" below.
//
// This interface does not (currently) support HTTP caching or
// authentication headers.
//
// Code will generally follow conventions for the Github API as an
// established example; see https://developer.github.com/v3/ for
// details.
//
// MIME Types
//
// This interface understands MIME types as follows:
//
//     application/vnd.diffeo.coordinate.v1+json
//
// JSON representation of version 1 of this interface.
//
//     application/vnd.diffeo.coordinate+json
//     application/json
//     text/json
//
// JSON representation of latest version of this interface.
//
// URL Scheme
//
// In most cases, Coordinate objects follow their natural hierarchy
// and are addressed by name.  For instance, the work spec "bar" in
// namespace "foo" would have a resource URL of
// /namespace/foo/work_spec/bar.  If the name is not URL-safe
// printable ASCII, it must be base64 encoded using the URL-safe
// alphabet (RFC 4648 section 5), with no padding, and adding an
// additional - at the front of the name:
// /namespace/-Zm9v/work_spec/-YmFy is the same resource as the
// preceding one.  Correspondingly, a single - means "empty", and a
// name that begins with - must be URL-encoded.  The work spec "-" in
// the empty namespace has a URL of /namespace/-/work_spec/-LQ.
//
// The following URLs are defined:
//
//     /
//     /namespace
//     /namespace/{namespace}
//     /namespace/{namespace}/work_spec
//     /namespace/{namespace}/work_spec/{spec}
//     /namespace/{namespace}/work_spec/{spec}/counts
//     /namespace/{namespace}/work_spec/{spec}/change
//     /namespace/{namespace}/work_spec/{spec}/adjust
//     /namespace/{namespace}/work_spec/{spec}/meta
//     /namespace/{namespace}/work_spec/{spec}/work_unit
//     /namespace/{namespace}/work_spec/{spec}/work_unit/{unit}
//       .../attempts
//       .../attempt/{worker}/{start_time}
//       .../attempt/{worker}/{start_time}/renew
//       .../attempt/{worker}/{start_time}/expire
//       .../attempt/{worker}/{start_time}/finish
//       .../attempt/{worker}/{start_time}/fail
//       .../attempt/{worker}/{start_time}/retry
//     /namespace/{namespace}/worker
//     /namespace/{namespace}/worker/{worker}
//     /namespace/{namespace}/worker/{worker}/request_attempts
//     /namespace/{namespace}/worker/{worker}/make_attempt
//     /namespace/{namespace}/worker/{worker}/active_attempts
//     /namespace/{namespace}/worker/{worker}/all_attempts
//     /namespace/{namespace}/worker/{worker}/child_attempts
package restserver
