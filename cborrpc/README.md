Diffeo CBOR-RPC wire format
===========================

The Diffeo Coordinate daemon uses a custom wire protocol based on the
[CBOR](http://cbor.io/) data encoding.  CBOR's data model is very
similar to JSON's, but with a couple of extensions, including separate
"text" and "byte string" types and tag annotations.

In all cases the Python Coordinate client code sends and receives
strings as UTF-8-encoded byte strings except as noted.  (This may be
an artifact of running it on Python 2, where the default string type
is ASCII-encoded byte string.)  It will accept Unicode strings over
the wire but will always send back byte strings.

Request and response messages in all cases are CBOR-encoded, and the
result sent as a CBOR-in-CBOR byte string with tag 24.  The system uses
the following tags:

* 24 (standard): tags a byte string as holding encoded CBOR
* 37 (standard): tags a length-16 byte string as holding a UUID
* 128 (non-standard): tags a list as holding a Python sequence

The client and server communicate over a persistent connection.
Requests include a correlation ID.  The client may "pipeline" requests
by sending further requests before receiving responses.  Only the
client expects to close a connection in the Python code, and it
typically will only do so after all outstanding requests have gotten
responses.

Requests are mappings where the keys are ASCII byte strings.  These
have keys:

* `id`: correlation ID, typically sequential per connection
* `method`: ASCII byte string, name of RPC method to invoke
* `params`: list of anything, parameters to the method

Responses are also mappings where the keys are ASCII byte strings.
These have keys:

* `id`: correlation ID, matches the ID of the request
* `response`: on success, any object, response to the method
* `error`: on failure, a mapping with a single key `message` holding the
  error message

Examples
--------

Consider the RPC call `list_work_specs({})`.  This would be encoded as:

    {
        "id": 1,
        "method": "list_work_specs",
        "params": [{}]
    }

and sent over the wire as:

    D8 18                 Tag 24, CBOR-encoded string follows
    58 25                 Byte string of length 37
    A3                    Map of three pairs
    42 69 64              Byte string "id"
    01                    Positive integer 1
    46 6D 65 74 68 6F 64  Byte string "method"
    4F 6C 69 73 74 5F 77 6F 72 6B 5F 73 70 65 63 73
                          Byte string "list_work_specs"
    46 70 61 72 61 6D 73  Byte string "params"
    81                    Array of length 1
    A0                    Map of 0 pairs
