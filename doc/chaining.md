Work Unit Chaining
==================

One work spec can indicate that it generates work units for another
work spec, using the `then` key in its work spec data.  This maps to
the `NextWorkSpecName` field in the `coordinate.WorkSpecMeta`
structure.

```json
{
    "name": "one",
    "then": "two"
}
```

```json
{
    "name": "two"
}
```

When work units for the first work spec complete successfully, the
engine looks for a special key `output` in the final effective work
unit data, taking into account any changes made in the course of the
active attempt.  `output` may be:

* An object, mapping work unit name to work unit data
* A list of strings, which are work unit names with empty data
* A list of lists, where each list contains at least two items; items
  are, in order, the work unit name, the work unit data, a work unit
  metadata object, and a work unit priority value

If the Python CBOR-based interface is used, any of these lists can be
tuples, and the work unit names may be either byte strings or
character strings.

In the list-of-lists form, the metadata object may contain keys:

* `priority`: specifies the priority of the created work unit; if a
  fourth parameter is included in the list and is not `null`, that
  priority parameter takes precedence over this setting
* `delay`: specifies a minimum time to wait before executing the
  created work unit, in seconds
  
Other keys are ignored.

Some examples:

```json
{
    "output": {
        "unit": {"key": "value"}
    }
}
```

```json
{
    "output": [
        "one", "two", "three"
    ]
}
```

```json
{
    "output": [
        ["first", {}, {"priority": 20}],
        ["second", {}, {}, 10],
        ["delayed", {}, {"delay": 90}],
        "third"
    ]
}
```

The last example creates four work units.  They will execute in order
"first" (priority 20), "second" (priority 10), and then "third"
(priority 0); "delayed" will not execute before 90 seconds have
passed, but if "third" has not executed by then, "delayed" will
probably execute before it (both have the same priority of 0 and
"delayed" is alphabetically first).
