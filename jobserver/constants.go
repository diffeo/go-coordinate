// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package jobserver

// WorkUnitStatus is one of the possible work unit states supported by the
// Python Coordinate server.
type WorkUnitStatus int

const (
	// Available work units can be returned by the get_work call.
	Available WorkUnitStatus = 1

	// Blocked work units cannot run until some other work units
	// complete.  This was supported in rejester but not in Python
	// Coordinate.
	// Blocked WorkUnitStatus = 2

	// Pending work units have been returned by the get_work call,
	// and have not yet been completed.
	Pending WorkUnitStatus = 3

	// Finished work units have completed successfully.
	Finished WorkUnitStatus = 4

	// Failed work units have completed unsuccessfully.
	Failed WorkUnitStatus = 5
)
