// Statistics for Coordinate objects.
//
// Copyright 2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"sort"
)

// SummaryRecord is a single piece of summary data, recording how
// many work units were in some status in some work spec.
type SummaryRecord struct {
	Namespace string
	WorkSpec  string
	Status    WorkUnitStatus
	Count     int
}

// Summary is a summary of work unit statuses for some part of
// the Coordinate system.  The records are in no particular order.
// The records should not contain records with zero count.
type Summary []SummaryRecord

// Sort sorts the records of a summary in place.
func (s Summary) Sort() {
	less := func(i, j int) bool {
		if s[i].Namespace < s[j].Namespace {
			return true
		}
		if s[i].Namespace > s[j].Namespace {
			return false
		}
		if s[i].WorkSpec < s[j].WorkSpec {
			return true
		}
		if s[i].WorkSpec > s[j].WorkSpec {
			return false
		}
		return s[i].Status < s[j].Status
	}
	sort.Slice(s, less)
}

// Summarizable describes Coordinate objects that can be summarized.
// The summary is not required to have exact counts of work units;
// counts may be rounded, delayed, not account for recently-expired
// work units, and so on.
type Summarizable interface {
	Summarize() (Summary, error)
}
