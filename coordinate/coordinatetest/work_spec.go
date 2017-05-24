// Copyright 2015-2017 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"time"
)

// TestChangeSpecData tests WorkSpec.SetData().
func (s *Suite) TestChangeSpecData() {
	var (
		err  error
		data map[string]interface{}
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestTwoWorkSpecsBasic",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	err = sts.WorkSpec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 2,
		"foo":    "bar",
	})
	s.NoError(err)

	data, err = sts.WorkSpec.Data()
	if s.NoError(err) {
		s.Equal("spec", data["name"])
		s.EqualValues(2, data["min_gb"])
		s.Equal("bar", data["foo"])
	}

	err = sts.WorkSpec.SetData(map[string]interface{}{})
	s.Exactly(coordinate.ErrNoWorkSpecName, err)

	err = sts.WorkSpec.SetData(map[string]interface{}{
		"name":   "name",
		"min_gb": 3,
	})
	s.Exactly(coordinate.ErrChangedName, err)

	data, err = sts.WorkSpec.Data()
	if s.NoError(err) {
		s.Equal("spec", data["name"])
		s.EqualValues(2, data["min_gb"])
		s.Equal("bar", data["foo"])
	}
}

// TestDataEmptyList verifies that an empty list gets preserved in the
// work spec data, and not remapped to nil.
func (s *Suite) TestDataEmptyList() {
	emptyList := []string{}
	s.NotNil(emptyList)
	s.Len(emptyList, 0)

	sts := SimpleTestSetup{
		NamespaceName: "TestDataEmptyList",
		WorkSpecData: map[string]interface{}{
			"name": "spec",
			"config": map[string]interface{}{
				"empty_list": emptyList,
			},
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	data, err := sts.WorkSpec.Data()
	if s.NoError(err) && s.NotNil(data) {
		if s.Contains(data, "config") {
			var found interface{}
			switch config := data["config"].(type) {
			case map[string]interface{}:
				found = config["empty_list"]
			case map[interface{}]interface{}:
				found = config["empty_list"]
			default:
				s.FailNow("strange type in work spec config")
			}
			s.NotNil(found)
			s.Len(found, 0)
		}
	}
}

// TestDefaultMeta tests that WorkSpec.Meta gets the correct defaults,
// which in a couple of cases are not zero values.
func (s *Suite) TestDefaultMeta() {
	sts := SimpleTestSetup{
		NamespaceName: "TestDefaultMeta",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	meta, err := sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		s.Equal(0, meta.Priority)
		s.Equal(20, meta.Weight)
		s.False(meta.Paused)
		s.False(meta.Continuous)
		s.False(meta.CanBeContinuous)
		s.Zero(meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(0, meta.MaxRunning)
		s.Equal(0, meta.MaxAttemptsReturned)
		s.Equal(0, meta.MaxRetries)
		s.Equal("", meta.NextWorkSpecName)
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		s.Equal("", meta.Runtime)
	}
}

// TestPrefilledMeta tests that WorkSpec.Meta() fills in correctly from
// "magic" keys in a work spec.
func (s *Suite) TestPrefilledMeta() {
	sts := SimpleTestSetup{
		NamespaceName: "TestPrefilledMeta",
		WorkSpecData: map[string]interface{}{
			"name":        "spec",
			"min_gb":      1,
			"priority":    10,
			"weight":      100,
			"disabled":    true,
			"continuous":  true,
			"interval":    60,
			"max_running": 10,
			"max_getwork": 1,
			"max_retries": 20,
			"then":        "spec2",
			"runtime":     "go",
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	meta, err := sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		s.Equal(10, meta.Priority)
		s.Equal(100, meta.Weight)
		s.True(meta.Paused)
		s.True(meta.Continuous)
		s.True(meta.CanBeContinuous)
		s.Equal(60*time.Second, meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(10, meta.MaxRunning)
		s.Equal(1, meta.MaxAttemptsReturned)
		s.Equal(20, meta.MaxRetries)
		s.Equal("spec2", meta.NextWorkSpecName)
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		s.Equal("go", meta.Runtime)
	}
}

// TestSetDataSetsMeta tests that...yeah
func (s *Suite) TestSetDataSetsMeta() {
	sts := SimpleTestSetup{
		NamespaceName: "TestSetDataSetsMeta",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	meta, err := sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		s.Equal(0, meta.Priority)
		s.Equal(20, meta.Weight)
		s.False(meta.Paused)
		s.False(meta.Continuous)
		s.False(meta.CanBeContinuous)
		s.Zero(meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(0, meta.MaxRunning)
		s.Equal(0, meta.MaxAttemptsReturned)
		s.Equal(0, meta.MaxRetries)
		s.Equal("", meta.NextWorkSpecName)
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		s.Equal("", meta.Runtime)
	}

	err = sts.WorkSpec.SetData(map[string]interface{}{
		"name":        "spec",
		"min_gb":      1,
		"priority":    10,
		"weight":      100,
		"disabled":    true,
		"continuous":  true,
		"interval":    60,
		"max_running": 10,
		"max_getwork": 1,
		"max_retries": 20,
		"then":        "spec2",
		"runtime":     "go",
	})
	s.NoError(err)

	meta, err = sts.WorkSpec.Meta(false)
	if s.NoError(err) {
		s.Equal(10, meta.Priority)
		s.Equal(100, meta.Weight)
		s.True(meta.Paused)
		s.True(meta.Continuous)
		s.True(meta.CanBeContinuous)
		s.Equal(60*time.Second, meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(10, meta.MaxRunning)
		s.Equal(1, meta.MaxAttemptsReturned)
		s.Equal(20, meta.MaxRetries)
		s.Equal("spec2", meta.NextWorkSpecName)
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		s.Equal("go", meta.Runtime)
	}
}

// TestNiceWeight tests the "weight = 20-nice" rule.
func (s *Suite) TestNiceWeight() {
	namespace, err := s.Coordinate.Namespace("TestNiceWeight")
	if !s.NoError(err) {
		return
	}
	defer namespace.Destroy()

	spec, err := namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   5,
	})
	if !s.NoError(err) {
		return
	}

	meta, err := spec.Meta(false)
	if s.NoError(err) {
		s.Equal(15, meta.Weight)
	}

	// Lower bound on weight
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   100,
	})
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(1, meta.Weight)
	}

	// No lower bound on niceness
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   -80,
	})
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(100, meta.Weight)
	}

	// Weight trumps nice
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"weight": 50,
		"nice":   5,
	})
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(50, meta.Weight)
	}

	// Removing either flag resets to default
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(20, meta.Weight)
	}
}

// TestSetMeta tests the basic SetMeta() call and a couple of its
// documented oddities.
func (s *Suite) TestSetMeta() {
	var (
		err       error
		namespace coordinate.Namespace
		spec      coordinate.WorkSpec
		meta      coordinate.WorkSpecMeta
	)

	namespace, err = s.Coordinate.Namespace("TestSetMeta")
	if !s.NoError(err) {
		return
	}
	defer namespace.Destroy()

	spec, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":       "spec",
		"min_gb":     1,
		"continuous": true,
	})
	if !s.NoError(err) {
		return
	}

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(0, meta.Priority)
		s.Equal(20, meta.Weight)
		s.False(meta.Paused)
		s.True(meta.Continuous)
		s.True(meta.CanBeContinuous)
		s.Zero(meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(0, meta.MaxRunning)
		s.Equal(0, meta.MaxAttemptsReturned)
		s.Equal(0, meta.MaxRetries)
		s.Equal("", meta.NextWorkSpecName)
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		s.Equal("", meta.Runtime)
	}

	err = spec.SetMeta(coordinate.WorkSpecMeta{
		Priority:            10,
		Weight:              100,
		Paused:              true,
		Continuous:          false,
		CanBeContinuous:     false,
		Interval:            time.Duration(60) * time.Second,
		MaxRunning:          10,
		MaxAttemptsReturned: 1,
		MaxRetries:          20,
		NextWorkSpecName:    "then",
		AvailableCount:      100,
		PendingCount:        50,
		Runtime:             "go",
	})
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.Equal(10, meta.Priority)
		s.Equal(100, meta.Weight)
		s.True(meta.Paused)
		s.False(meta.Continuous)
		// Cannot clear "can be continuous" flag
		s.True(meta.CanBeContinuous)
		s.Equal(60*time.Second, meta.Interval)
		s.WithinDuration(time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		s.Equal(10, meta.MaxRunning)
		s.Equal(1, meta.MaxAttemptsReturned)
		s.Equal(20, meta.MaxRetries)
		// Cannot change following work spec
		s.Equal("", meta.NextWorkSpecName)
		// Cannot set the counts
		s.Equal(0, meta.AvailableCount)
		s.Equal(0, meta.PendingCount)
		// Cannot change the language runtime
		s.Equal("", meta.Runtime)
	}
}

// TestMetaContinuous specifically checks that you cannot enable the
// "continuous" flag on non-continuous work specs.
func (s *Suite) TestMetaContinuous() {
	var (
		err       error
		namespace coordinate.Namespace
		spec      coordinate.WorkSpec
		meta      coordinate.WorkSpecMeta
	)

	namespace, err = s.Coordinate.Namespace("TestMetaContinuous")
	if !s.NoError(err) {
		return
	}
	defer namespace.Destroy()

	spec, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	if !s.NoError(err) {
		return
	}

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		s.False(meta.Continuous)
		s.False(meta.CanBeContinuous)
	}

	meta.Continuous = true
	err = spec.SetMeta(meta)
	s.NoError(err)

	meta, err = spec.Meta(false)
	if s.NoError(err) {
		// Cannot set the "continuous" flag
		s.False(meta.Continuous)
		s.False(meta.CanBeContinuous)
	}
}

// TestMetaCounts does basic tests on the "available" and "pending" counts.
func (s *Suite) TestMetaCounts() {
	sts := SimpleTestSetup{
		NamespaceName: "TestMetaCounts",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	checkCounts := func(available, pending int) {
		meta, err := sts.WorkSpec.Meta(true)
		if s.NoError(err) {
			s.Equal(available, meta.AvailableCount)
			s.Equal(pending, meta.PendingCount)
		}
	}
	checkCounts(0, 0)

	// Adding a work unit adds to the available count
	_, err := sts.AddWorkUnit("one")
	if s.NoError(err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	s.Clock.Add(5 * time.Second)
	attempt := sts.RequestOneAttempt(s)
	checkCounts(0, 1)

	// Expiring an attempt makes it available again
	err = attempt.Expire(nil)
	if s.NoError(err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	s.Clock.Add(5 * time.Second)
	attempt = sts.RequestOneAttempt(s)
	checkCounts(0, 1)

	// Marking an attempt retryable makes it pending again
	err = attempt.Retry(nil, time.Duration(0))
	if s.NoError(err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	s.Clock.Add(5 * time.Second)
	attempt = sts.RequestOneAttempt(s)
	checkCounts(0, 1)

	// Finishing an attempt takes it out of the list entirely
	err = attempt.Finish(nil)
	if s.NoError(err) {
		checkCounts(0, 0)
	}
}

// TestSpecDeletedGone validates that, if you delete a work spec,
// subsequent attempts to use it return ErrGone.
func (s *Suite) TestSpecDeletedGone() {
	sts := SimpleTestSetup{
		NamespaceName: "TestSpecDeletedGone",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	err := sts.Namespace.DestroyWorkSpec(sts.WorkSpecName)
	s.NoError(err)

	// Test a couple of basic things
	_, err = sts.WorkSpec.Meta(false)
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("unexpected error reading deleted work spec meta",
			"+v", err)
	}

	_, err = sts.WorkSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("unexpected error adding work to deleted work spec",
			"+v", err)
	}
}

// TestSpecInNamespaceGone validates that, if you delete a work spec's
// namespace, attempts to use the work spec return ErrGone.
func (s *Suite) TestSpecInNamespaceGone() {
	sts := SimpleTestSetup{
		NamespaceName: "TestSpecInNamespaceGone",
		WorkSpecName:  "spec",
	}
	sts.SetUp(s)
	// We are about to blow up the namespace now so there is no cleanup

	err := sts.Namespace.Destroy()
	s.NoError(err)

	// Test a couple of basic things
	_, err = sts.WorkSpec.Meta(false)
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("unexpected error reading deleted work spec meta",
			"+v", err)
	}

	_, err = sts.WorkSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		s.Equal(sts.WorkSpecName, nsws.Name)
	} else {
		s.Fail("unexpected error adding work to deleted work spec",
			"+v", err)
	}
}

// TestOneDayInterval tests a continuous work spec with a 1-day
// interval.  This is a regression test for a specific parsing issue
// in the PostgreSQL backend.
func (s *Suite) TestOneDayInterval() {
	sts := SimpleTestSetup{
		NamespaceName: "TestOneDayInterval",
		WorkerName:    "worker",
		WorkSpecData: map[string]interface{}{
			"name":       "spec",
			"min_gb":     1,
			"continuous": true,
			"interval":   86400,
		},
	}
	sts.SetUp(s)
	defer sts.TearDown(s)

	sts.RequestOneAttempt(s)
}
