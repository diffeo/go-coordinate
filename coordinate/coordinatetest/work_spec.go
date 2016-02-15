// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinatetest

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestChangeSpecData tests WorkSpec.SetData().
func TestChangeSpecData(t *testing.T) {
	var (
		err  error
		data map[string]interface{}
	)

	sts := SimpleTestSetup{
		NamespaceName: "TestTwoWorkSpecsBasic",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	err = sts.WorkSpec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 2,
		"foo":    "bar",
	})
	assert.NoError(t, err)

	data, err = sts.WorkSpec.Data()
	if assert.NoError(t, err) {
		assert.Equal(t, "spec", data["name"])
		assert.EqualValues(t, 2, data["min_gb"])
		assert.Equal(t, "bar", data["foo"])
	}

	err = sts.WorkSpec.SetData(map[string]interface{}{})
	assert.Exactly(t, coordinate.ErrNoWorkSpecName, err)

	err = sts.WorkSpec.SetData(map[string]interface{}{
		"name":   "name",
		"min_gb": 3,
	})
	assert.Exactly(t, coordinate.ErrChangedName, err)

	data, err = sts.WorkSpec.Data()
	if assert.NoError(t, err) {
		assert.Equal(t, "spec", data["name"])
		assert.EqualValues(t, 2, data["min_gb"])
		assert.Equal(t, "bar", data["foo"])
	}
}

// TestDataEmptyList verifies that an empty list gets preserved in the
// work spec data, and not remapped to nil.
func TestDataEmptyList(t *testing.T) {
	emptyList := []string{}
	assert.NotNil(t, emptyList)
	assert.Len(t, emptyList, 0)

	sts := SimpleTestSetup{
		NamespaceName: "TestDataEmptyList",
		WorkSpecData: map[string]interface{}{
			"name": "spec",
			"config": map[string]interface{}{
				"empty_list": emptyList,
			},
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	data, err := sts.WorkSpec.Data()
	if assert.NoError(t, err) && assert.NotNil(t, data) {
		if assert.Contains(t, data, "config") {
			var found interface{}
			switch config := data["config"].(type) {
			case map[string]interface{}:
				found = config["empty_list"]
			case map[interface{}]interface{}:
				found = config["empty_list"]
			default:
				t.FailNow()
			}
			assert.NotNil(t, found)
			assert.Len(t, found, 0)
		}
	}
}

// TestDefaultMeta tests that WorkSpec.Meta gets the correct defaults,
// which in a couple of cases are not zero values.
func TestDefaultMeta(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestDefaultMeta",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	meta, err := sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 0, meta.Priority)
		assert.Equal(t, 20, meta.Weight)
		assert.False(t, meta.Paused)
		assert.False(t, meta.Continuous)
		assert.False(t, meta.CanBeContinuous)
		assert.Zero(t, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 0, meta.MaxRunning)
		assert.Equal(t, 0, meta.MaxAttemptsReturned)
		assert.Equal(t, "", meta.NextWorkSpecName)
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		assert.Equal(t, "", meta.Runtime)
	}
}

// TestPrefilledMeta tests that WorkSpec.Meta() fills in correctly from
// "magic" keys in a work spec.
func TestPrefilledMeta(t *testing.T) {
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
			"then":        "spec2",
			"runtime":     "go",
		},
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	meta, err := sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 10, meta.Priority)
		assert.Equal(t, 100, meta.Weight)
		assert.True(t, meta.Paused)
		assert.True(t, meta.Continuous)
		assert.True(t, meta.CanBeContinuous)
		assert.Equal(t, 60*time.Second, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 10, meta.MaxRunning)
		assert.Equal(t, 1, meta.MaxAttemptsReturned)
		assert.Equal(t, "spec2", meta.NextWorkSpecName)
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		assert.Equal(t, "go", meta.Runtime)
	}
}

// TestSetDataSetsMeta tests that...yeah
func TestSetDataSetsMeta(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestSetDataSetsMeta",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	meta, err := sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 0, meta.Priority)
		assert.Equal(t, 20, meta.Weight)
		assert.False(t, meta.Paused)
		assert.False(t, meta.Continuous)
		assert.False(t, meta.CanBeContinuous)
		assert.Zero(t, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 0, meta.MaxRunning)
		assert.Equal(t, 0, meta.MaxAttemptsReturned)
		assert.Equal(t, "", meta.NextWorkSpecName)
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		assert.Equal(t, "", meta.Runtime)
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
		"then":        "spec2",
		"runtime":     "go",
	})
	assert.NoError(t, err)

	meta, err = sts.WorkSpec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 10, meta.Priority)
		assert.Equal(t, 100, meta.Weight)
		assert.True(t, meta.Paused)
		assert.True(t, meta.Continuous)
		assert.True(t, meta.CanBeContinuous)
		assert.Equal(t, 60*time.Second, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 10, meta.MaxRunning)
		assert.Equal(t, 1, meta.MaxAttemptsReturned)
		assert.Equal(t, "spec2", meta.NextWorkSpecName)
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		assert.Equal(t, "go", meta.Runtime)
	}
}

// TestNiceWeight tests the "weight = 20-nice" rule.
func TestNiceWeight(t *testing.T) {
	namespace, err := Coordinate.Namespace("TestNiceWeight")
	if !assert.NoError(t, err) {
		return
	}
	defer namespace.Destroy()

	spec, err := namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   5,
	})
	if !assert.NoError(t, err) {
		return
	}

	meta, err := spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 15, meta.Weight)
	}

	// Lower bound on weight
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   100,
	})
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, meta.Weight)
	}

	// No lower bound on niceness
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"nice":   -80,
	})
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 100, meta.Weight)
	}

	// Weight trumps nice
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
		"weight": 50,
		"nice":   5,
	})
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 50, meta.Weight)
	}

	// Removing either flag resets to default
	err = spec.SetData(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 20, meta.Weight)
	}
}

// TestSetMeta tests the basic SetMeta() call and a couple of its
// documented oddities.
func TestSetMeta(t *testing.T) {
	var (
		err       error
		namespace coordinate.Namespace
		spec      coordinate.WorkSpec
		meta      coordinate.WorkSpecMeta
	)

	namespace, err = Coordinate.Namespace("TestSetMeta")
	if !assert.NoError(t, err) {
		return
	}
	defer namespace.Destroy()

	spec, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":       "spec",
		"min_gb":     1,
		"continuous": true,
	})
	if !assert.NoError(t, err) {
		return
	}

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 0, meta.Priority)
		assert.Equal(t, 20, meta.Weight)
		assert.False(t, meta.Paused)
		assert.True(t, meta.Continuous)
		assert.True(t, meta.CanBeContinuous)
		assert.Zero(t, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 0, meta.MaxRunning)
		assert.Equal(t, 0, meta.MaxAttemptsReturned)
		assert.Equal(t, "", meta.NextWorkSpecName)
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		assert.Equal(t, "", meta.Runtime)
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
		NextWorkSpecName:    "then",
		AvailableCount:      100,
		PendingCount:        50,
		Runtime:             "go",
	})
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.Equal(t, 10, meta.Priority)
		assert.Equal(t, 100, meta.Weight)
		assert.True(t, meta.Paused)
		assert.False(t, meta.Continuous)
		// Cannot clear "can be continuous" flag
		assert.True(t, meta.CanBeContinuous)
		assert.Equal(t, 60*time.Second, meta.Interval)
		assert.WithinDuration(t, time.Time{}, meta.NextContinuous, 1*time.Microsecond)
		assert.Equal(t, 10, meta.MaxRunning)
		assert.Equal(t, 1, meta.MaxAttemptsReturned)
		// Cannot change following work spec
		assert.Equal(t, "", meta.NextWorkSpecName)
		// Cannot set the counts
		assert.Equal(t, 0, meta.AvailableCount)
		assert.Equal(t, 0, meta.PendingCount)
		// Cannot change the language runtime
		assert.Equal(t, "", meta.Runtime)
	}
}

// TestMetaContinuous specifically checks that you cannot enable the
// "continuous" flag on non-continuous work specs.
func TestMetaContinuous(t *testing.T) {
	var (
		err       error
		namespace coordinate.Namespace
		spec      coordinate.WorkSpec
		meta      coordinate.WorkSpecMeta
	)

	namespace, err = Coordinate.Namespace("TestMetaContinuous")
	if !assert.NoError(t, err) {
		return
	}
	defer namespace.Destroy()

	spec, err = namespace.SetWorkSpec(map[string]interface{}{
		"name":   "spec",
		"min_gb": 1,
	})
	if !assert.NoError(t, err) {
		return
	}

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		assert.False(t, meta.Continuous)
		assert.False(t, meta.CanBeContinuous)
	}

	meta.Continuous = true
	err = spec.SetMeta(meta)
	assert.NoError(t, err)

	meta, err = spec.Meta(false)
	if assert.NoError(t, err) {
		// Cannot set the "continuous" flag
		assert.False(t, meta.Continuous)
		assert.False(t, meta.CanBeContinuous)
	}
}

// TestMetaCounts does basic tests on the "available" and "pending" counts.
func TestMetaCounts(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestMetaCounts",
		WorkerName:    "worker",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	checkCounts := func(available, pending int) {
		meta, err := sts.WorkSpec.Meta(true)
		if assert.NoError(t, err) {
			assert.Equal(t, available, meta.AvailableCount)
			assert.Equal(t, pending, meta.PendingCount)
		}
	}
	checkCounts(0, 0)

	// Adding a work unit adds to the available count
	_, err := sts.AddWorkUnit("one")
	if assert.NoError(t, err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	Clock.Add(5 * time.Second)
	attempt := sts.RequestOneAttempt(t)
	checkCounts(0, 1)

	// Expiring an attempt makes it available again
	err = attempt.Expire(nil)
	if assert.NoError(t, err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	Clock.Add(5 * time.Second)
	attempt = sts.RequestOneAttempt(t)
	checkCounts(0, 1)

	// Marking an attempt retryable makes it pending again
	err = attempt.Retry(nil, time.Duration(0))
	if assert.NoError(t, err) {
		checkCounts(1, 0)
	}

	// Starting an attempt makes it pending
	Clock.Add(5 * time.Second)
	attempt = sts.RequestOneAttempt(t)
	checkCounts(0, 1)

	// Finishing an attempt takes it out of the list entirely
	err = attempt.Finish(nil)
	if assert.NoError(t, err) {
		checkCounts(0, 0)
	}
}

// TestSpecDeletedGone validates that, if you delete a work spec,
// subsequent attempts to use it return ErrGone.
func TestSpecDeletedGone(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestSpecDeletedGone",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	defer sts.TearDown(t)

	err := sts.Namespace.DestroyWorkSpec(sts.WorkSpecName)
	assert.NoError(t, err)

	// Test a couple of basic things
	_, err = sts.WorkSpec.Meta(false)
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "unexpected error reading deleted work spec meta",
			"+v", err)
	}

	_, err = sts.WorkSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "unexpected error adding work to deleted work spec",
			"+v", err)
	}
}

// TestSpecInNamespaceGone validates that, if you delete a work spec's
// namespace, attempts to use the work spec return ErrGone.
func TestSpecInNamespaceGone(t *testing.T) {
	sts := SimpleTestSetup{
		NamespaceName: "TestSpecInNamespaceGone",
		WorkSpecName:  "spec",
	}
	sts.SetUp(t)
	// We are about to blow up the namespace now so there is no cleanup

	err := sts.Namespace.Destroy()
	assert.NoError(t, err)

	// Test a couple of basic things
	_, err = sts.WorkSpec.Meta(false)
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "unexpected error reading deleted work spec meta",
			"+v", err)
	}

	_, err = sts.WorkSpec.AddWorkUnit("foo", map[string]interface{}{}, coordinate.WorkUnitMeta{})
	if err == coordinate.ErrGone {
		// okay
	} else if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
		assert.Equal(t, sts.WorkSpecName, nsws.Name)
	} else {
		assert.Fail(t, "unexpected error adding work to deleted work spec",
			"+v", err)
	}
}
