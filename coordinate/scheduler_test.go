// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

// A note on probability:
//
// Many of the tests (all of the multi-spec simplified tests and all
// of the multi-choice then-chain tests) look at the probability
// distribution of what comes out of the scheduler.  If the odds of
// picking choice 𝑎 are P(𝑎), then the expected number in 𝑛 runs is
// E(𝑎)=𝑛P(𝑎), and the standard deviation is σ(𝑎)=√(𝑛P(𝑎)(1-P(𝑎))).
// The tests are tuned so that we'll accept E(𝑎) within ±3σ(𝑎).

// expectation calculates a basic expected number of occurrences for
// an event that is expected to happen (numerator/denominator) times,
// in n trials.
func expectation(n, numerator, denominator int) int {
	return (n * numerator) / denominator
}

// stdDev calculates a standard deviation for the number of
// occurrences for an event that is expected to happen
// (numerator/denominator) times, in n trials.
func stdDev(n, numerator, denominator int) float64 {
	p := float64(numerator) / float64(denominator)
	return math.Sqrt(float64(n) * p * (1 - p))
}

// runScheduler runs the simplified scheduler the specified number of
// times and returns the number of times each work spec was chosen.
func runScheduler(t *testing.T, metas map[string]*WorkSpecMeta, trials int) map[string]int {
	counts := make(map[string]int)
	for i := 0; i < trials; i++ {
		workSpecName, err := SimplifiedScheduler(metas, time.Now(), 1)
		if assert.NoError(t, err) {
			counts[workSpecName]++
		}
	}
	return counts
}

// TestEmpty verifies that the simplified scheduler does the right
// thing with no data.
func TestEmpty(t *testing.T) {
	metas := map[string]*WorkSpecMeta{}
	_, err := SimplifiedScheduler(metas, time.Now(), 1)
	assert.Equal(t, ErrNoWork, err)
}

// TestOneSpec verifies that the simplified scheduler does the
// right thing with one plain work spec.
func TestOneSpec(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestZeroWeight verifies that the simplified scheduler does the
// right thing if it only has work specs with zero weight.
func TestZeroWeight(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         0,
			AvailableCount: 1000,
		},
	}
	_, err := SimplifiedScheduler(metas, time.Now(), 1)
	assert.Equal(t, ErrNoWork, err)
}

// TestTwoSpecsOnePaused verifies that the simplified scheduler does
// not return a paused work spec.
func TestTwoSpecsOnePaused(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
			Paused:         true,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestTwoSpecsOneEmpty verifies that the simplified scheduler does
// not return a work spec with no work units.
func TestTwoSpecsOneEmpty(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 0,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestTwoSpecsOneFull verifies that the simplified scheduler does
// not return a work spec that has already reached its MaxRunning count.
func TestTwoSpecsOneFull(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			PendingCount:   10,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			PendingCount:   10,
			MaxRunning:     10,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestTwoEqualSpecs verifies that the simplified scheduler picks two
// equivalent work specs with roughly equal weight.
func TestTwoEqualSpecs(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	delta := 3 * stdDev(trials, 1, 2)
	assert.InDelta(t, trials/2, counts["one"], delta)
	assert.InDelta(t, trials/2, counts["two"], delta)
}

// TestTwoUnequalSpecs verifies that the simplified scheduler picks two
// equivalent work specs with very different weights.
func TestTwoUnequalSpecs(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         10,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	delta := 3 * stdDev(trials, 1, 11)
	assert.InDelta(t, trials*1/11, counts["one"], delta)
	assert.InDelta(t, trials*10/11, counts["two"], delta)
}

// TestTwoUnequalSpecsWithWork verifies that the simplified scheduler
// picks two equivalent work specs with very different weights, and
// with some pending work.
func TestTwoUnequalSpecsWithWork(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         10,
			PendingCount:   2,
			AvailableCount: 998,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	// These actual ratios come from the way the scheduler makes
	// its choices.  There are 2 work units pending, and there
	// will be 3 in total if one more is added.  Work spec "one"
	// "wants" 1/11 of this total, or 3/11 in all.  Work spec
	// "two" "wants" 10/11 of this total, or 30/11, but already
	// has 22/11 pending, so it "wants" 8/11 more.
	assert.InDelta(t, trials*3/11, counts["one"],
		3*stdDev(trials, 3, 11))
	assert.InDelta(t, trials*8/11, counts["two"],
		3*stdDev(trials, 8, 11))
}

// TestTwoUnequalSpecsOneFull verifies that the simplified scheduler
// can be forced to choose a lower-weight work spec.
func TestTwoUnequalSpecsOneFull(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			PendingCount:   0,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:       1,
			PendingCount: 10,
			MaxRunning:   10,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestThreeSpecsOneOverfull verifies that the scheduler behaves reasonably
// if one work spec has more jobs than its weight suggests.
func TestThreeSpecsOneOverfull(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			PendingCount:   0,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         5,
			PendingCount:   0,
			AvailableCount: 1000,
		},
		"three": &WorkSpecMeta{
			Weight:         1,
			PendingCount:   99,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	// This setup produces a negative score for "three"!  "one"
	// should have a score of 100, and "two" 500, but "three"
	// should come up with
	// (weight * (total pending + 1)) - pending * total weight
	// 1 * 100 - 99 * 7 = 100 - 693 = -593
	// and so "three" should basically just get ignored.
	assert.InDelta(t, trials*1/6, counts["one"], 3*stdDev(trials, 1, 6))
	assert.InDelta(t, trials*5/6, counts["two"], 3*stdDev(trials, 5, 6))
}

// TestTwoSpecsContinuous tests that a continuous work spec can be
// returned according to its weight, provided it has no work units.
func TestTwoSpecsContinuous(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:     1,
			Continuous: true,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.InDelta(t, trials/2, counts["one"], 3*stdDev(trials, 1, 2))
	assert.InDelta(t, trials/2, counts["two"], 3*stdDev(trials, 1, 2))
}

// TestTwoSpecsContinuousBusy tests that a continuous work spec will
// not be returned if it has pending but not available work units.
func TestTwoSpecsContinuousBusy(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:       1,
			Continuous:   true,
			PendingCount: 1,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["one"])
}

// TestThreeSpecsEqual tests that the scheduler behaves consistently
// with three equal work specs.
func TestThreeSpecsEqual(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"three": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.InDelta(t, trials/3, counts["one"], 3*stdDev(trials, 1, 3))
	assert.InDelta(t, trials/3, counts["two"], 3*stdDev(trials, 1, 3))
	assert.InDelta(t, trials/3, counts["three"], 3*stdDev(trials, 1, 3))
}

// TestThreeSpecsPriority tests that the scheduler gives absolute
// priority if the priority field is specified.
func TestThreeSpecsPriority(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"three": &WorkSpecMeta{
			Priority:       2,
			Weight:         1,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.Equal(t, trials, counts["three"])
}

// TestThreeSpecsPriorityBusy tests that the scheduler will give out
// lower-priority work specs if higher-priority ones are busy.
func TestThreeSpecsPriorityBusy(t *testing.T) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"two": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
		"three": &WorkSpecMeta{
			Priority: 2,
			Weight:   1,
		},
	}
	trials := 1000
	counts := runScheduler(t, metas, trials)
	assert.InDelta(t, trials/2, counts["one"], 3*stdDev(trials, 1, 2))
	assert.InDelta(t, trials/2, counts["two"], 3*stdDev(trials, 1, 2))
}
