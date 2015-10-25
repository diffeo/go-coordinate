package coordinate

import (
	"fmt"
	"gopkg.in/check.v1"
	"math"
	"testing"
	"time"
)

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

// SimplifiedSuite encapsulates the simplified scheduler tests.
type SimplifiedSuite struct{}

func init() {
	check.Suite(&SimplifiedSuite{})
}

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

// stdDev calculates a rounded standard deviation for the number of
// occurrences for an event that is expected to happen
// (numerator/denominator) times, in n trials.
func stdDev(n, numerator, denominator int) int {
	p := float64(numerator) / float64(denominator)
	sigma := math.Sqrt(float64(n) * p * (1 - p))
	return int(math.Ceil(sigma))
}

type threeSigmaChecker struct {
	*check.CheckerInfo
}

func (c *threeSigmaChecker) Info() *check.CheckerInfo {
	return c.CheckerInfo
}

func (c *threeSigmaChecker) Check(params []interface{}, names []string) (bool, string) {
	if len(params) < 4 {
		return false, "internal: too few parameters"
	}
	if len(names) < 4 {
		return false, "internal: too few names"
	}
	obtained, ok := params[0].(int)
	if !ok {
		return false, names[0] + " not an int"
	}
	n, ok := params[1].(int)
	if !ok {
		return false, names[1] + " not an int"
	}
	numerator, ok := params[2].(int)
	if !ok {
		return false, names[2] + " not an int"
	}
	denominator, ok := params[3].(int)
	if !ok {
		return false, names[3] + " not an int"
	}
	expected := expectation(n, numerator, denominator)
	sigma := stdDev(n, numerator, denominator)
	minimum := expected - 3*sigma
	maximum := expected + 3*sigma
	if obtained >= minimum && obtained <= maximum {
		return true, ""
	}
	return false, fmt.Sprintf("out of range (%v-%v), expected=%v",
		minimum, maximum, expected)
}

var WithinThreeSigma check.Checker = &threeSigmaChecker{
	&check.CheckerInfo{
		Name:   "WithinThreeSigma",
		Params: []string{"obtained", "n", "numerator", "denominator"}},
}

// RunScheduler runs the simplified scheduler the specified number of
// times and returns the number of times each work spec was chosen.
func (s *SimplifiedSuite) RunScheduler(c *check.C, metas map[string]*WorkSpecMeta, trials int) map[string]int {
	counts := make(map[string]int)
	for i := 0; i < trials; i++ {
		workSpecName, err := SimplifiedScheduler(metas, time.Now(), 1)
		c.Assert(err, check.IsNil)
		counts[workSpecName]++
	}
	return counts
}

// TestEmpty verifies that the simplified scheduler does the right
// thing with no data.
func (s *SimplifiedSuite) TestEmpty(c *check.C) {
	metas := map[string]*WorkSpecMeta{}
	_, err := SimplifiedScheduler(metas, time.Now(), 1)
	c.Check(err, check.Equals, ErrNoWork)
}

// TestOneSpec verifies that the simplified scheduler does the
// right thing with one plain work spec.
func (s *SimplifiedSuite) TestOneSpec(c *check.C) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         1,
			AvailableCount: 1000,
		},
	}
	trials := 1000
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestZeroWeight verifies that the simplified scheduler does the
// right thing if it only has work specs with zero weight.
func (s *SimplifiedSuite) TestZeroWeight(c *check.C) {
	metas := map[string]*WorkSpecMeta{
		"one": &WorkSpecMeta{
			Weight:         0,
			AvailableCount: 1000,
		},
	}
	_, err := SimplifiedScheduler(metas, time.Now(), 1)
	c.Check(err, check.Equals, ErrNoWork)
}

// TestTwoSpecsOnePaused verifies that the simplified scheduler does
// not return a paused work spec.
func (s *SimplifiedSuite) TestTwoSpecsOnePaused(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestTwoSpecsOneEmpty verifies that the simplified scheduler does
// not return a work spec with no work units.
func (s *SimplifiedSuite) TestTwoSpecsOneEmpty(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestTwoSpecsOneFull verifies that the simplified scheduler does
// not return a work spec that has already reached its MaxRunning count.
func (s *SimplifiedSuite) TestTwoSpecsOneFull(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestTwoEqualSpecs verifies that the simplified scheduler picks two
// equivalent work specs with roughly equal weight.
func (s *SimplifiedSuite) TestTwoEqualSpecs(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 2)
	c.Check(counts["two"], WithinThreeSigma, trials, 1, 2)
}

// TestTwoUnequalSpecs verifies that the simplified scheduler picks two
// equivalent work specs with very different weights.
func (s *SimplifiedSuite) TestTwoUnequalSpecs(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 11)
	c.Check(counts["two"], WithinThreeSigma, trials, 10, 11)
}

// TestTwoUnequalSpecsWithWork verifies that the simplified scheduler
// picks two equivalent work specs with very different weights, and
// with some pending work.
func (s *SimplifiedSuite) TestTwoUnequalSpecsWithWork(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	// These actual ratios come from the way the scheduler makes
	// its choices.  There are 2 work units pending, and there
	// will be 3 in total if one more is added.  Work spec "one"
	// "wants" 1/11 of this total, or 3/11 in all.  Work spec
	// "two" "wants" 10/11 of this total, or 30/11, but already
	// has 22/11 pending, so it "wants" 8/11 more.
	c.Check(counts["one"], WithinThreeSigma, trials, 3, 11)
	c.Check(counts["two"], WithinThreeSigma, trials, 8, 11)
}

// TestTwoUnequalSpecsOneFull verifies that the simplified scheduler
// can be forced to choose a lower-weight work spec.
func (s *SimplifiedSuite) TestTwoUnequalSpecsOneFull(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestThreeSpecsOneOverfull verifies that the scheduler behaves reasonably
// if one work spec has more jobs than its weight suggests.
func (s *SimplifiedSuite) TestThreeSpecsOneOverfull(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	// This setup produces a negative score for "three"!  "one"
	// should have a score of 100, and "two" 500, but "three"
	// should come up with
	// (weight * (total pending + 1)) - pending * total weight
	// 1 * 100 - 99 * 7 = 100 - 693 = -593
	// and so "three" should basically just get ignored.
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 6)
	c.Check(counts["two"], WithinThreeSigma, trials, 5, 6)
}

// TestTwoSpecsContinuous tests that a continuous work spec can be
// returned according to its weight, provided it has no work units.
func (s *SimplifiedSuite) TestTwoSpecsContinuous(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 2)
	c.Check(counts["two"], WithinThreeSigma, trials, 1, 2)
}

// TestTwoSpecsContinuousBusy tests that a continuous work spec will
// not be returned if it has pending but not available work units.
func (s *SimplifiedSuite) TestTwoSpecsContinuousBusy(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], check.Equals, trials)
}

// TestThreeSpecsEqual tests that the scheduler behaves consistently
// with three equal work specs.
func (s *SimplifiedSuite) TestThreeSpecsEqual(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 3)
	c.Check(counts["two"], WithinThreeSigma, trials, 1, 3)
	c.Check(counts["three"], WithinThreeSigma, trials, 1, 3)
}

// TestThreeSpecsPriority tests that the scheduler gives absolute
// priority if the priority field is specified.
func (s *SimplifiedSuite) TestThreeSpecsPriority(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["three"], check.Equals, trials)
}

// TestThreeSpecsPriorityBusy tests that the scheduler will give out
// lower-priority work specs if higher-priority ones are busy.
func (s *SimplifiedSuite) TestThreeSpecsPriorityBusy(c *check.C) {
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
	counts := s.RunScheduler(c, metas, trials)
	c.Check(counts["one"], WithinThreeSigma, trials, 1, 2)
	c.Check(counts["two"], WithinThreeSigma, trials, 1, 2)
}
