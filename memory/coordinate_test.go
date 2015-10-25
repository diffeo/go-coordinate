package memory_test

import (
	"github.com/benbjohnson/clock"
	"github.com/dmaze/goordinate/coordinate/coordinatetest"
	"github.com/dmaze/goordinate/memory"
	"gopkg.in/check.v1"
	"testing"
)

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	clock := clock.NewMock()
	backend := memory.NewWithClock(clock)
	check.Suite(&coordinatetest.Suite{
		Coordinate: backend,
		Clock:      clock,
	})
}
