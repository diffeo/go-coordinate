package memory_test

import (
	"github.com/dmaze/goordinate/coordinate/coordinatetest"
	"github.com/dmaze/goordinate/memory"
	"gopkg.in/check.v1"
	"testing"
)

// Test is the top-level entry point to run tests.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	check.Suite(&coordinatetest.Suite{Coordinate: memory.New()})
}
