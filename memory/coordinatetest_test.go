package memory_test

import "testing"
import "github.com/diffeo/go-coordinate/coordinate/coordinatetest"

func TestAttemptLifetime(t *testing.T) {
	coordinatetest.TestAttemptLifetime(t)
}
func TestAttemptMetadata(t *testing.T) {
	coordinatetest.TestAttemptMetadata(t)
}
func TestWorkUnitChaining(t *testing.T) {
	coordinatetest.TestWorkUnitChaining(t)
}
func TestChainingMixed(t *testing.T) {
	coordinatetest.TestChainingMixed(t)
}
func TestChainingTwoStep(t *testing.T) {
	coordinatetest.TestChainingTwoStep(t)
}
func TestChainingExpiry(t *testing.T) {
	coordinatetest.TestChainingExpiry(t)
}
func TestChainingDuplicate(t *testing.T) {
	coordinatetest.TestChainingDuplicate(t)
}
func TestAttemptExpiration(t *testing.T) {
	coordinatetest.TestAttemptExpiration(t)
}
func TestRetryDelay(t *testing.T) {
	coordinatetest.TestRetryDelay(t)
}
func TestAttemptFractionalStart(t *testing.T) {
	coordinatetest.TestAttemptFractionalStart(t)
}
func TestAttemptGone(t *testing.T) {
	coordinatetest.TestAttemptGone(t)
}
func TestNamespaceTrivial(t *testing.T) {
	coordinatetest.TestNamespaceTrivial(t)
}
func TestNamespaces(t *testing.T) {
	coordinatetest.TestNamespaces(t)
}
func TestSpecCreateDestroy(t *testing.T) {
	coordinatetest.TestSpecCreateDestroy(t)
}
func TestSpecErrors(t *testing.T) {
	coordinatetest.TestSpecErrors(t)
}
func TestTwoWorkSpecsBasic(t *testing.T) {
	coordinatetest.TestTwoWorkSpecsBasic(t)
}
func TestConcurrentExecution(t *testing.T) {
	coordinatetest.TestConcurrentExecution(t)
}
func TestAddSameUnit(t *testing.T) {
	coordinatetest.TestAddSameUnit(t)
}
func BenchmarkWorkUnitCreation(b *testing.B) {
	coordinatetest.BenchmarkWorkUnitCreation(b)
}
func BenchmarkWorkUnitExecution(b *testing.B) {
	coordinatetest.BenchmarkWorkUnitExecution(b)
}
func BenchmarkMultiAttempts(b *testing.B) {
	coordinatetest.BenchmarkMultiAttempts(b)
}
func BenchmarkUnitOutput(b *testing.B) {
	coordinatetest.BenchmarkUnitOutput(b)
}
func TestChangeSpecData(t *testing.T) {
	coordinatetest.TestChangeSpecData(t)
}
func TestDataEmptyList(t *testing.T) {
	coordinatetest.TestDataEmptyList(t)
}
func TestDefaultMeta(t *testing.T) {
	coordinatetest.TestDefaultMeta(t)
}
func TestPrefilledMeta(t *testing.T) {
	coordinatetest.TestPrefilledMeta(t)
}
func TestSetDataSetsMeta(t *testing.T) {
	coordinatetest.TestSetDataSetsMeta(t)
}
func TestNiceWeight(t *testing.T) {
	coordinatetest.TestNiceWeight(t)
}
func TestSetMeta(t *testing.T) {
	coordinatetest.TestSetMeta(t)
}
func TestMetaContinuous(t *testing.T) {
	coordinatetest.TestMetaContinuous(t)
}
func TestMetaCounts(t *testing.T) {
	coordinatetest.TestMetaCounts(t)
}
func TestSpecDeletedGone(t *testing.T) {
	coordinatetest.TestSpecDeletedGone(t)
}
func TestSpecInNamespaceGone(t *testing.T) {
	coordinatetest.TestSpecInNamespaceGone(t)
}
func TestOneDayInterval(t *testing.T) {
	coordinatetest.TestOneDayInterval(t)
}
func TestTrivialWorkUnitFlow(t *testing.T) {
	coordinatetest.TestTrivialWorkUnitFlow(t)
}
func TestWorkUnitQueries(t *testing.T) {
	coordinatetest.TestWorkUnitQueries(t)
}
func TestDeleteWorkUnits(t *testing.T) {
	coordinatetest.TestDeleteWorkUnits(t)
}
func TestCountWorkUnitStatus(t *testing.T) {
	coordinatetest.TestCountWorkUnitStatus(t)
}
func TestWorkUnitOrder(t *testing.T) {
	coordinatetest.TestWorkUnitOrder(t)
}
func TestWorkUnitPriorityCtor(t *testing.T) {
	coordinatetest.TestWorkUnitPriorityCtor(t)
}
func TestWorkUnitPrioritySet(t *testing.T) {
	coordinatetest.TestWorkUnitPrioritySet(t)
}
func TestWorkUnitData(t *testing.T) {
	coordinatetest.TestWorkUnitData(t)
}
func TestRecreateWorkUnits(t *testing.T) {
	coordinatetest.TestRecreateWorkUnits(t)
}
func TestContinuous(t *testing.T) {
	coordinatetest.TestContinuous(t)
}
func TestContinuousInterval(t *testing.T) {
	coordinatetest.TestContinuousInterval(t)
}
func TestMaxRunning(t *testing.T) {
	coordinatetest.TestMaxRunning(t)
}
func TestRequestSpecificSpec(t *testing.T) {
	coordinatetest.TestRequestSpecificSpec(t)
}
func TestByRuntime(t *testing.T) {
	coordinatetest.TestByRuntime(t)
}
func TestNotBeforeDelayedStatus(t *testing.T) {
	coordinatetest.TestNotBeforeDelayedStatus(t)
}
func TestNotBeforeAttempt(t *testing.T) {
	coordinatetest.TestNotBeforeAttempt(t)
}
func TestNotBeforePriority(t *testing.T) {
	coordinatetest.TestNotBeforePriority(t)
}
func TestDelayedOutput(t *testing.T) {
	coordinatetest.TestDelayedOutput(t)
}
func TestUnitDeletedGone(t *testing.T) {
	coordinatetest.TestUnitDeletedGone(t)
}
func TestUnitSpecDeletedGone(t *testing.T) {
	coordinatetest.TestUnitSpecDeletedGone(t)
}
func TestWorkerAncestry(t *testing.T) {
	coordinatetest.TestWorkerAncestry(t)
}
func TestWorkerAdoption(t *testing.T) {
	coordinatetest.TestWorkerAdoption(t)
}
func TestWorkerMetadata(t *testing.T) {
	coordinatetest.TestWorkerMetadata(t)
}
func TestWorkerAttempts(t *testing.T) {
	coordinatetest.TestWorkerAttempts(t)
}
func TestDeactivateChild(t *testing.T) {
	coordinatetest.TestDeactivateChild(t)
}
