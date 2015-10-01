package postgres

const (
	// SQL table names:
	workSpecTable = "work_spec"
	workUnitTable = "work_unit"
	attemptTable  = "attempt"

	// SQL column names:
	attemptID         = attemptTable + ".id"
	attemptStatus     = attemptTable + ".status"
	workSpecID        = workSpecTable + ".id"
	workSpecName      = workSpecTable + ".name"
	workSpecNamespace = workSpecTable + ".namespace_id"
	workUnitID        = workUnitTable + ".id"
	workUnitName      = workUnitTable + ".name"
	workUnitSpec      = workUnitTable + ".work_spec_id"
	workUnitAttempt   = workUnitTable + ".active_attempt_id"

	// This join selects all work units and attempts, including
	// work units with no active attempt
	workUnitAttemptJoin = (workUnitTable + " LEFT OUTER JOIN " +
		attemptTable + "  ON " + workUnitAttempt + "=" + attemptID)

	// WHERE clause fragments:
	inThisNamespace    = workSpecNamespace + "=$1"
	inThisWorkSpec     = workUnitSpec + "=$1"
	workUnitInSpec     = workUnitSpec + "=" + workSpecID
	attemptIsAvailable = ("(" +
		attemptStatus + " IS NULL OR " +
		attemptStatus + "='expired' OR " +
		attemptStatus + "='retryable')")
)
