// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"database/sql"
	"sync"
	"time"
)

// expiry manages the semi-global expiration process.  In particular
// it ensures that not more than one instance of expiration is running
// at a time.
type expiry struct {
	Cond    *sync.Cond
	Running bool
}

// Init initializes an expiry object.
func (exp *expiry) Init() {
	exp.Cond = sync.NewCond(&sync.Mutex{})
}

// Do runs expiry.  When it returns, an instance of expiry has run to
// completion.  It may not actually perform expiry itself, instead
// blocking on some other goroutine to finish the job.
func (exp *expiry) Do(c coordinable) {
	// This lock protects Running and also is involved in the
	// condition variable
	exp.Cond.L.Lock()
	if exp.Running {
		// Note that really our only goal here is to ensure
		// that expiry runs once, so while the sync.Cond
		// documentation suggests running in a loop to make
		// sure the condition really is satisfied, if we ever
		// get signaled then our condition has been met.
		exp.Cond.Wait()
	} else {
		exp.Running = true
		exp.Cond.L.Unlock()
		// Unlock before actually running expiry so that other
		// goroutines can run; they will block in the section
		// above

		_ = withTx(c, false, func(tx *sql.Tx) error {
			return expireAttempts(c, tx)
		})

		exp.Cond.L.Lock()
		exp.Running = false
		exp.Cond.Broadcast()
	}
	exp.Cond.L.Unlock()
}

// expireAttempts finds all attempts whose expiration time has passed
// and expires them.  It runs on all attempts for all work units in all
// work specs in all namespaces (which simplifies the query).  Expired
// attempts' statuses become "expired", and those attempts cease to be
// the active attempt for their corresponding work unit.
//
// In general this should be called in its own transaction and its error
// return ignored:
//
//     _ = withTx(self, false, func(tx *sql.Tx) error {
//              return expireAttempts(self, tx)
//     })
//
// Expiry is generally secondary to whatever actual work is going on.
// If a result is different because of expiry, pretend the relevant
// call was made a second earlier or later.  If this fails, then
// either there is a concurrency issue (and since the query is
// system-global, the other expirer will clean up for us) or there is
// an operational error (and the caller will fail afterwards).
func expireAttempts(c coordinable, tx *sql.Tx) error {
	// There are several places this is called with much smaller
	// scope.  For instance, Attempt.Status() needs to invoke
	// expiry but only actually cares about this very specific
	// attempt.  If there are multiple namespaces,
	// Worker.RequestAttempts() only cares about this namespace
	// (though it will run on all work specs).  It may help system
	// performance to try to run this with narrower scope.
	//
	// This is probably also an excellent candidate for a stored
	// procedure.
	var (
		now        time.Time
		cte, query string
		count      int64
		result     sql.Result
		err        error
	)

	now = c.Coordinate().clock.Now()

	// Remove expiring attempts from their work unit
	qp := queryParams{}
	cte = buildSelect([]string{
		attemptID,
	}, []string{
		attemptTable,
	}, []string{
		attemptIsPending,
		attemptIsExpired(&qp, now),
	})
	query = buildUpdate(workUnitTable,
		[]string{"active_attempt_id=NULL"},
		[]string{"active_attempt_id IN (" + cte + ")"})
	result, err = tx.Exec(query, qp...)
	if err != nil {
		return err
	}

	// If this marked nothing as expired, we're done
	count, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

	// Mark attempts as expired
	qp = queryParams{}
	// A slightly exotic setup, since we want to reuse the "now"
	// param
	dollarsNow := qp.Param(now)
	fields := fieldList{}
	fields.AddDirect("expiration_time", dollarsNow)
	fields.AddDirect("status", "'expired'")
	query = buildUpdate(attemptTable, fields.UpdateChanges(), []string{
		attemptIsPending,
		attemptExpirationTime + "<" + dollarsNow,
	})
	_, err = tx.Exec(query, qp...)
	return err
}
