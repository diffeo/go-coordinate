// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"fmt"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/lib/pq"
	"github.com/ugorji/go/codec"
	"math"
	"strings"
	"time"
)

// dictionary <-> binary encoders

func mapToBytes(in map[string]interface{}) (out []byte, err error) {
	cbor := new(codec.CborHandle)
	err = cborrpc.SetExts(cbor)
	if err != nil {
		return
	}
	encoder := codec.NewEncoderBytes(&out, cbor)
	err = encoder.Encode(in)
	return
}

func bytesToMap(in []byte) (out map[string]interface{}, err error) {
	cbor := new(codec.CborHandle)
	err = cborrpc.SetExts(cbor)
	if err != nil {
		return
	}
	decoder := codec.NewDecoderBytes(in, cbor)
	err = decoder.Decode(&out)
	return
}

// other SQL encoders

// durationToSQL converts a time.Duration to ISO standard SQL syntax,
// e.g. "1 2:3:4" for one day, two hours, three minutes, and four seconds.
func durationToSQL(d time.Duration) []byte {
	dSeconds := d.Seconds()
	dMinutes, fSeconds := math.Modf(dSeconds / 60)
	seconds := fSeconds * 60
	dHours, fMinutes := math.Modf(dMinutes / 60)
	minutes := fMinutes * 60
	days, fHours := math.Modf(dHours / 24)
	hours := fHours * 24
	sql := fmt.Sprintf("%.0f %.0f:%.0f:%f", days, hours, minutes, seconds)
	return []byte(sql)
}

func sqlToDuration(sql string) (time.Duration, error) {
	var (
		days, hours, minutes int64
		seconds              float64
		err                  error
	)
	// Two shots
	_, err = fmt.Sscanf(sql, "%d:%d:%f", &hours, &minutes, &seconds)
	if err != nil {
		_, err = fmt.Sscanf(sql, "%d %d:%d:%f", &days, &hours, &minutes, &seconds)
	}
	// Duration's unit is nanoseconds; make sure everything has int64
	// type
	dHours := hours * 24 * days
	dMinutes := minutes + 60*dHours
	dSeconds := seconds + 60*float64(dMinutes)
	d := time.Duration(int64(float64(dSeconds * float64(time.Second))))
	return d, err
}

// timeToNullTime encodes a time as a pq-specific NullTime, by mapping the
// zero time to null.
func timeToNullTime(t time.Time) pq.NullTime {
	return pq.NullTime{Time: t, Valid: !t.IsZero()}
}

// nullTimeToTime decodes a pq-specific NullTime to a time, by mapping
// a null value to zero time.
func nullTimeToTime(nt pq.NullTime) time.Time {
	if nt.Valid {
		return nt.Time
	}
	return time.Time{}
}

// buildSelect constructs a simple SQL SELECT statement by string
// concatenation.  All of the conditions are ANDed together.
func buildSelect(outputs, tables, conditions []string) string {
	query := "SELECT "
	query += strings.Join(outputs, ", ")
	query += " FROM "
	query += strings.Join(tables, ", ")
	if len(conditions) > 0 {
		query += " WHERE "
		query += strings.Join(conditions, " AND ")
	}
	return query
}

// buildUpdate constructs a simple SQL UPDATE statement by string
// concatenation.  All of the conditions are ANDed together.
func buildUpdate(table string, changes, conditions []string) string {
	query := "UPDATE " + table
	if len(changes) > 0 {
		query += " SET " + strings.Join(changes, ", ")
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	return query
}
