// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restdata

import (
	"errors"
	"fmt"
	"github.com/diffeo/go-coordinate/coordinate"
	"net/http"
	"runtime"
)

// ErrorStatus describes errors that correspond to specific HTTP status
// codes.
type ErrorStatus interface {
	// HTTPStatus returns the HTTP status code for this error.
	HTTPStatus() int
}

// ErrUnsupportedMediaType is returned from Decode() if the provided
// Content-Type: is unrecognized.  This translates directly into the
// equivalent HTTP 415 error.
type ErrUnsupportedMediaType struct {
	Type string
}

func (e ErrUnsupportedMediaType) Error() string {
	return fmt.Sprintf("Unsupported media type %q", e.Type)
}

// HTTPStatus returns a fixed 415 Unsupported Media Type error code.
func (e ErrUnsupportedMediaType) HTTPStatus() int {
	return http.StatusUnsupportedMediaType
}

// ErrNotFound is a wrapper error that indicates that, due to the
// embedded error, a REST service should return a 404 Not Found error.
type ErrNotFound struct {
	Err error
}

func (e ErrNotFound) Error() string {
	return e.Err.Error()
}

// HTTPStatus returns a fixed 404 Not Found error code.
func (e ErrNotFound) HTTPStatus() int {
	return http.StatusNotFound
}

// ErrBadRequest is returned as an error when there is an error decoding
// HTTP headers or the request body.
type ErrBadRequest struct {
	Err error
}

func (e ErrBadRequest) Error() string {
	return e.Err.Error()
}

// HTTPStatus returns a fixed 400 Bad Request HTTP status code.
func (e ErrBadRequest) HTTPStatus() int {
	return http.StatusBadRequest
}

// FromError populates an ErrorResponse to fill in its fields based
// on an error value.  This remaps the well-known Coordinate errors
// to specific e.Error codes.
func (e *ErrorResponse) FromError(err error) {
	switch err {
	case coordinate.ErrNoWorkSpecName:
		e.Error = "ErrNoWorkSpecName"
	case coordinate.ErrBadWorkSpecName:
		e.Error = "ErrBadWorkSpecName"
	case coordinate.ErrChangedName:
		e.Error = "ErrChangedName"
	case coordinate.ErrLostLease:
		e.Error = "ErrLostLease"
	case coordinate.ErrNotPending:
		e.Error = "ErrNotPending"
	case coordinate.ErrCannotBecomeContinuous:
		e.Error = "ErrCannotBecomeContinuous"
	case coordinate.ErrWrongBackend:
		e.Error = "ErrWrongBackend"
	case coordinate.ErrNoWork:
		e.Error = "ErrNoWork"
	case coordinate.ErrWorkUnitNotList:
		e.Error = "ErrWorkUnitNotList"
	case coordinate.ErrWorkUnitTooShort:
		e.Error = "ErrWorkUnitTooShort"
	case coordinate.ErrBadPriority:
		e.Error = "ErrBadPriority"
	case coordinate.ErrGone:
		e.Error = "ErrGone"
	}
	switch et := err.(type) {
	case coordinate.ErrNoSuchWorkSpec:
		e.Error = "ErrNoSuchWorkSpec"
		e.Value = et.Name
	case coordinate.ErrNoSuchWorkUnit:
		e.Error = "ErrNoSuchWorkUnit"
		e.Value = et.Name
	case ErrNotFound:
		// Discard this wrapper and return the embedded error
		e.FromError(et.Err)
	case ErrBadRequest:
		e.FromError(et.Err)
	}
}

// ToError converts e back to a Coordinate error, if that is possible.
// If not, returns a plain error with e.Message text.
func (e *ErrorResponse) ToError() error {
	switch e.Error {
	case "ErrNoWorkSpecName":
		return coordinate.ErrNoWorkSpecName
	case "ErrBadWorkSpecName":
		return coordinate.ErrBadWorkSpecName
	case "ErrChangedName":
		return coordinate.ErrChangedName
	case "ErrLostLease":
		return coordinate.ErrLostLease
	case "ErrNotPending":
		return coordinate.ErrNotPending
	case "ErrCannotBecomeContinuous":
		return coordinate.ErrCannotBecomeContinuous
	case "ErrWrongBackend":
		return coordinate.ErrWrongBackend
	case "ErrNoWork":
		return coordinate.ErrNoWork
	case "ErrWorkUnitNotList":
		return coordinate.ErrWorkUnitNotList
	case "ErrWorkUnitTooShort":
		return coordinate.ErrWorkUnitTooShort
	case "ErrBadPriority":
		return coordinate.ErrBadPriority
	case "ErrGone":
		return coordinate.ErrGone
	case "ErrNoSuchWorkSpec":
		return coordinate.ErrNoSuchWorkSpec{Name: e.Value}
	case "ErrNoSuchWorkUnit":
		return coordinate.ErrNoSuchWorkUnit{Name: e.Value}
	default:
		return errors.New(e.Message)
	}
}

// FromPanic populates an error response based on a panic.  Typical use
// is:
//
//     defer func() {
//         if obj := recovered(); obj != nil {
//             resp := restdata.ErrorResponse{}
//             resp.FromPanic(obj)
//             // write resp out as makes sense
//         }
//    }
func (e *ErrorResponse) FromPanic(obj interface{}) {
	e.Error = "panic"
	if recoveredError, isError := obj.(error); isError {
		e.Message = recoveredError.Error()
	} else {
		e.Message = fmt.Sprintf("%+v", obj)
	}
	var stack [4096]byte
	len := runtime.Stack(stack[:], false)
	e.Stack = string(stack[:len])
}
