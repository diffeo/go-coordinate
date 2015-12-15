// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

// This file contains a REST skeleton framework.
//
// The bulk of this is dealing with HTTP content type negotiation, and
// providing a standard way to deal with input and output values.
// This could probably be made more generic: the major variables are
// the type canonicalization map, the context builder, and specific
// codecs.  In turn our specific choice of MIME type implies an intent
// to support different JSON variants to produce the same underlying
// structure, which this fails badly at.
//
// Another more generic solution out there is
// https://github.com/jchannon/negotiator.  This only deals with
// output type negotiation, forces all JSON-ish output to report
// itself as "application/json", and doesn't deal well with other HTTP
// status codes.

import (
	"errors"
	"fmt"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/ugorji/go/codec"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

var typeMap = map[string]string{
	"text/json":              restdata.V1JSONMediaType,
	"application/json":       restdata.V1JSONMediaType,
	restdata.JSONMediaType:   restdata.V1JSONMediaType,
	restdata.V1JSONMediaType: restdata.V1JSONMediaType,
}

// errBadAccept is returned from negotiateResponse() if the Accept:
// header is malformed (and no more specific error applies).
var errBadAccept = errors.New("Invalid Accept: header")

// errNotAcceptable is returned from negotiateResponse() if the Accept:
// header does not mention any media types we can actually return.
type errNotAcceptable struct{}

func (e errNotAcceptable) Error() string {
	return "No acceptable representation for response"
}

func (e errNotAcceptable) HTTPStatus() int {
	return http.StatusNotAcceptable
}

// errNotImplemented is returned from an arbitrary handler function if
// the actual function is not implemented.
type errNotImplemented struct {
	Text string
}

func (e errNotImplemented) Error() string {
	if e.Text == "" {
		return "Not implemented"
	}
	return e.Text
}

func (e errNotImplemented) HTTPStatus() int {
	return http.StatusNotImplemented
}

// errMethodNotAllowed is used within the resourceHandler implementation
// to flag an error if a particular HTTP method is not allowed.  This
// corresponds exactly to the 405 Method Not Allowed HTTP status code.
type errMethodNotAllowed struct {
	Method string
}

func (e errMethodNotAllowed) Error() string {
	return fmt.Sprintf("Method %v not allowed", e.Method)
}

func (e errMethodNotAllowed) HTTPStatus() int {
	return http.StatusMethodNotAllowed
}

// responseCreated is returned as a value response from handler
// functions that want to indicate that a new resource was created.
type responseCreated struct {
	// Location holds the canonical URL to the newly created resource.
	Location string

	// Body contains the object sent in the body of the response.
	Body interface{}
}

type resourceHandler struct {
	// Representation is an object representing this resource.
	// A copy of this object will be passed to handler functions.
	Representation interface{}

	// Context reads an HTTP request and produces a context object.
	Context func(req *http.Request) (*context, error)

	// Get, if non-nil, returns a representation of the object.
	// Its return type should be the same type as Representation,
	// though this is not enforced.
	Get func(*context) (interface{}, error)

	// Put, if non-nil, updates the representation of the object.
	// The interface parameter is guaranteed to be the same type
	// as Representation.  The return can be any useful return
	// value.
	Put func(*context, interface{}) (interface{}, error)

	// Post, if non-nil, takes some arbitrary action.  The
	// interface parameter is guaranteed to be the same type as
	// Representation, though in this case this is not necessarily
	// a representation of the resource.  The return can be any
	// useful return value, include responseCreated.
	Post func(*context, interface{}) (interface{}, error)

	// Delete, if non-nil, deletes the object.  The return can be
	// any useful return value.
	Delete func(*context) (interface{}, error)
}

func (h *resourceHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	var (
		ctx          *context
		in, out      interface{}
		err          error
		status       int
		responseType string
	)

	// Recover from panics by sending an HTTP error.
	defer func() {
		if recovered := recover(); recovered != nil {
			response := restdata.ErrorResponse{}
			response.FromPanic(recovered)
			resp.Header().Set("Content-Type", restdata.V1JSONMediaType)
			resp.WriteHeader(http.StatusInternalServerError)
			json := &codec.JsonHandle{}
			encoder := codec.NewEncoder(resp, json)
			encoder.MustEncode(response)
		}
	}()

	// Start by trying to come up with a response type, even before
	// trying to parse the input.  This determines what format an
	// error message could be sent back as.
	if err == nil {
		// Errors here by default are in the header setup
		status = http.StatusBadRequest
		responseType, err = negotiateResponse(req)
		if err != nil {
			// Gotta pick something
			responseType = restdata.V1JSONMediaType
		}
	}

	// Get bits from URL parameters
	if err == nil {
		ctx, err = h.Context(req)
	}

	// Read the (JSON?) body, if it's there
	if err == nil && (req.Method == "PUT" || req.Method == "POST") {
		// Make a new object of the same type as h.In
		in = reflect.Zero(reflect.TypeOf(h.Representation)).Interface()

		// Then decode the message body into that object
		contentType := req.Header.Get("Content-Type")
		err = restdata.Decode(contentType, req.Body, &in)
	}

	// Actually call the handler method
	if err == nil {
		// We will return this if the method is unexpected or
		// we don't have a handler for it
		err = errMethodNotAllowed{Method: req.Method}
		// If anything else goes wrong here, it's an error in
		// client code
		status = http.StatusInternalServerError
		switch req.Method {
		case "GET", "HEAD":
			if h.Get != nil {
				out, err = h.Get(ctx)
			}
		case "PUT":
			if h.Put != nil {
				out, err = h.Put(ctx, in)
			}
		case "POST":
			if h.Post != nil {
				out, err = h.Post(ctx, in)
			}
		case "DELETE":
			if h.Delete != nil {
				out, err = h.Delete(ctx)
			}
		}
	}

	// Fix up the final result based on what we know.
	if err != nil {
		// Pick a better status code if we know of one
		if errS, hasStatus := err.(restdata.ErrorStatus); hasStatus {
			status = errS.HTTPStatus()
		}
		resp := restdata.ErrorResponse{Error: "error", Message: err.Error()}
		resp.FromError(err)
		// Remap well-known coordinate errors
		out = resp
	} else if out == nil {
		status = http.StatusNoContent
	} else if created, isCreated := out.(responseCreated); isCreated {
		status = http.StatusCreated
		if created.Location != "" {
			resp.Header().Set("Location", created.Location)
		}
		if req.Method == "HEAD" {
			out = nil
		} else {
			out = created.Body
		}
	} else {
		status = http.StatusOK
		if req.Method == "HEAD" {
			out = nil
		}
	}

	// Come up with a function to write the response.  If setting
	// this up fails it could produce another error.  :-/ It is
	// also possible for the actual writer to fail, but by the
	// point this happens we've already written an HTTP status
	// line, so we're not necessarily doing better than panicking.
	responseWriters := map[string]func(){
		restdata.V1JSONMediaType: func() {
			json := &codec.JsonHandle{}
			encoder := codec.NewEncoder(resp, json)
			encoder.MustEncode(out)
		},
	}
	responseWriter, understood := responseWriters[typeMap[responseType]]
	if !understood {
		// We shouldn't get here, because it implies response
		// type negotiation failed...but here we are
		responseWriter = responseWriters[restdata.V1JSONMediaType]
		status = http.StatusInternalServerError
		out = restdata.ErrorResponse{Error: "error", Message: "Invalid response type " + responseType}
	}

	// Actually send the response
	if out != nil {
		resp.Header().Set("Content-Type", responseType)
	}
	resp.WriteHeader(status)
	if out != nil {
		responseWriter()
	}
}

// negotiateResponse returns a supported MIME type for the response
// body, following the path laid out in RFC 7231 section 5.3.
func negotiateResponse(req *http.Request) (string, error) {
	accept := req.Header.Get("Accept")
	if accept == "" {
		accept = "*/*"
	}
	bestType := ""
	bestQ := 0.0
	mediaRanges := strings.Split(accept, ",")
	for _, mediaRange := range mediaRanges {
		mediaRange = strings.TrimSpace(mediaRange)
		mediaType, params, err := mime.ParseMediaType(mediaRange)
		if err != nil {
			return "", err
		}

		// What is the "q" ("quality") parameter for this type?
		// If it is less than the best known so far, skip it
		q := 1.0
		if qStr, haveQ := params["q"]; haveQ {
			q, err = strconv.ParseFloat(qStr, 64)
			if err != nil {
				return "", err
			}
			if q < 0.0 || q > 1.0 {
				return "", errBadAccept
			}
		}
		if q < bestQ {
			continue
		}

		// This is acceptable if it's listed in the type
		// map; or it's one of a couple of specific wildcards.
		// Also need to handle wildcard precedence.  So:
		if mediaType == "*/*" {
			// Doesn't override anything.
			if q > bestQ {
				bestType = mediaType
				bestQ = q
			}
		} else if mediaType == "text/*" || mediaType == "application/*" {
			// Only overrides "*/*".
			if q > bestQ || bestType == "*/*" {
				bestType = mediaType
				bestQ = q
			}
		} else if _, knownType := typeMap[mediaType]; knownType {
			// Overrides any wildcard.  We want the first one
			// at a given q to win.
			if q > bestQ || bestType == "*/*" || bestType == "text/*" || bestType == "application/*" {
				bestType = mediaType
				bestQ = q
			}
		}
		// Otherwise we don't recognize this type at all, so
		// just drop it.
		//
		// The RFC endorses honoring type parameters as being
		// "more specific" but we don't really deal with that.
	}
	// If this failed to win, return an error
	if bestQ == 0.0 {
		return "", errNotAcceptable{}
	}
	switch bestType {
	case "*/*":
		return restdata.V1JSONMediaType, nil
	case "application/*":
		return restdata.V1JSONMediaType, nil
	case "text/*":
		return "text/json", nil
	default:
		return bestType, nil
	}
}
