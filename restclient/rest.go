// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

// This file provides generic REST client code.

import (
	"bytes"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/jtacoma/uritemplates"
	"github.com/ugorji/go/codec"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// refreshable is any object that knows how to retrieve its own content.
// Typically this will be a resource that calls its own Get() method.
type refreshable interface {
	Refresh() error
}

// resource is any object that has a URL and a representation.
type resource struct {
	URL *url.URL
}

func (r *resource) Template(template string, vars map[string]interface{}) (*url.URL, error) {
	// Build the template object
	tmpl, err := uritemplates.Parse(template)
	if err != nil {
		return nil, err
	}

	// Encode all of the values if required
	for k, v := range vars {
		if s, isString := v.(string); isString {
			vars[k] = restdata.MaybeEncodeName(s)
		}
		if ss, isStringSlice := v.([]string); isStringSlice {
			tt := make([]string, len(ss))
			for i, s := range ss {
				tt[i] = restdata.MaybeEncodeName(s)
			}
			vars[k] = tt
		}
	}

	// Expand the template to produce a string
	expanded, err := tmpl.Expand(vars)
	if err != nil {
		return nil, err
	}

	// Return the parsed URL of the result, relative to ourselves
	return r.URL.Parse(expanded)
}

// Do performs some HTTP action.  If in is non-nil, the request data is
// serialized and sent as the body of, for instance, a POST request.
// If out is non-nil, the response data (if any) is deserialized into
// this object, which must be of pointer type.
func (r *resource) Do(method string, url *url.URL, in, out interface{}) (err error) {
	json := &codec.JsonHandle{}

	// Set up the body as serialized JSON, if there is one
	var body io.Reader
	if in != nil {
		reader, writer := io.Pipe()
		encoder := codec.NewEncoder(writer, json)
		finished := make(chan error)
		go func() {
			err := encoder.Encode(in)
			err = firstError(err, writer.Close())
			finished <- err
		}()
		defer func() {
			err = firstError(err, <-finished)
		}()
		body = reader
	}

	// Create the request and set headers
	req, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", restdata.V1JSONMediaType)
	}
	if out != nil {
		req.Header.Set("Accept", restdata.V1JSONMediaType)
	}

	// Actually do the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	// If the response included a body, clean up afterwards
	if resp.Body != nil {
		defer func() {
			err = firstError(err, resp.Body.Close())
		}()
	}

	// Check the response code
	if err = checkHTTPStatus(resp); err != nil {
		return err
	}

	// If there is both a body and a requested output,
	// decode it
	if resp.Body != nil && out != nil {
		contentType := resp.Header.Get("Content-Type")
		err = restdata.Decode(contentType, resp.Body, out)
	}

	return err // may be nil
}

// Get retrieves the resource from its own URL.  The result is stored
// in result, which must be of pointer type.
func (r *resource) Get(out interface{}) (err error) {
	return r.Do("GET", r.URL, nil, out)
}

// GetFrom retrieves a resource from some other URL.  template is
// interpreted as a URI template, modified by vars, and the result
// taken relative to the resource's URL.  The result is stored in
// result, which must be of pointer type.
func (r *resource) GetFrom(template string, vars map[string]interface{}, out interface{}) (err error) {
	url, err := r.Template(template, vars)
	if err == nil {
		err = r.Do("GET", url, nil, out)
	}
	return err
}

// Put updates the resource at its own URL.  The server response is
// stored in out, which must be of pointer type.
func (r *resource) Put(in, out interface{}) error {
	return r.Do("PUT", r.URL, in, out)
}

// PutTo updates a resource at some other URL.  template is
// interpreted as a URI template, modified by vars, and the result
// taken relative to the resource's URL.  The server response is
// stored in result, which must be of pointer type.
func (r *resource) PutTo(template string, vars map[string]interface{}, in, out interface{}) (err error) {
	url, err := r.Template(template, vars)
	if err == nil {
		err = r.Do("PUT", url, in, out)
	}
	return err
}

// PostTo submits data to a service at some other URL.  template is
// interpreted as a URI template, modified by vars, and the result
// taken relative to the resource's URL.  The server response is
// stored in out, which must be of pointer type.
func (r *resource) PostTo(template string, vars map[string]interface{}, in, out interface{}) error {
	url, err := r.Template(template, vars)
	if err == nil {
		err = r.Do("POST", url, in, out)
	}
	return err
}

// Delete deletes the resource at its own URL.
func (r *resource) Delete() (err error) {
	return r.Do("DELETE", r.URL, nil, nil)
}

// DeleteAt deletes the resource at some other URL.  template is
// interpreted as a URI template, modified by vars, and the result
// taken relative to the resource's URL.  The server response is
// stored in out, which must be of pointer type.
func (r *resource) DeleteAt(template string, vars map[string]interface{}, out interface{}) error {
	url, err := r.Template(template, vars)
	if err == nil {
		err = r.Do("DELETE", url, nil, out)
	}
	return err
}

// ErrorHTTP is a catch-all error for non-successes returned from the
// REST endpoint.
type ErrorHTTP struct {
	// Response holds a pointer to the failing HTTP response.
	Response *http.Response

	// Body holds the contents of the message body, presumed to
	// be text.
	Body string
}

func (e ErrorHTTP) Error() string {
	return e.Response.Status
}

// checkHTTPStatus examines an HTTP response and returns an error if
// it is not successful.
func checkHTTPStatus(resp *http.Response) error {
	if len(resp.Status) > 0 && resp.Status[0] == '2' {
		return nil
	}

	// Always collect the entire body; we will need it as a fallback
	// and can only parse it once.
	var body []byte
	var err error
	if resp.Body != nil {
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
	}

	// Take a shot at decoding it as a better error
	var errResp restdata.ErrorResponse
	contentType := resp.Header.Get("Content-Type")
	err2 := restdata.Decode(contentType, bytes.NewReader(body), &errResp)
	if err2 == nil {
		// Given that we decoded that successfully, return the
		// server-provided error
		return errResp.ToError()
	}

	return ErrorHTTP{Response: resp, Body: string(body)}
}

func firstError(e1, e2 error) error {
	if e1 != nil {
		return e1
	}
	return e2
}
