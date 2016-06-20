// Regression tests for rest.go.
//
// Main tests are really by running the end-to-end path, using the
// coordinatetest tests driven from restclient.  This only contains
// special-case bug tests.
//
// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"errors"
	"github.com/diffeo/go-coordinate/memory"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"testing"
)

type failResponseWriter struct {
	Headers    http.Header
	StatusCode int
}

func (rw *failResponseWriter) Header() http.Header {
	if rw.Headers == nil {
		rw.Headers = make(http.Header)
	}
	return rw.Headers
}

func (rw *failResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("foo")
}

func (rw *failResponseWriter) WriteHeader(code int) {
	rw.StatusCode = code
}

// TestDoubleFault checks that, if there is an error serializing a JSON
// response, it doesn't actually panic the process.
func TestDoubleFault(t *testing.T) {
	backend := memory.New()
	namespace, err := backend.Namespace("")
	if !assert.NoError(t, err) {
		return
	}
	_, err = namespace.SetWorkSpec(map[string]interface{}{
		"name": "spec",
	})
	if !assert.NoError(t, err) {
		return
	}

	router := NewRouter(backend)
	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: "/namespace/-/work_spec/spec",
		},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Close:      true,
		Host:       "localhost",
	}
	resp := &failResponseWriter{}
	router.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
