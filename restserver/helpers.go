// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

// This file contains various HTTP-related helpers.  I sort of suspect
// most of them belong in some sort of standard library I haven't
// immediately found.

import (
	"fmt"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
	"net/url"
	"strings"
)

type urlBuilder struct {
	Router *mux.Router
	Params []string
	Error  error
}

func buildURLs(router *mux.Router, params ...string) *urlBuilder {
	// Encode all of the values in params
	for i, value := range params {
		if i%2 == 1 {
			params[i] = restdata.MaybeEncodeName(value)
		}
	}
	return &urlBuilder{Router: router, Params: params}
}

func (u *urlBuilder) Route(route string) *mux.Route {
	if u.Error != nil {
		return nil
	}
	r := u.Router.Get(route)
	if r == nil {
		u.Error = fmt.Errorf("No such route %q", route)
	}
	return r
}

func (u *urlBuilder) URL(out *string, route string) *urlBuilder {
	var r *mux.Route
	var url *url.URL
	if u.Error == nil {
		r = u.Route(route)
	}
	if u.Error == nil {
		url, u.Error = r.URL(u.Params...)
	}
	if u.Error == nil {
		*out = url.String()
	}
	return u
}

func (u *urlBuilder) Template(out *string, route, param string) *urlBuilder {
	var r *mux.Route
	var url *url.URL
	if u.Error == nil {
		r = u.Route(route)
	}
	if u.Error == nil {
		params := append([]string{param, "---"}, u.Params...)
		url, u.Error = r.URL(params...)
	}
	if u.Error == nil {
		*out = strings.Replace(url.String(), "---", "{"+param+"}", 1)
	}
	return u
}
