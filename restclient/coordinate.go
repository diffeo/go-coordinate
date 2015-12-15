// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package restclient provides a Coordinate-compatible HTTP REST client
// that talks to the matching server in the "restserver" package.
//
// The server in github.com/diffeo/go-coordinate/cmd/coordinated can
// run a compatible REST server.  Call New() with the base URL of that
// service; for instance,
//
//     c, err := restclient.New("http://localhost:5980/")
package restclient

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"net/url"
)

// New creates a new Coordinate interface that speaks to an external
// REST server.
func New(baseURL string) (coordinate.Coordinate, error) {
	var (
		err error
		url *url.URL
		c   *restCoordinate
	)
	url, err = url.Parse(baseURL)
	if err == nil {
		c = &restCoordinate{
			resource: resource{URL: url},
		}
		err = c.Refresh()
	}

	if err != nil {
		return nil, err
	}
	return c, nil
}

type restCoordinate struct {
	resource
	Representation restdata.RootData
}

func (c *restCoordinate) Refresh() error {
	c.Representation = restdata.RootData{}
	return c.Get(&c.Representation)
}

func (c *restCoordinate) Namespace(name string) (coordinate.Namespace, error) {
	var err error
	ns := &namespace{}
	ns.URL, err = c.Template(c.Representation.NamespaceURL, map[string]interface{}{"namespace": name})
	if err == nil {
		err = ns.Refresh()
	}
	if err == nil {
		return ns, nil
	}
	return nil, err
}

func (c *restCoordinate) Namespaces() (map[string]coordinate.Namespace, error) {
	resp := restdata.NamespaceList{}
	err := c.GetFrom(c.Representation.NamespacesURL, map[string]interface{}{}, &resp)
	if err != nil {
		return nil, err
	}
	result := make(map[string]coordinate.Namespace)
	for _, nsR := range resp.Namespaces {
		ns := namespace{}
		ns.URL, err = c.URL.Parse(nsR.URL)
		if err != nil {
			return nil, err
		}
		err = ns.Refresh()
		if err != nil {
			return nil, err
		}
		result[ns.Name()] = &ns
	}
	return result, nil
}
