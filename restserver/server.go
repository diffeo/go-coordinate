// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
	"net/http"
)

// NewRouter creates a new HTTP handler that processes all Coordinate
// requests.  All Coordinate resources are under the URL path root,
// e.g. /v1/namespace/foo.  For more control over this setup, create
// a mux.Router and call PopulateRouter instead.
func NewRouter(c coordinate.Coordinate) http.Handler {
	r := mux.NewRouter()
	PopulateRouter(r, c)
	return r
}

// PopulateRouter adds Coordinate routes to an existing
// github.com/gorilla/mux router object.  This can be used, for
// instance, to place the Coordinate interface under a subpath:
//
//     import "github.com/diffeo/go-coordinate/memory"
//     import "github.com/gorilla/mux"
//     r := mux.Router()
//     s := r.PathPrefix("/coordinate").Subrouter()
//     c := memory.New()
//     PopulateRouter(s, c)
func PopulateRouter(r *mux.Router, c coordinate.Coordinate) {
	api := &restAPI{Coordinate: c, Router: r}
	api.PopulateRouter(r)
}

// restAPI holds the persistent state for the Coordinate REST API.
type restAPI struct {
	Coordinate coordinate.Coordinate
	Router     *mux.Router
}

// PopulateRouter adds all Coordinate URL paths to a router.
func (api *restAPI) PopulateRouter(r *mux.Router) {
	api.PopulateNamespace(r)
	r.Path("/").Name("root").Handler(&resourceHandler{
		Representation: restdata.RootData{},
		Context:        api.Context,
		Get:            api.RootDocument,
	})
}

func (api *restAPI) RootDocument(ctx *context) (interface{}, error) {
	resp := restdata.RootData{}
	err := buildURLs(api.Router).
		URL(&resp.NamespacesURL, "namespaces").
		Template(&resp.NamespaceURL, "namespace", "namespace").
		Error
	return resp, err
}
