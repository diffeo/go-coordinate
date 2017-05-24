// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restserver

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
	"github.com/gorilla/mux"
)

func (api *restAPI) fillNamespaceShort(namespace coordinate.Namespace, summary *restdata.NamespaceShort) error {
	summary.Name = namespace.Name()
	return buildURLs(api.Router, "namespace", summary.Name).
		URL(&summary.URL, "namespace").
		Error
}

func (api *restAPI) fillNamespace(namespace coordinate.Namespace, result *restdata.Namespace) error {
	err := api.fillNamespaceShort(namespace, &result.NamespaceShort)
	if err == nil {
		err = buildURLs(api.Router, "namespace", result.Name).
			URL(&result.SummaryURL, "namespaceSummary").
			URL(&result.WorkSpecsURL, "workSpecs").
			Template(&result.WorkSpecURL, "workSpec", "spec").
			URL(&result.WorkersURL, "workers").
			Template(&result.WorkerURL, "worker", "worker").
			Error
	}
	return err
}

// NamespaceList gets a list of all namespaces known in the system.
func (api *restAPI) NamespaceList(ctx *context) (interface{}, error) {
	namespaces, err := api.Coordinate.Namespaces()
	if err != nil {
		return nil, err
	}
	result := restdata.NamespaceList{}
	for _, ns := range namespaces {
		summary := restdata.NamespaceShort{}
		err = api.fillNamespaceShort(ns, &summary)
		if err != nil {
			return nil, err
		}
		result.Namespaces = append(result.Namespaces, summary)
	}
	return result, nil
}

// NamespacePost creates a new namespace, or retrieves a pointer to
// an existing one.
func (api *restAPI) NamespacePost(ctx *context, in interface{}) (interface{}, error) {
	req, valid := in.(restdata.NamespaceShort)
	if !valid {
		return nil, errUnmarshal
	}
	ns, err := api.Coordinate.Namespace(req.Name)
	if err != nil {
		return nil, err
	}
	// We will return "created", where the content is the full
	// namespace data
	result := restdata.Namespace{}
	err = api.fillNamespace(ns, &result)
	if err != nil {
		return nil, err
	}
	return responseCreated{
		Location: result.URL,
		Body:     result,
	}, nil
}

// NamespaceGet retrieves an existing namespace, or creates a new one.
func (api *restAPI) NamespaceGet(ctx *context) (interface{}, error) {
	// If we've gotten here, we're just returning ctx.Namespace
	result := restdata.Namespace{}
	err := api.fillNamespace(ctx.Namespace, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NamespaceDelete destroys an existing namespace.
func (api *restAPI) NamespaceDelete(ctx *context) (interface{}, error) {
	err := ctx.Namespace.Destroy()
	return nil, err
}

// NamespaceSummaryGet produces a summary for a namespace.
func (api *restAPI) NamespaceSummaryGet(ctx *context) (interface{}, error) {
	return ctx.Namespace.Summarize()
}

// PopulateNamespace adds namespace-specific routes to a router.
// r should be rooted at the root of the Coordinate URL tree, e.g. "/".
func (api *restAPI) PopulateNamespace(r *mux.Router) {
	r.Path("/namespace").Name("namespaces").Handler(&resourceHandler{
		Representation: restdata.NamespaceShort{},
		Context:        api.Context,
		Get:            api.NamespaceList,
		Post:           api.NamespacePost,
	})
	r.Path("/namespace/{namespace}").Name("namespace").Handler(&resourceHandler{
		Representation: restdata.Namespace{},
		Context:        api.Context,
		Get:            api.NamespaceGet,
		Delete:         api.NamespaceDelete,
	})
	r.Path("/namespace/{namespace}/summary").Name("namespaceSummary").Handler(&resourceHandler{
		Representation: coordinate.Summary{},
		Context:        api.Context,
		Get:            api.NamespaceSummaryGet,
	})
	sr := r.PathPrefix("/namespace/{namespace}").Subrouter()
	api.PopulateWorkSpec(sr)
	api.PopulateWorker(sr)
}
