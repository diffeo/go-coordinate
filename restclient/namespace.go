// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

import (
	"errors"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
)

type namespace struct {
	resource
	Representation restdata.Namespace
}

func (ns *namespace) Refresh() error {
	ns.Representation = restdata.Namespace{}
	return ns.Get(&ns.Representation)
}

func (ns *namespace) Name() string {
	return ns.Representation.Name
}

func (ns *namespace) Destroy() error {
	return ns.Delete()
}

func (ns *namespace) makeWorkSpec(name string) (spec *workSpec, err error) {
	spec = &workSpec{}
	spec.URL, err = ns.Template(ns.Representation.WorkSpecURL, map[string]interface{}{"spec": name})
	if err == nil {
		err = spec.Refresh()
	}
	return
}

func (ns *namespace) SetWorkSpec(data map[string]interface{}) (coordinate.WorkSpec, error) {
	var (
		err      error
		reqdata  restdata.WorkSpec
		respdata restdata.WorkSpecShort
		spec     *workSpec
	)
	reqdata = restdata.WorkSpec{Data: data}
	spec = &workSpec{}
	if err == nil {
		err = ns.PostTo(ns.Representation.WorkSpecsURL, map[string]interface{}{}, reqdata, &respdata)
	}
	if err == nil {
		spec.URL, err = ns.Template(respdata.URL, map[string]interface{}{})
	}
	if err == nil {
		err = spec.Refresh()
	}
	if err == nil {
		return spec, nil
	}
	return nil, err
}

func (ns *namespace) WorkSpec(name string) (coordinate.WorkSpec, error) {
	spec, err := ns.makeWorkSpec(name)
	if err == nil {
		return spec, nil
	}
	if http, isHTTP := err.(ErrorHTTP); isHTTP {
		if http.Response.StatusCode == 404 {
			err = coordinate.ErrNoSuchWorkSpec{Name: name}
		}
	}
	return nil, err
}

func (ns *namespace) DestroyWorkSpec(name string) error {
	spec, err := ns.makeWorkSpec(name)
	if err == nil {
		err = spec.Delete()
	}
	return err
}

func (ns *namespace) WorkSpecNames() ([]string, error) {
	repr := restdata.WorkSpecList{}
	err := ns.GetFrom(ns.Representation.WorkSpecsURL, map[string]interface{}{}, &repr)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(repr.WorkSpecs))
	for i, spec := range repr.WorkSpecs {
		result[i] = spec.Name
	}
	return result, nil
}

func (ns *namespace) Worker(name string) (coordinate.Worker, error) {
	var w worker
	var err error
	w.URL, err = ns.Template(ns.Representation.WorkerURL, map[string]interface{}{"worker": name})
	if err == nil {
		err = w.Refresh()
	}
	return &w, err
}

func (ns *namespace) Workers() (map[string]coordinate.Worker, error) {
	return nil, errors.New("not implemented")
}
