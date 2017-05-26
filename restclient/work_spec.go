// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restclient

import (
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/restdata"
)

type workSpec struct {
	resource
	Representation restdata.WorkSpec
}

func workSpecFromURL(parent *resource, path string) (*workSpec, error) {
	var spec workSpec
	var err error
	spec.URL, err = parent.Template(path, map[string]interface{}{})
	if err == nil {
		err = spec.Refresh()
	}
	return &spec, err
}

func (spec *workSpec) Refresh() error {
	spec.Representation = restdata.WorkSpec{}
	return spec.Get(&spec.Representation)
}

func (spec *workSpec) Name() string {
	return spec.Representation.Name
}

func (spec *workSpec) Data() (map[string]interface{}, error) {
	err := spec.Refresh()
	if err == nil {
		return spec.Representation.Data, nil
	}
	return nil, err
}

func (spec *workSpec) SetData(data map[string]interface{}) error {
	repr := restdata.WorkSpec{Data: data}
	return spec.Put(repr, nil)
}

func (spec *workSpec) Meta(withCounts bool) (meta coordinate.WorkSpecMeta, err error) {
	err = spec.GetFrom(spec.Representation.MetaURL, map[string]interface{}{"counts": withCounts}, &meta)
	return
}

func (spec *workSpec) SetMeta(meta coordinate.WorkSpecMeta) error {
	return spec.PutTo(spec.Representation.MetaURL, map[string]interface{}{}, meta, nil)
}

func (spec *workSpec) AddWorkUnit(name string, data map[string]interface{}, meta coordinate.WorkUnitMeta) (coordinate.WorkUnit, error) {
	repr := restdata.WorkUnit{}
	repr.Name = name
	repr.Data = data
	repr.Meta = &meta

	unit := workUnit{workSpec: spec}
	err := spec.PostTo(spec.Representation.WorkUnitsURL, map[string]interface{}{}, repr, &unit.Representation)
	if err == nil {
		unit.URL, err = spec.Template(unit.Representation.URL, map[string]interface{}{})
	}
	if err == nil {
		return &unit, nil
	}
	return nil, err
}

func (spec *workSpec) WorkUnit(name string) (coordinate.WorkUnit, error) {
	unit := workUnit{workSpec: spec}
	var err error
	unit.URL, err = spec.Template(spec.Representation.WorkUnitURL, map[string]interface{}{"unit": name})
	if err == nil {
		err = unit.Refresh()
	}
	return &unit, err
}

func queryToParams(q coordinate.WorkUnitQuery) map[string]interface{} {
	result := make(map[string]interface{})
	if q.Names != nil {
		names := make([]interface{}, len(q.Names))
		for i, name := range q.Names {
			names[i] = name
		}
		result["name"] = names
	}
	if q.Statuses != nil {
		statuses := make([]interface{}, len(q.Statuses))
		for i, status := range q.Statuses {
			s, err := status.MarshalJSON()
			if err == nil {
				// s should be a byte string whose first
				// and last bytes are ASCII double quote
				statuses[i] = string(s)[1 : len(s)-1]
			} else {
				statuses[i] = status
			}
		}
		result["status"] = statuses
	}
	if q.PreviousName != "" {
		result["previous"] = q.PreviousName
	}
	if q.Limit != 0 {
		result["limit"] = q.Limit
	}
	return result
}

func (spec *workSpec) WorkUnits(q coordinate.WorkUnitQuery) (map[string]coordinate.WorkUnit, error) {
	params := queryToParams(q)
	var repr restdata.WorkUnitList
	err := spec.GetFrom(spec.Representation.WorkUnitQueryURL, params, &repr)
	if err == nil {
		units := make(map[string]coordinate.WorkUnit)
		for _, rUnit := range repr.WorkUnits {
			unit, err := workUnitFromURL(&spec.resource, rUnit.URL, spec)
			if err != nil {
				return nil, err
			}
			err = unit.Refresh()
			if err != nil {
				return nil, err
			}
			units[unit.Name()] = unit
		}
		return units, nil
	}
	return nil, err
}

func (spec *workSpec) CountWorkUnitStatus() (map[coordinate.WorkUnitStatus]int, error) {
	result := make(map[coordinate.WorkUnitStatus]int)
	err := spec.GetFrom(spec.Representation.WorkUnitCountsURL, map[string]interface{}{}, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (spec *workSpec) SetWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	params := queryToParams(q)
	repr := restdata.WorkUnit{Meta: &coordinate.WorkUnitMeta{
		Priority: priority,
	}}
	return spec.PostTo(spec.Representation.WorkUnitChangeURL, params, repr, nil)
}

func (spec *workSpec) AdjustWorkUnitPriorities(q coordinate.WorkUnitQuery, priority float64) error {
	params := queryToParams(q)
	repr := restdata.WorkUnit{Meta: &coordinate.WorkUnitMeta{
		Priority: priority,
	}}
	return spec.PostTo(spec.Representation.WorkUnitAdjustURL, params, repr, nil)
}

func (spec *workSpec) DeleteWorkUnits(q coordinate.WorkUnitQuery) (int, error) {
	params := queryToParams(q)
	var repr restdata.WorkUnitDeleted
	err := spec.DeleteAt(spec.Representation.WorkUnitQueryURL, params, &repr)
	if err == nil {
		return repr.Deleted, nil
	}
	return 0, err
}

func (spec *workSpec) Summarize() (coordinate.Summary, error) {
	var summary coordinate.Summary
	err := spec.GetFrom(spec.Representation.SummaryURL, nil, &summary)
	return summary, err
}
