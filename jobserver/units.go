package jobserver

import "github.com/dmaze/goordinate/cborrpc"
import "errors"
import "reflect"
import "github.com/dmaze/goordinate/coordinate"
import "github.com/mitchellh/mapstructure"

type addWorkUnitItem struct {
	key      string
	data     map[string]interface{}
	metadata map[string]interface{}
	priority *int
}

// AddWorkUnits adds any number of work units to a work spec.  Each oy
// the work units is a cborrpc.PythonTuple or slice containing a
// string with the work unit key, a dictionary with the work unit
// data, and an optional dictionary with additional metadata.
func (jobs *JobServer) AddWorkUnits(workSpecName string, workUnitKvp []interface{}) (bool, string, error) {
	spec, err := jobs.Namespace.WorkSpec(workSpecName)
	if err != nil {
		return false, "", err
	}

	// Unmarshal the work unit list into a []addWorkUnitItem This
	// will try to mimic the Python version in terms of whether an
	// error or exception is returned, but there's no consistency
	// there.  Just note that the tests don't check the return
	// value and assume this succeeds.
	items := make([]addWorkUnitItem, 0, len(workUnitKvp))
	for _, kvp := range workUnitKvp {
		var kvpT cborrpc.PythonTuple
		var kvpL []interface{}
		var ok bool

		kvpT, ok = kvp.(cborrpc.PythonTuple)
		if ok {
			kvpL = kvpT.Items
		} else {
			kvpL, ok = kvp.([]interface{})
		}
		if !ok {
			return false, "", errors.New("work unit must be a list")
		}
		if len(kvpL) < 2 {
			return false, "", errors.New("too few parameters to work unit")
		}
		name := cborrpc.SloppyString(kvpL[0])
		if name == nil {
			return false, "", errors.New("work unit key must be a string")
		}
		data := cborrpc.StringKeyedMap(kvpL[1])
		if data == nil {
			return false, "", errors.New("work unit data must be a map")
		}
		var metadata map[string]interface{}
		if len(kvpL) > 2 {
			metadata = cborrpc.StringKeyedMap(kvpL[2])
			if metadata == nil {
				return false, "", errors.New("work unit metadata must be a map")
			}
		}
		var priority *int
		if len(kvpL) > 3 {
			pri, ok := kvpL[3].(int)
			if !ok {
				return false, "", errors.New("work unit priority must be an int")
			}
			priority = &pri
		}
		item := addWorkUnitItem{*name, data, metadata, priority}
		items = append(items, item)
	}

	// If we've gotten to here, then we've marshaled all of the
	// parameters into items.
	for _, item := range items {
		priority := item.priority
		var priValue int
		if priority == nil {
			priItem := item.metadata["priority"]
			if priItem != nil {
				priValue, ok := priItem.(int)
				if ok {
					priority = &priValue
				}
			}
		}
		if priority == nil {
			priValue = 0
			priority = &priValue
		}
		_, err = spec.AddWorkUnit(item.key, item.data, *priority)
		if err != nil {
			// Again, Python coordinate expects to never see
			// a failure here?
			return false, "", err
		}
	}
	return true, "", nil
}

// GetWorkUnitsOptions contains unmarshaled options for GetWorkUnits().
type GetWorkUnitsOptions struct {
	// WorkUnitKeys contains a list of work unit keys to retrieve.
	// If this option is supplied, all other options are ignored.
	WorkUnitKeys []string

	// State provides a list of states to query on.  If this is
	// provided then only work units in one of the specified states
	// will be returned.
	State []WorkUnitStatus

	// Start gives a starting point to iterate through the list of
	// work units.  It is the name of the last work unit returned
	// in the previous call to GetWorkUnits().  No work unit whose
	// name is lexicographically less than this will be returned.
	Start string

	// Limit specifies the maximum number of work units to return.
	// Defaults to 1000.
	Limit int
}

// gwuStateHook is a mapstructure decode hook that expands a single int
// or a PythonTuple into a slice of int (WorkUnitStatus).
func gwuStateHook(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
	// to must be []WorkUnitStatus
	if to.Kind() != reflect.Slice || to.Elem().Name() != "WorkUnitStatus" {
		return data, nil
	}
	// If from is a tuple, return its contents
	if tuple, ok := data.(cborrpc.PythonTuple); ok {
		return tuple.Items, nil
	}
	// If from is an int, box it
	if single, ok := data.(int); ok {
		return []WorkUnitStatus{WorkUnitStatus(single)}, nil
	}
	// Otherwise, hope we can deal normally
	return data, nil
}

// GetWorkUnits retrieves the keys and data dictionaries for some number
// of work units.  If options contains "work_unit_keys", those specific
// work units are retrieved; otherwise the work units are based on
// which of GetWorkUnitsOptions are present.
//
// On success, the return value is a slice of cborrpc.PythonTuple
// objects where each contains the work unit key as a string and the
// data dictionary.
func (jobs *JobServer) GetWorkUnits(workSpecName string, options map[string]interface{}) ([]interface{}, string, error) {
	var workUnits map[string]coordinate.WorkUnit
	gwuOptions := GetWorkUnitsOptions{
		Limit: 1000,
	}

	spec, err := jobs.Namespace.WorkSpec(workSpecName)
	var decoder *mapstructure.Decoder
	if err == nil {
		config := mapstructure.DecoderConfig{
			DecodeHook: gwuStateHook,
			Result:     &gwuOptions,
		}
		decoder, err = mapstructure.NewDecoder(&config)
	}
	if err == nil {
		err = decoder.Decode(options)
	}
	if err == nil {
		query := coordinate.WorkUnitQuery{
			Names: gwuOptions.WorkUnitKeys,
		}
		if gwuOptions.WorkUnitKeys == nil {
			query.PreviousName = gwuOptions.Start
			query.Limit = gwuOptions.Limit
		}
		if gwuOptions.WorkUnitKeys == nil && gwuOptions.State != nil {
			query.Statuses = make([]coordinate.WorkUnitStatus, len(gwuOptions.State))
			for i, state := range gwuOptions.State {
				query.Statuses[i], err = translateWorkUnitStatus(state)
				if err != nil {
					break
				}
			}
		}
		if err == nil {
			workUnits, err = spec.WorkUnits(query)
		}
	}
	if err != nil {
		return nil, "", err
	}
	// The marshalled result is a list of pairs of (key, data).
	var result []interface{}
	for name, unit := range workUnits {
		var data map[string]interface{}
		attempt, err := unit.ActiveAttempt()
		if err == nil && attempt != nil {
			data, err = attempt.Data()
		}
		if err == nil && data == nil {
			data, err = unit.Data()
		}
		if err != nil {
			return nil, "", err
		}
		tuple := cborrpc.PythonTuple{Items: []interface{}{name, data}}
		result = append(result, tuple)
	}
	return result, "", nil
}

// GetWorkUnitStatus returns a summary status of zero or more work
// units in a single work spec.  On success, the returned list of
// dictionaries corresponds one-to-one with workUnitKeys.  If there is
// no such work unit, nil is in the list; otherwise each map contains
// keys "status", "expiration", "worker_id", and "traceback".
func (jobs *JobServer) GetWorkUnitStatus(workSpecName string, workUnitKeys []string) ([]map[string]interface{}, string, error) {
	spec, err := jobs.Namespace.WorkSpec(workSpecName)
	if err != nil {
		return nil, "", err
	}

	result := make([]map[string]interface{}, len(workUnitKeys))
	for i, key := range workUnitKeys {
		workUnit, err := spec.WorkUnit(key)
		if err != nil {
			return nil, "", err
		} else if workUnit == nil {
			result[i] = nil
		} else {
			r := make(map[string]interface{})
			status, attempt, err := workUnitStatus(workUnit)
			if err != nil {
				return nil, "", err
			}
			r["status"] = status
			if attempt != nil {
				r["worker_id"] = attempt.Worker().Name()
			}
			if status == Pending && attempt != nil {
				expiration, err := attempt.ExpirationTime()
				if err != nil {
					return nil, "", err
				}
				r["expiration"] = expiration.Unix()
			}
			if status == Failed && attempt != nil {
				data, err := attempt.Data()
				if err != nil {
					return nil, "", err
				}
				if traceback := data["traceback"]; traceback != nil {
					r["traceback"] = traceback
				}
			}
			result[i] = r
		}
	}
	return result, "", nil
}

// CountWorkUnits returns the number of work units in each status for
// a given work spec.
func (jobs *JobServer) CountWorkUnits(workSpecName string) (map[WorkUnitStatus]int, string, error) {
	workSpec, err := jobs.Namespace.WorkSpec(workSpecName)
	if err != nil {
		return nil, "", err
	}

	// TODO(dmaze): This is a bad way to do this; it should be
	// boiled down to a single call in the API

	result := make(map[WorkUnitStatus]int)
	var workUnits map[string]coordinate.WorkUnit
	var prev string
	for {
		workUnits, err = workSpec.WorkUnits(coordinate.WorkUnitQuery{
			PreviousName: prev,
			Limit: 1000,
		})
		if err != nil {
			return nil, "", err
		}
		for name, workUnit := range workUnits {
			status, _, err := workUnitStatus(workUnit)
			if err != nil {
				return nil, "", err
			}
			result[status]++
			prev = name
		}
		if len(workUnits) == 0 {
			break
		}
	}
	return result, "", nil
}

// workUnitStatus extracts a summary of the status of a single work
// unit.  This produces its external coordinate status and the active
// attempt (if any) on success.
func workUnitStatus(workUnit coordinate.WorkUnit) (status WorkUnitStatus, attempt coordinate.Attempt, err error) {
	var attemptStatus coordinate.AttemptStatus
	attempt, err = workUnit.ActiveAttempt()
	if err == nil && attempt == nil {
		status = Available
		return
	}
	if err == nil {
		attemptStatus, err = attempt.Status()
	}
	if err == nil {
		switch attemptStatus {
		case coordinate.Pending:
			status = Pending
		case coordinate.Expired:
			status = Available
			attempt = nil
		case coordinate.Finished:
			status = Finished
		case coordinate.Failed:
			status = Failed
		case coordinate.Retryable:
			status = Available
			attempt = nil
		default:
			err = errors.New("unexpected attempt status")
		}
	}
	return
}

// DelWorkUnitsOptions specifies the options for DelWorkUnits.  The
// first of All, WorkUnitKeys, or State given defines the operation to
// perform.  If none of these are given, the zero value for this
// structure tells DelWorkUnits to do nothing.
type DelWorkUnitsOptions struct {
	// All, if set to true, directs DelWorkUnits to delete
	// all of the work units in its work spec.  If this is
	// provided, all other options are ignored.
	All bool

	// WorkUnitKeys, if provided, is a list of specific work unit
	// keys to delete.  If this is given and All is false, then
	// these specific work units are deleted; if State is also
	// given, then each work unit must be in that state to be
	// deleted.
	WorkUnitKeys []string

	// State, if provided, is one of the external Coordinate work
	// unit statuses, and all work units in this state are deleted.
	// If WorkUnitKeys is also provided then only those work units
	// will be deleted, and then only if in this state.
	State WorkUnitStatus
}

// DelWorkUnits deletes work units from an existing work spec.  If
// options is empty, this does nothing.  On success, returns the
// number of work units deleted.
func (jobs *JobServer) DelWorkUnits(workSpecName string, options map[string]interface{}) (int, string, error) {
	workSpec, err := jobs.Namespace.WorkSpec(workSpecName)
	var dwuOptions DelWorkUnitsOptions
	var status coordinate.WorkUnitStatus
	if err == nil {
		err = decode(&dwuOptions, options)
	}
	if err == nil && !dwuOptions.All {
		status, err = translateWorkUnitStatus(dwuOptions.State)
	}
	if err == nil {
		var query coordinate.WorkUnitQuery
		if !dwuOptions.All {
			if dwuOptions.WorkUnitKeys != nil {
				query.Names = dwuOptions.WorkUnitKeys
			} else if status != coordinate.AnyStatus {
				query.Statuses = []coordinate.WorkUnitStatus{status}
			}
		}
		err = workSpec.DeleteWorkUnits(query)
	}
	return 0, "", err
}

// Archive causes the system to clean up completed work units.  The
// system will keep up to a pre-specified limit of work units that
// have completed successfully, and will also remove work units that
// have completed successfully but are beyond a pre-specified age.
// The work units are deleted as in DelWorkUnits().  The return value
// is always nil.
//
// TODO(dmaze): Actually implement this.  This probably involves
// triggering a background task the system would need to do on its own
// in any case.  The observable effects of this are minimal, especially
// in a default/test configuration.
func (jobs *JobServer) Archive(options map[string]interface{}) (interface{}, error) {
	return nil, nil
}
