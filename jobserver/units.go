package jobserver

import (
	"errors"
	"github.com/dmaze/goordinate/cborrpc"
	"github.com/dmaze/goordinate/coordinate"
	"github.com/mitchellh/mapstructure"
	"math"
	"reflect"
)

// AddWorkUnitItem describes a single work unit to be added.  This is
// actually passed across the wire as a tuple of Key, Data, Metadata,
// and Priority, in that order, with later fields possibly missing.
type AddWorkUnitItem struct {
	// Key defines the name of the work unit.
	Key string

	// Data is the dictionary of per-work-unit data.
	Data map[string]interface{}

	// Metadata defines additional settings for this work unit.
	// The only recognized key is "priority", which is used only
	// if the Priority field is not set.
	Metadata map[string]interface{}

	// Priority defines a relative priority for this work unit.
	// Higher priority runs sooner.
	Priority float64
}

// unmarshalAddWorkUnitItem converts an arbitrary object (which really
// should be a cborpc.PythonTuple or a list) into an AddWorkUnitItem.
func unmarshalAddWorkUnitItem(obj interface{}) (result AddWorkUnitItem, err error) {
	var (
		decoder      *mapstructure.Decoder
		haveMetadata bool
		havePriority bool
		kvpList      []interface{}
		kvpMap       map[string]interface{}
		ok           bool
	)
	// obj must be a tuple (or a list)
	if kvpList, ok = cborrpc.Detuplify(obj); !ok {
		err = errors.New("work unit must be a list")
		return
	}
	// Turn that list into a string-keyed map
	if len(kvpList) < 2 {
		err = errors.New("too few parameters to work unit")
		return
	}
	kvpMap = make(map[string]interface{})
	kvpMap["key"] = kvpList[0]
	kvpMap["data"] = kvpList[1]
	if len(kvpList) >= 3 && kvpList[2] != nil {
		kvpMap["metadata"] = kvpList[2]
		haveMetadata = true
	}
	if len(kvpList) >= 4 && kvpList[3] != nil {
		kvpMap["priority"] = kvpList[3]
		havePriority = true
	}
	// Now we can invoke mapstructure
	config := mapstructure.DecoderConfig{
		DecodeHook: cborrpc.DecodeBytesAsString,
		Result:     &result,
	}
	decoder, err = mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(kvpMap)
	}
	if err == nil && haveMetadata && !havePriority {
		// See if the caller passed metadata["priority"]
		// instead of an explicit priority field.
		if priority, ok := result.Metadata["priority"]; ok {
			if result.Priority, ok = priority.(float64); !ok {
				err = errors.New("priority must be a number")
			}
		}
	}
	return
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

	// Unmarshal the work unit list into a []AddWorkUnitItem.
	// Fail now if any are invalid.
	items := make([]AddWorkUnitItem, len(workUnitKvp))
	for i, kvp := range workUnitKvp {
		items[i], err = unmarshalAddWorkUnitItem(kvp)
		if err != nil {
			return false, "", err
		}
	}

	// Now go through and add them all
	for _, item := range items {
		_, err = spec.AddWorkUnit(item.Key, item.Data, item.Priority)
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
	WorkUnitKeys []string `mapstructure:"work_unit_keys"`

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
			DecodeHook: mapstructure.ComposeDecodeHookFunc(gwuStateHook, cborrpc.DecodeBytesAsString),
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

// PrioritizeWorkUnitsOptions specifies which work units PrioritizeWorkUnits
// should adjust and how.
type PrioritizeWorkUnitsOptions struct {
	// WorkUnitKeys gives the names of the work units to reprioritize.
	// If not present, does nothing.
	WorkUnitKeys []string `mapstructure:"work_unit_keys"`

	// Priority sets an absolute priority.  If a NaN value, make a
	// change specified by Adjustment instead.
	Priority float64

	// Adjustment is added to the priorities of each of the work
	// units, if Priority is NaN.  If also a NaN value, do nothing.
	Adjustment float64
}

// PrioritizeWorkUnits changes the priorities of some number of work
// units.  The actual work units are in options["work_unit_keys"].  A
// higher priority results in the work units being scheduled sooner.
func (jobs *JobServer) PrioritizeWorkUnits(workSpecName string, options map[string]interface{}) (bool, string, error) {
	var (
		err      error
		query    coordinate.WorkUnitQuery
		workSpec coordinate.WorkSpec
	)
	pwuOptions := PrioritizeWorkUnitsOptions{
		Priority:   math.NaN(),
		Adjustment: math.NaN(),
	}
	workSpec, err = jobs.Namespace.WorkSpec(workSpecName)
	if err == nil {
		err = decode(&pwuOptions, options)
	}
	if err == nil && pwuOptions.WorkUnitKeys == nil {
		return false, "missing work_unit_keys", err
	}
	if err == nil {
		query.Names = pwuOptions.WorkUnitKeys
		if !math.IsNaN(pwuOptions.Priority) {
			err = workSpec.SetWorkUnitPriorities(query, pwuOptions.Priority)
		} else if !math.IsNaN(pwuOptions.Adjustment) {
			err = workSpec.AdjustWorkUnitPriorities(query, pwuOptions.Adjustment)
		}
	}
	return err == nil, "", err
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
			Limit:        1000,
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
	WorkUnitKeys []string `mapstructure:"work_unit_keys"`

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
	var (
		count      int
		dwuOptions DelWorkUnitsOptions
		status     coordinate.WorkUnitStatus
	)
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
		count, err = workSpec.DeleteWorkUnits(query)
	}
	return count, "", err
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
