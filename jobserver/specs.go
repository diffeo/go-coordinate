package jobserver

import "errors"
import "github.com/dmaze/goordinate/cborrpc"
import "github.com/dmaze/goordinate/coordinate"
import "github.com/mitchellh/mapstructure"
import "time"

// ListWorkSpecsOptions contains options to the ListWorkSpecs call.
type ListWorkSpecsOptions struct {
	// Start indicates the name of the first work spec to emit.
	// Empty string means start at the beginning.
	Start string

	// Limit indicates the maximum number of work specs to emit.
	Limit int
}

// ListWorkSpecs retrieves a list of all work specs in the server
// namespace.  It does not expect to return an error.
func (jobs *JobServer) ListWorkSpecs(options map[string]interface{}) (result []map[string]interface{}, next string, err error) {
	var names []string
	lwsOptions := ListWorkSpecsOptions{
		Limit: 1000,
	}
	err = decode(&lwsOptions, options)
	if err == nil {
		names, err = jobs.Namespace.WorkSpecNames()
	}
	if err == nil {
		result = make([]map[string]interface{}, len(names))
		for i, name := range names {
			if name < lwsOptions.Start {
				continue
			}
			if len(result) >= lwsOptions.Limit {
				next = name
				break
			}
			var data map[string]interface{}
			var workSpec coordinate.WorkSpec
			if err == nil {
				workSpec, err = jobs.Namespace.WorkSpec(name)
			}
			if err == nil {
				data, err = workSpec.Data()
			}
			if err == nil {
				result[i] = data
			}
		}
	}
	if err != nil {
		result = nil
		next = ""
	}
	return
}

// SetWorkSpec creates or updates a work spec from a map.  The map
// must contain a key "name" with a string value which indicates which
// work spec is being modified.  Other keys may have meaning and
// specific format requirements as well.
func (jobs *JobServer) SetWorkSpec(specMap map[string]interface{}) (bool, string, error) {
	// If anything in the spec is string *valued* set that now
	for key, value := range specMap {
		bytes, ok := value.([]byte)
		if ok {
			specMap[key] = string(bytes)
		}
	}
	spec, err := jobs.Namespace.SetWorkSpec(specMap)
	return (spec != nil), "", err
}

// DelWorkSpec deletes a single work spec.
func (jobs *JobServer) DelWorkSpec(workSpecName string) (bool, string, error) {
	err := jobs.Namespace.DestroyWorkSpec(workSpecName)
	return err == nil, "", err
}

// Clear deletes every work spec.  Call this with caution.  Returns
// the number of work specs deleted.
func (jobs *JobServer) Clear() (count int, err error) {
	// NB: it is tempting to DestroyNamespace() to much the same effect
	var names []string
	names, err = jobs.Namespace.WorkSpecNames()
	if err != nil {
		return
	}
	for _, name := range names {
		err = jobs.Namespace.DestroyWorkSpec(name)
		if err != nil {
			// Ignore concurrent deletes
			if _, missing := err.(coordinate.ErrNoSuchWorkSpec); !missing {
				return
			}
		}
	}
	count = len(names)
	return
}

// GetWorkSpec retrieves the definition of a work spec.  If the named
// work spec does not exist, returns nil (not an error).
func (jobs *JobServer) GetWorkSpec(workSpecName string) (data map[string]interface{}, err error) {
	var spec coordinate.WorkSpec
	spec, err = jobs.Namespace.WorkSpec(workSpecName)
	if err == nil {
		data, err = spec.Data()
	}
	return
}

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

	// States provides a list of states to query on.  If this is
	// provided then only work units in one of the specified states
	// will be returned.
	//
	// TODO(dmaze): Python also allows single (int) status or tuple
	States []WorkUnitStatus

	// Start gives a starting point to iterate through the list of
	// work units.  If zero, return results starting at the first
	// work unit; otherwise skip this many.
	Start uint

	// Limit specifies the maximum number of work units to return.
	// Defaults to 1000.
	Limit uint
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
	if err == nil {
		err = decode(&gwuOptions, options)
	}
	if err == nil && gwuOptions.WorkUnitKeys != nil {
		// Fetch these specific keys, ignore all other options
		workUnits, err = spec.WorkUnits(gwuOptions.WorkUnitKeys)
	}
	if err == nil && gwuOptions.WorkUnitKeys == nil {
		// Fetch all units in specific states, or all units in
		// general
		if gwuOptions.States != nil {
			return nil, "", errors.New("GetWorkUnits by state not implemented yet")
		}
		workUnits, err = spec.AllWorkUnits(gwuOptions.Start, gwuOptions.Limit)
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
	var start uint
	for {
		workUnits, err = workSpec.AllWorkUnits(start, 1000)
		if err != nil {
			return nil, "", err
		}
		for _, workUnit := range workUnits {
			status, _, err := workUnitStatus(workUnit)
			if err != nil {
				return nil, "", err
			}
			result[status]++
		}
		if len(workUnits) == 0 {
			break
		} else {
			start += uint(len(workUnits))
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

// WorkSpecStatus is a value passed as a "status" option to
// ControlWorkSpec().
type WorkSpecStatus int

const (
	// Runnable indicates a work spec is not paused.
	Runnable WorkSpecStatus = 1

	// Paused indicates a work spec is paused.
	Paused WorkSpecStatus = 2
)

// ControlWorkSpecOptions defines the list of actions ControlWorkSpec
// can take.
type ControlWorkSpecOptions struct {
	// Continuous can change the "continuous" flag on a work spec.
	// If a work spec declares itself as "continuous", the
	// scheduler can generate empty work units for it if there is
	// no other work to do.  This flag can pause and resume the
	// generation of these artificial work units for work specs
	// that declare themselves continuous.  Trying to set this
	// flag for a work spec that does not declare itself
	// continuous is an error.
	Continuous bool

	// Status indicates whether or not the work spec is paused.
	Status WorkSpecStatus

	// Weight controls the relative scheduling weight of this work
	// spec.
	Weight int

	// Interval specifies the minimum time, in seconds, between
	// generating continuous work units.
	Interval float64

	// MaxRunning specifies the maximum number of work units that
	// can concurrently run for this work spec, across the entire
	// system.
	MaxRunning int
}

// ControlWorkSpec makes changes to a work spec that are not directly
// reflected in the work spec definition.  This allows work specs to
// be paused or to stop generating new continuous jobs.
// ControlWorkSpecOptions has a complete listing of what can be done.
func (jobs *JobServer) ControlWorkSpec(workSpecName string, options map[string]interface{}) (bool, string, error) {
	var (
		cwsOptions ControlWorkSpecOptions
		decoder    *mapstructure.Decoder
		err        error
		metadata   mapstructure.Metadata
		workSpec   coordinate.WorkSpec
		wsMeta     coordinate.WorkSpecMeta
	)

	workSpec, err = jobs.Namespace.WorkSpec(workSpecName)
	if err == nil {
		// We care a lot about "false" vs. not present for
		// these things.  Manually create the decoder.
		config := mapstructure.DecoderConfig{
			Result:   &cwsOptions,
			Metadata: &metadata,
		}
		decoder, err = mapstructure.NewDecoder(&config)
	}
	if err == nil {
		err = decoder.Decode(options)
	}

	// Get the existing metadata, then change it based on what
	// we got provided
	if err == nil {
		wsMeta, err = workSpec.Meta(false)
	}
	if err == nil {
		for _, key := range metadata.Keys {
			switch key {
			case "Continuous":
				wsMeta.Continuous = cwsOptions.Continuous
			case "Status":
				wsMeta.Paused = cwsOptions.Status == Paused
			case "Weight":
				wsMeta.Weight = cwsOptions.Weight
			case "Interval":
				wsMeta.Interval = time.Duration(cwsOptions.Interval) * time.Second
			case "MaxRunning":
				wsMeta.MaxRunning = cwsOptions.MaxRunning
			}
		}
	}
	if err == nil {
		err = workSpec.SetMeta(wsMeta)
	}
	return err == nil, "", err
}

// GetWorkSpecMeta returns a set of control options for a given work
// spec.  The returned map has the full set of keys that
// ControlWorkSpec() will accept.
func (jobs *JobServer) GetWorkSpecMeta(workSpecName string) (result map[string]interface{}, _ string, err error) {
	var (
		workSpec coordinate.WorkSpec
		meta coordinate.WorkSpecMeta
	)
	
	workSpec, err = jobs.Namespace.WorkSpec(workSpecName)
	if err == nil {
		meta, err = workSpec.Meta(false)
	}
	if err == nil {
		result = make(map[string]interface{})
		if meta.Paused {
			result["status"] = Paused
		} else {
			result["status"] = Runnable
		}
		result["continuous"] = meta.Continuous
		result["interval"] = meta.Interval.Seconds()
		result["max_running"] = meta.MaxRunning
		result["weight"] = meta.Weight
	}
	return
}
