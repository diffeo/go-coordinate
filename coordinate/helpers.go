// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package coordinate

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strings"
	"time"
)

// WorkSpecData contains data that can be extracted from a work spec's
// data dictionary.  This is not used directly in the Coordinate API,
// but WorkSpec.SetData(), via ExtractWorkSpecMeta(), will attempt to
// get these values from a work spec dictionary.
type WorkSpecData struct {
	// Name of the work spec.
	Name string

	// Disabled indicates whether the work spec will start paused.
	// Defaults to false.
	Disabled bool

	// Continuous indicates whether the work spec expects to
	// receive generated work units.  The Coordinate system can
	// produce these work units if there is no other work to be
	// done for this work spec.  Defaults to false.
	Continuous bool

	// Interval specifies the minimum interval, in seconds, between
	// running generated work units for continuous work specs.
	Interval float64

	// Priority specifies an absolute priority for this work spec.
	// Work specs with higher priority will always run before
	// work specs with lower priority.  Defaults to 0.
	Priority int

	// Weight specifies the relative weight of this work spec.
	// Work specs with twice the weight will aim for twice as many
	// concurrently running work specs.  If this is zero, uses a
	// value derived from Nice; if both are use, uses 20.
	Weight int

	// Nice specifies the "niceness" of this work spec, as the Unix
	// nice(1) tool.  If Weight is zero, then it is set to 20 - Nice.
	Nice int

	// MinGb specifies the minimum amount of memory required to run.
	MinGb float64 `mapstructure:"min_gb"`

	// MaxRunning specifies the maximum number of work units that
	// are allowed to be concurrently running, across all workers.
	// If zero, there is no limit.
	MaxRunning int `mapstructure:"max_running"`

	// MaxGetwork specifies the maximum number of attempts that can
	// be returned from a single call to Worker.RequestAttempts().
	// If zero, there is no limit.
	MaxGetwork int `mapstructure:"max_getwork"`

	// Then specifies the name of another work spec that runs
	// after this one.  On successful completion, if Then is a
	// non-empty string and the updated work unit data contains
	// "outputs", these will be translated into new work units in
	// the Then work spec.
	Then string
}

// ExtractWorkSpecMeta fills in as much of a WorkSpecMeta object as
// possible based on information given in a work spec definition.
func ExtractWorkSpecMeta(workSpecDict map[string]interface{}) (name string, meta WorkSpecMeta, err error) {
	data := WorkSpecData{}
	config := mapstructure.DecoderConfig{Result: &data}
	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return
	}
	err = decoder.Decode(workSpecDict)
	if err != nil {
		// I hate checking for this specific message, but it's
		// the only way to detect this
		msError, ok := err.(*mapstructure.Error)
		if ok {
			for _, message := range msError.Errors {
				if strings.HasPrefix(message, "'Name' expected type 'string', got") {
					err = ErrBadWorkSpecName
				}
			}
		}
		return
	}
	if err == nil {
		if data.Name == "" {
			err = ErrNoWorkSpecName
		}
	}
	if err == nil {
		name = data.Name
		if data.Weight == 0 {
			data.Weight = 20 - data.Nice
		}
		if data.Weight <= 0 {
			data.Weight = 1
		}
		meta.Priority = data.Priority
		meta.Weight = data.Weight
		meta.Paused = data.Disabled
		meta.Continuous = data.Continuous
		meta.CanBeContinuous = data.Continuous
		meta.MinMemoryGb = data.MinGb
		meta.Interval = time.Duration(data.Interval) * time.Second
		meta.MaxRunning = data.MaxRunning
		meta.MaxAttemptsReturned = data.MaxGetwork
		meta.NextWorkSpecName = data.Then
	}
	return
}

// TODO(dmaze): YOU ARE HERE
//
// scp sends back
//
// u'output': [b'key',
//             (b'key', {}, {u'priority': 0L})]
//
// which is the usual awful mix of parts, including duplicating the key.
// We can't usefully decode this as anything other than a []interface{}
// and then try to pattern-match each item as either a plain string
// or a tuple (which can be either a 2- or 3-tuple).

// AddWorkUnitItem describes a single work unit to be added.  This is
// returned from ExtractWorkUnitOutput.  When it appears in a work
// unit's data "output" field, it is generally as a list or
// cborrpc.PythonTuple of the corresponding fields.
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

// ExtractWorkUnitOutput coerces the "output" key from a work unit into
// a map of new work units.  The resulting map is nil if output cannot
// be coerced, or else is a map from work unit key to data dictionary.
// Backends should call this when an attempt is successfully finished
// to get new work units to create, if the work spec's metadata's
// NextWorkSpec field is non-empty.
func ExtractWorkUnitOutput(output interface{}) map[string]AddWorkUnitItem {
	result := make(map[string]AddWorkUnitItem)

	// Can we decode it as a map?
	var newUnits map[string]map[string]interface{}
	config := mapstructure.DecoderConfig{Result: &newUnits}
	decoder, err := mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(output)
	}
	if err == nil {
		for name, data := range newUnits {
			result[name] = AddWorkUnitItem{Key: name, Data: data}
		}
		return result
	}

	// Otherwise try it as a list or tuple.  Try to get to a
	// reflect.Value that is a slice of something.
	var list reflect.Value
	if tuple, ok := output.(cborrpc.PythonTuple); ok {
		list = reflect.ValueOf(tuple.Items)
	} else {
		list = reflect.ValueOf(output)
		if list.Kind() != reflect.Slice {
			return result // not a list at all
		}
	}

	// Now run through the list
	for i := 0; i < list.Len(); i++ {
		item := list.Index(i).Interface()
		awuItem, err := ExtractAddWorkUnitItem(item)
		if err == nil {
			result[awuItem.Key] = awuItem
		}
	}
	return result
}

// ExtractAddWorkUnitItem converts an arbitrary object (which really
// should be a cborpc.PythonTuple or a list) into an AddWorkUnitItem.
func ExtractAddWorkUnitItem(obj interface{}) (result AddWorkUnitItem, err error) {
	var (
		decoder      *mapstructure.Decoder
		haveMetadata bool
		havePriority bool
		kvpList      []interface{}
		kvpMap       map[string]interface{}
		str          string
		bstr         []byte
		ok           bool
	)
	// If we got handed a string (or a byte string) turn it into
	// a work unit with no data
	if str, ok = obj.(string); ok {
		result.Key = str
		result.Data = make(map[string]interface{})
		return
	}
	if bstr, ok = obj.([]byte); ok {
		result.Key = string(bstr)
		result.Data = make(map[string]interface{})
		return
	}

	// Otherwise obj must be a tuple (or a list)
	if kvpList, ok = cborrpc.Detuplify(obj); !ok {
		err = ErrWorkUnitNotList
		return
	}
	// Turn that list into a string-keyed map
	if len(kvpList) < 2 {
		err = ErrWorkUnitTooShort
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
				err = ErrBadPriority
			}
		}
	}
	return
}
