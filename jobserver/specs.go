package jobserver

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
	MaxRunning int `mapstructure:"max_running"`
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
		meta     coordinate.WorkSpecMeta
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

// Now returns the current Unix time as seen by the server.  It can be
// used as a simple aliveness test for the server.
func (jobs *JobServer) Now() (int64, error) {
	return time.Now().Unix(), nil
}
