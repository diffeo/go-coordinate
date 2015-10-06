package coordinate

import (
	"github.com/mitchellh/mapstructure"
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
	if err == nil {
		err = decoder.Decode(workSpecDict)
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

// ExtractWorkUnitOutput coerces the "output" key from a work unit into
// a map of new work units.  The resulting map is nil if output cannot
// be coerced, or else is a map from work unit key to data dictionary.
// Backends should call this when an attempt is successfully finished
// to get new work units to create, if the work spec's metadata's
// NextWorkSpec field is non-empty.
func ExtractWorkUnitOutput(output interface{}) map[string]map[string]interface{} {
	var newUnits map[string]map[string]interface{}

	// Can we decode it as a map?
	config := mapstructure.DecoderConfig{Result: &newUnits}
	decoder, err := mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(output)
	}
	if err == nil {
		return newUnits
	}

	// Otherwise assume a format failure.  Try to decode it as a
	// string list instead.
	var list []string
	config = mapstructure.DecoderConfig{Result: &list}
	decoder, err = mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(output)
	}
	if err == nil {
		// Each of the items in list is the name of a work
		// unit and the datas are empty
		newUnits = make(map[string]map[string]interface{})
		for _, key := range list {
			newUnits[key] = map[string]interface{}{}
		}
		return newUnits
	}

	// Failing that, maybe it's a list of byte strings.
	var blist [][]byte
	config = mapstructure.DecoderConfig{Result: &blist}
	decoder, err = mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(output)
	}
	if err == nil {
		newUnits = make(map[string]map[string]interface{})
		for _, key := range blist {
			newUnits[string(key)] = map[string]interface{}{}
		}
		return newUnits
	}

	// Otherwise, we don't know how to decode it
	return nil
}
