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

	// MaxRunning specifies the maximum number of work units that
	// are allowed to be concurrently running, across all workers.
	// If zero, there is no limit.
	MaxRunning int

	// MaxGetwork specifies the maximum number of attempts that can
	// be returned from a single call to Worker.RequestAttempts().
	// If zero, there is no limit.
	MaxGetwork int

	// Then specifies the name of another work spec that runs
	// after this one.  On successful completion, if Then is a
	// non-empty string and the updated work unit data contains
	// "outputs", these will be translated into new work units in
	// the Then work spec.
	Then string

	// ThenPreempts specifies whether the scheduler will
	// unconditionally run work units in the Then work spec before
	// this one.  Future versions of the scheduler may ignore this
	// flag.  Defaults to true.
	ThenPreempts bool
}

// ExtractWorkSpecMeta fills in as much of a WorkSpecMeta object as
// possible based on information given in a work spec definition.
func ExtractWorkSpecMeta(workSpecDict map[string]interface{}) (name string, meta WorkSpecMeta, err error) {
	data := WorkSpecData{
		ThenPreempts: true,
	}
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
		meta.Interval = time.Duration(data.Interval) * time.Second
		meta.MaxRunning = data.MaxRunning
		meta.MaxAttemptsReturned = data.MaxGetwork
		meta.NextWorkSpecName = data.Then
		meta.NextWorkSpecPreempts = data.ThenPreempts
	}
	return
}
