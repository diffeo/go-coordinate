package jobserver

import "github.com/mitchellh/mapstructure"

// decode is a helper that uses the mapstructure library to decode a
// string-keyed map into a structure.
func decode(result interface{}, options map[string]interface{}) error {
	config := mapstructure.DecoderConfig{Result: result}
	decoder, err := mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(options)
	}
	return err
}
