package jobserver

import "github.com/dmaze/goordinate/cborrpc"
import "github.com/dmaze/goordinate/coordinate"
import "github.com/mitchellh/mapstructure"
import "errors"

// decode is a helper that uses the mapstructure library to decode a
// string-keyed map into a structure.
func decode(result interface{}, options map[string]interface{}) error {
	config := mapstructure.DecoderConfig{
		DecodeHook: cborrpc.DecodeBytesAsString,
		Result: result,
	}
	decoder, err := mapstructure.NewDecoder(&config)
	if err == nil {
		err = decoder.Decode(options)
	}
	return err
}

// translateWorkUnitStatus converts a CBOR-RPC work unit status to a
// Go coordinate.WorkUnitStatus.  0 is not used in the CBOR-RPC API,
// and it is an easy default, so we use it to mean "unspecified" which
// usually translates to coordinate.AnyStatus.  Returns an error only
// if the status value is undefined (which includes the unused Python
// BLOCKED).
func translateWorkUnitStatus(status WorkUnitStatus) (coordinate.WorkUnitStatus, error) {
	switch status {
	case 0: return coordinate.AnyStatus, nil
	case Available: return coordinate.AvailableUnit, nil
	case Pending: return coordinate.PendingUnit, nil
	case Finished: return coordinate.FinishedUnit, nil
	case Failed: return coordinate.FailedUnit, nil
	default: return coordinate.AnyStatus, errors.New("invalid work unit status")
	}
}
