package cborrpc

import (
	"errors"
	"github.com/mitchellh/mapstructure"
	"reflect"
)

// CreateParamList tries to match a CBOR-RPC parameter list to a specific
// callable's parameter list.  funcv is the reflected method to eventually
// call, and params is the list of parameters from the CBOR-RPC request.
// On success, the return value is a list of parameter values that can be
// passed to funcv.Call().
func CreateParamList(funcv reflect.Value, params []interface{}) ([]reflect.Value, error) {
	funct := funcv.Type()
	numParams := funct.NumIn()
	if len(params) != numParams {
		return nil, errors.New("wrong number of parameters")
	}
	results := make([]reflect.Value, numParams)
	for i := 0; i < numParams; i++ {
		paramType := funct.In(i)
		paramValue := reflect.New(paramType)
		param := paramValue.Interface()
		config := mapstructure.DecoderConfig{
			DecodeHook: DecodeBytesAsString,
			Result:     param,
		}
		decoder, err := mapstructure.NewDecoder(&config)
		if err != nil {
			return nil, err
		}
		err = decoder.Decode(params[i])
		if err != nil {
			return nil, err
		}
		results[i] = paramValue.Elem()
	}
	return results, nil
}

// SloppyString converts a string or []byte to a string, or returns nil.
func SloppyString(obj interface{}) *string {
	switch str := obj.(type) {
	case string:
		return &str
	case []byte:
		s := string(str)
		return &s
	default:
		return nil
	}
}

// DecodeBytesAsString is a mapstructure decode hook that accepts a
// byte slice where a string is expected.
func DecodeBytesAsString(from, to reflect.Type, data interface{}) (interface{}, error) {
	if to.Kind() == reflect.String && from.Kind() == reflect.Slice && from.Elem().Kind() == reflect.Uint8 {
		return string(data.([]uint8)), nil
	}
	return data, nil
}

// StringKeyedMap tries to convert an arbitrary object to a string-keyed
// map.  If this fails (because obj isn't a map or because any of its keys
// aren't strings) returns nil without further explanation.
func StringKeyedMap(obj interface{}) map[string]interface{} {
	objAsMap, ok := obj.(map[interface{}]interface{})
	if !ok {
		// not a map
		return nil
	}

	result := make(map[string]interface{})
	for key, value := range objAsMap {
		var keyAsString string
		keyAsString, ok = key.(string)
		if !ok {
			// some key isn't a string
			return nil
		}
		result[keyAsString] = value
	}
	return result
}

// StringList tries to convert an arbitrary object to a list of strings.
// If this fails, returns nil without further explanation.
func StringList(obj interface{}) []string {
	// First get a list of thingies
	var listI []interface{}
	switch listO := obj.(type) {
	case PythonTuple:
		listI = listO.Items
	case []interface{}:
		listI = listO
	default:
		return nil
	}

	// Now go through the list and cast each to string
	var result []string
	result = make([]string, 0, len(listI))
	for _, stringish := range listI {
		stringP := SloppyString(stringish)
		if stringP == nil {
			return nil
		}
		result = append(result, *stringP)
	}
	return result
}
