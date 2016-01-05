// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

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

// DecodeBytesAsString is a mapstructure decode hook that accepts a
// byte slice where a string is expected.
func DecodeBytesAsString(from, to reflect.Type, data interface{}) (interface{}, error) {
	if to.Kind() == reflect.String && from.Kind() == reflect.Slice && from.Elem().Kind() == reflect.Uint8 {
		return string(data.([]uint8)), nil
	}
	return data, nil
}

// Detuplify removes a tuple wrapper.  If obj is a tuple, returns
// the contained slice.  If obj is a slice, returns it.  Otherwise
// returns failure.
func Detuplify(obj interface{}) ([]interface{}, bool) {
	switch t := obj.(type) {
	case PythonTuple:
		return t.Items, true
	case []interface{}:
		return t, true
	default:
		return nil, false
	}
}

// SloppyDetuplify turns any object into a slice.  If it is already a
// PythonTuple or a slice, returns the slice as Detuplify; otherwise
// packages up obj into a new slice.  This never fails.
func SloppyDetuplify(obj interface{}) []interface{} {
	if slice, ok := Detuplify(obj); ok {
		return slice
	}
	return []interface{}{obj}
}

// Destringify tries to turn any object into a string.  If it is a
// string or byte slice, returns the string and true; otherwise returns
// empty string and false.
func Destringify(obj interface{}) (string, bool) {
	switch s := obj.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}
