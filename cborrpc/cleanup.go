package cborrpc

import "errors"
import "fmt"
import "reflect"

// CreateParamList tries to match a CBOR-RPC parameter list to a specific
// callable's parameter list.  funcv is the reflected method to eventually
// call, and params is the list of parameters from the CBOR-RPC request.
// On success, the return value is a list of parameter values that can be
// passed to funcv.Call().
//
// Parameters are fixed up with FixUpType().
func CreateParamList(funcv reflect.Value, params []interface{}) ([]reflect.Value, error) {
	funct := funcv.Type()
	numParams := funct.NumIn()
	if len(params) != numParams {
		return nil, errors.New("wrong number of parameters")
	}
	results := make([]reflect.Value, 0, numParams)
	for i := 0; i < numParams; i++ {
		param := reflect.ValueOf(params[i])
		param, err := FixUpType(funct.In(i), param)
		if err != nil {
			return nil, err
		}
		results = append(results, param)
	}
	return results, nil
}

// FixUpType tries to convert a value into an expected type.  This
// conversion works as follows:
//
// * If a parameter is an interface{}, it is passed on as-is.
// * If a parameter is a string, a []byte value is cast to string.
// * If a parameter is a map[string]interface{}, and the value is a
//   map, then all keys in the map are checked to be string or []byte or
//   not, and converted to string if possible.
func FixUpType(expected reflect.Type, actual reflect.Value) (reflect.Value, error) {
	actualT := actual.Type()
	// In practice we are probably getting passed an interface{},
	// deal with the concrete value underneath it
	if actualT.Kind() == reflect.Interface {
		actual = actual.Elem()
		actualT = actual.Type()
	}
	if actualT == expected {
		return actual, nil
	} else if expected.Kind() == reflect.Interface {
		// Anything goes, if acceptable
		if actualT.Implements(expected) {
			return actual, nil
		}
	} else if expected.Kind() == reflect.String {
		// Can convert []byte to string, but nothing else
		if actualT.Kind() == reflect.Slice &&
			actualT.Elem().Kind() == reflect.Uint8 {
			bytes := actual.Bytes()
			str := string(bytes)
			return reflect.ValueOf(str), nil
		}
	} else if expected.Kind() == reflect.Map {
		// Can convert map to map, must convert all keys and
		// values too
		if actualT.Kind() == reflect.Map {
			result := reflect.MakeMap(expected)
			var err error
			var key2, value2 reflect.Value
			for _, keyV := range actual.MapKeys() {
				valueV := actual.MapIndex(keyV)
				if err == nil {
					key2, err = FixUpType(expected.Key(), keyV)
				}
				if err == nil {
					value2, err = FixUpType(expected.Elem(), valueV)
				}
				if err == nil {
					result.SetMapIndex(key2, value2)
				}
			}
			if err == nil {
				return result, nil
			}
		}
	} else if expected.Kind() == reflect.Slice {
		// Can convert slice to slice, must convert all elements too
		if actualT.Kind() == reflect.Slice {
			elemT := expected.Elem()
			length := actual.Len()
			result := reflect.MakeSlice(expected, length, length)
			var err error
			for i := 0; i < length && err == nil; i++ {
				item := actual.Index(i)
				newItem, err := FixUpType(elemT, item)
				if err == nil {
					result.Index(i).Set(newItem)
				}
			}
			if err == nil {
				return result, nil
			}
		}
	}
	return reflect.ValueOf(nil), fmt.Errorf("cannot convert %v (%v) to %v", actual, actualT, expected)
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
	case PythonTuple: listI = listO.Items
	case []interface{}: listI = listO
	default: return nil
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
