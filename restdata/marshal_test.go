// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restdata

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"reflect"
	"testing"
)

func TestDataDictMarshal(t *testing.T) {
	tests := []struct {
		Object DataDict
		JSON   string
	}{
		{
			Object: DataDict{},
			JSON:   "{}",
		},
		{
			Object: DataDict{
				"key": "value",
			},
			JSON: "{\"key\":\"value\"}",
		},
		{
			Object: DataDict{
				"key": cborrpc.PythonTuple{Items: []interface{}{}},
			},
			// The encoded CBOR is
			// A1            101 00001 map of 1 item
			// 63 6B 65 79   011 00011  string len 3 "key"
			// D8 80         110 11000  tuple tag 128
			// 80            100 00000    list of 0 items
			JSON: "\"oWNrZXnYgIA=\"",
		},
	}
	for _, test := range tests {
		json, err := test.Object.MarshalJSON()
		if err != nil {
			t.Errorf("MarshalJSON(%+v) => error %+v",
				test.Object, err)
		} else if string(json) != test.JSON {
			t.Errorf("MarshalJSON(%+v) => %v, want %v",
				test.Object, string(json), test.JSON)
		}

		var obj DataDict
		err = (&obj).UnmarshalJSON([]byte(test.JSON))
		if err != nil {
			t.Errorf("UnmarshalJSON(%v) => error %+v",
				test.JSON, err)
		} else if !reflect.DeepEqual(obj, test.Object) {
			t.Errorf("UnmarshalJSON(%v) => %+v, want %+v",
				test.JSON, obj, test.Object)
		}
	}
}
