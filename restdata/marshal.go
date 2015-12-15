// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restdata

import (
	"encoding/base64"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/ugorji/go/codec"
	"io"
	"mime"
	"reflect"
)

// Decode tries to decode a restdata object from a reader, such as an
// HTTP request or response.  out must be a pointer type.
func Decode(contentType string, r io.Reader, out interface{}) error {
	if contentType == "" {
		// RFC 7231 section 3.1.1.5
		// We could also consider http.DetectContentType()
		contentType = "application/octet-stream"
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return err
	}

	// Promote to more specific types
	switch mediaType {
	case "text/json", "application/json", JSONMediaType, V1JSONMediaType:
		mediaType = V1JSONMediaType

	default:
		return ErrUnsupportedMediaType{Type: mediaType}
	}

	// Actually decode the object based on the selected type.
	switch mediaType {
	case V1JSONMediaType:
		// (We will be happy we picked this library if we do
		// CBOR over the wire; it is also used for the
		// CBOR-RPC Python compatibility interface)
		json := &codec.JsonHandle{}
		decoder := codec.NewDecoder(r, json)
		err = decoder.Decode(out)
	default:
		err = ErrUnsupportedMediaType{Type: mediaType}
	}
	return err
}

// needsCBOREncoding decides whether an object needs to be encoded as
// CBOR.  It does iff any of its embedded objects are one of the types
// with special CBOR round-trip handling; these are cborrpc.PythonTuple
// and uuid.UUID.  If this returns false, the object can be safely
// round-tripped as JSON, to the best of our knowledge.
func needsCBOREncoding(v reflect.Value) bool {
	// Decide we need encoding for any type with the correct local name
	switch v.Type().Name() {
	case "PythonTuple", "UUID":
		return true
	}

	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		// needs encoding if any embedded value does
		for i := 0; i < v.Len(); i++ {
			if needsCBOREncoding(v.Index(i)) {
				return true
			}
		}
		return false

	case reflect.Map:
		// needs encoding if any key or value does
		for _, key := range v.MapKeys() {
			if needsCBOREncoding(key) {
				return true
			}
			if needsCBOREncoding(v.MapIndex(key)) {
				return true
			}
		}
		return false

	case reflect.Interface, reflect.Ptr:
		// needs encoding if its target does
		vv := v.Elem()
		if vv.IsValid() {
			return needsCBOREncoding(vv)
		}
		return false

	case reflect.Struct:
		// needs encoding if any field does
		for i := 0; i < v.NumField(); i++ {
			if needsCBOREncoding(v.Field(i)) {
				return true
			}
		}
		return false
	}

	// anything else either can be passed through as is,
	// or can't be passed through at all
	return false
}

// MarshalJSON returns a JSON representation of a data dictionary.
// If any of the dictionary's embedded values is a cborrpc.PythonTuple
// or a uuid.UUID, returns a base64-encoded CBOR string; otherwise
// returns a normal JSON object.
func (d DataDict) MarshalJSON() (out []byte, err error) {
	var v interface{}
	if needsCBOREncoding(reflect.ValueOf(d)) {
		// Do CBOR encoding to a byte array
		var intermediate []byte
		cborHandle := &codec.CborHandle{}
		err = cborrpc.SetExts(cborHandle)
		if err != nil {
			return nil, err
		}
		encoder := codec.NewEncoderBytes(&intermediate, cborHandle)
		err = encoder.Encode(map[string]interface{}(d))
		if err != nil {
			return nil, err
		}

		// base64 encode that byte array
		s := base64.StdEncoding.EncodeToString(intermediate)

		// Then we will JSON encode that string
		v = s
	} else {
		// We will JSON encode the base object
		v = map[string]interface{}(d)
	}
	codecHandle := &codec.JsonHandle{}
	encoder := codec.NewEncoderBytes(&out, codecHandle)
	err = encoder.Encode(v)
	return
}

// UnmarshalJSON converts a byte array back into a data dictionary.
// If it is a string, it should be base64-encoded CBOR.  If it is
// an object it is decoded normally.
func (d *DataDict) UnmarshalJSON(in []byte) error {
	jsonHandle := &codec.JsonHandle{}
	var h codec.Handle
	var b []byte
	if len(in) > 0 && in[0] == '"' {
		// This is a string.  Decode it from JSON...
		var s string
		decoder := codec.NewDecoderBytes(in, jsonHandle)
		err := decoder.Decode(&s)
		if err != nil {
			return err
		}

		// ...base64 decode it...
		b, err = base64.StdEncoding.DecodeString(s)
		if err != nil {
			return err
		}

		// ...and having gotten that byte string back, we will go
		// on to decode it as CBOR.
		cborHandle := codec.CborHandle{}
		err = cborrpc.SetExts(&cborHandle)
		if err != nil {
			return err
		}
		h = &cborHandle
	} else {
		// This is not a string and we will decode it as straight
		// JSON.
		h = jsonHandle
		b = in
	}
	decoder := codec.NewDecoderBytes(b, h)
	return decoder.Decode((*map[string]interface{})(d))
}
