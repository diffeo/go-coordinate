// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package postgres

import (
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/ugorji/go/codec"
)

// dictionary <-> binary encoders

func mapToBytes(in map[string]interface{}) (out []byte, err error) {
	cbor := new(codec.CborHandle)
	err = cborrpc.SetExts(cbor)
	if err != nil {
		return
	}
	encoder := codec.NewEncoderBytes(&out, cbor)
	err = encoder.Encode(in)
	return
}

func bytesToMap(in []byte) (out map[string]interface{}, err error) {
	cbor := new(codec.CborHandle)
	err = cborrpc.SetExts(cbor)
	if err != nil {
		return
	}
	decoder := codec.NewDecoderBytes(in, cbor)
	err = decoder.Decode(&out)
	return
}
