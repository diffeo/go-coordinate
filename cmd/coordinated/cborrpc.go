// Copyright 2015-2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/diffeo/go-coordinate/cborrpc"
	"github.com/diffeo/go-coordinate/coordinate"
	"github.com/diffeo/go-coordinate/jobserver"
	"github.com/ugorji/go/codec"
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
)

// ServeCBORRPC runs a CBOR-RPC server on the specified local address.
// This serves connections forever, and probably wants to be run in a
// goroutine.  Panics on any error in the initial setup or in accepting
// connections.
func ServeCBORRPC(
	coord coordinate.Coordinate,
	gConfig map[string]interface{},
	network, laddr string,
) {
	var (
		cbor      *codec.CborHandle
		err       error
		namespace coordinate.Namespace
		ln        net.Listener
		conn      net.Conn
		jobd      *jobserver.JobServer
	)

	cbor = new(codec.CborHandle)
	if err == nil {
		err = cborrpc.SetExts(cbor)
	}
	if err == nil {
		namespace, err = coord.Namespace("")
	}
	if err == nil {
		jobd = &jobserver.JobServer{
			Namespace:    namespace,
			GlobalConfig: gConfig,
			Clock:        clock.New(),
		}
		ln, err = net.Listen(network, laddr)
	}
	for err == nil {
		conn, err = ln.Accept()
		if err == nil {
			go handleConnection(conn, jobd, cbor)
		}
	}
	panic(err)
}

// Convert a "snake case" name, like 'foo_bar_baz', to a "camel case" name
// with its first letter capitalized, like 'FooBarBaz'.
func snakeToCamel(s string) string {
	words := strings.Split(s, "_")
	for n := range words {
		words[n] = strings.Title(words[n])
	}
	return strings.Join(words, "")
}

func handleConnection(conn net.Conn, jobd *jobserver.JobServer, cbor *codec.CborHandle) {
	defer conn.Close()

	jobdv := reflect.ValueOf(jobd)

	reader := bufio.NewReader(conn)
	decoder := codec.NewDecoder(reader, cbor)
	writer := bufio.NewWriter(conn)
	encoder := codec.NewEncoder(writer, cbor)

	for {
		var request cborrpc.Request
		err := decoder.Decode(&request)
		if err == io.EOF {
			return
		} else if err != nil {
			fmt.Printf("Error reading message: %v\n", err)
			return
		}
		response := doRequest(jobdv, request)
		err = encoder.Encode(response)
		if err != nil {
			fmt.Printf("Error encoding response: %v\n", err)
			return
		}
		err = writer.Flush()
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}

func doRequest(jobdv reflect.Value, request cborrpc.Request) (response cborrpc.Response) {
	response.ID = request.ID

	// If we panic in the middle of this, turn it into a response
	defer func() {
		if oops := recover(); oops != nil {
			buf := make([]byte, 65536)
			runtime.Stack(buf, false)
			fmt.Printf("%v\n%v\n", oops, string(buf))
			response.Error = fmt.Sprintf("%v", oops)
		}
	}()

	method := snakeToCamel(request.Method)
	var err error
	var params, returns []reflect.Value
	var returnsString, returnsError bool
	funcv := jobdv.MethodByName(method)
	if !funcv.IsValid() {
		err = fmt.Errorf("no such method %v", method)
	}
	if err == nil {
		funct := funcv.Type()
		numOut := funct.NumOut()
		if numOut > 0 {
			lastt := funct.Out(numOut - 1)
			returnsError = lastt.PkgPath() == "" && lastt.Name() == "error"
		}
		if numOut > 1 {
			secondt := funct.Out(numOut - 2)
			returnsString = secondt.PkgPath() == "" && secondt.Name() == "string"
		}

		// In theory, the wire format could have a map
		// of kwargs instead
		params, err = cborrpc.CreateParamList(funcv, request.Params)
	}
	if err == nil {
		// The wire format has an explicit error
		// return, which gets mapped to Python
		// Exception, plus most methods return a pair
		// of (response, error message).  Deal with
		// this on the Go side by having functions
		// return both kinds of errors (if
		// appropriate).
		returns = funcv.Call(params)
		if len(returns) == 0 {
			err = errors.New("empty return from method")
		} else if returnsError {
			errV := returns[len(returns)-1].Interface()
			if errV != nil {
				err = errV.(error)
			}
			returns = returns[0 : len(returns)-1]
		}
	}

	// If we are expecting to return a string message, and there
	// is no error, remap an empty string to nil
	if returnsString && len(returns) > 0 {
		msg := returns[len(returns)-1].String()
		if msg == "" {
			returns[len(returns)-1] = reflect.ValueOf(nil)
		}

		// If we got back a NoSuchWorkSpec error, report that
		// as a string (most RPC calls that take a work spec
		// parameter can produce this and return it as a string
		// error, not an exception)
		if err != nil {
			if nsws, ok := err.(coordinate.ErrNoSuchWorkSpec); ok {
				err = nil
				returns[len(returns)-1] = reflect.ValueOf(nsws.Error())
			}
		}
	}

	if err != nil {
		response.Error = err.Error()
	} else if len(returns) == 1 {
		response.Result = returns[0].Interface()
	} else {
		results := make([]interface{}, len(returns))
		for i, retval := range returns {
			if retval.IsValid() {
				results[i] = retval.Interface()
			}
		}
		response.Result = results
	}

	return
}
