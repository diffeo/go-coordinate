// Package goordinated provides a wire-compatible reimplementation of
// the Diffeo Coordinate daemon.  This is intended to be fully
// compatible with the existing Coordinate toolset at
// https://github.com/diffeo/coordinate.  This is purely a server-side
// daemon; it does not include application code or a worker
// implementation.
package main

import "bufio"
import "errors"
import "flag"
import "fmt"
import "io"
import "net"
import "reflect"
import "runtime"
import "strings"

import "github.com/dmaze/goordinate/backend"
import "github.com/dmaze/goordinate/cborrpc"
import "github.com/dmaze/goordinate/jobserver"
import "github.com/ugorji/go/codec"

func main() {
	bind := flag.String("bind", ":5932", "[ip]:port to listen on")
	backend := backend.Backend{"memory", ""}
	flag.Var(&backend, "backend", "impl[:address] of the storage backend")
	flag.Parse()

	coordinate := backend.Coordinate()
	namespace, err := coordinate.Namespace("")
	if err != nil {
		panic(err)
	}
	jobd := &jobserver.JobServer{namespace}

	ln, err := net.Listen("tcp", *bind)
	if err != nil {
		fmt.Printf("Could not listen to %v: %v\n", *bind, err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Could not accept connection: %v\n", err)
			return
		}
		go handleConnection(conn, jobd)
	}
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

func handleConnection(conn net.Conn, jobd *jobserver.JobServer) {
	defer conn.Close()

	jobdv := reflect.ValueOf(jobd)

	cbor := new(codec.CborHandle)
	cborrpc.SetExts(cbor)
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
		fmt.Printf("Request: %v\n", request)
		response := doRequest(jobdv, request)
		fmt.Printf("Response: %v\n", response)
		encoder.Encode(response)
		writer.Flush()
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
	funcv := jobdv.MethodByName(method)
	if !funcv.IsValid() {
		err = fmt.Errorf("no such method %v", method)
	}
	if err == nil {
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
		} else {
			errV := returns[len(returns)-1].Interface()
			if errV != nil {
				err = errV.(error)
			}
			returns = returns[0 : len(returns)-1]
		}
	}

	if err != nil {
		response.Error = err.Error()
	} else if len(returns) == 1 {
		response.Result = returns[0].Interface()
	} else {
		results := make([]interface{}, len(returns))
		for i := range returns {
			results[i] = returns[i].Interface()
		}
		response.Result = results
	}

	return
}
