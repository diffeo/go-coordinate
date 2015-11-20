// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Package coordinated provides a wire-compatible reimplementation of
// the Diffeo Coordinate daemon.  This is intended to be fully
// compatible with the existing Coordinate toolset at
// https://github.com/diffeo/coordinate.  This is purely a server-side
// daemon; it does not include application code or a worker
// implementation.
package main

import (
	"flag"
	"io/ioutil"

	"github.com/diffeo/go-coordinate/backend"
	"gopkg.in/yaml.v2"
)

func main() {
	var err error

	cborRPCBind := flag.String("cborrpc", ":5932",
		"[ip]:port for CBOR-RPC interface")
	backend := backend.Backend{Implementation: "memory", Address: ""}
	flag.Var(&backend, "backend", "impl[:address] of the storage backend")
	config := flag.String("config", "", "global configuration YAML file")
	flag.Parse()

	var gConfig map[string]interface{}
	if *config != "" {
		gConfig, err = loadConfigYaml(*config)
		if err != nil {
			panic(err)
		}
	}

	coordinate, err := backend.Coordinate()
	if err != nil {
		panic(err)
	}

	go ServeCBORRPC(coordinate, gConfig, "tcp", *cborRPCBind)
	select {}
}

func loadConfigYaml(filename string) (map[string]interface{}, error) {
	var result map[string]interface{}
	var err error
	var bytes []byte
	bytes, err = ioutil.ReadFile(filename)
	if err == nil {
		err = yaml.Unmarshal(bytes, &result)
	}
	return result, err
}
