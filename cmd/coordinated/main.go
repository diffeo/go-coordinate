// Copyright 2015-2017 Diffeo, Inc.
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
	"github.com/diffeo/go-coordinate/cache"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

func main() {
	var err error

	cborRPCBind := flag.String("cborrpc", ":5932",
		"[ip]:port for CBOR-RPC interface")
	httpBind := flag.String("http", ":5980",
		"[ip]:port for HTTP REST interface")
	backend := backend.Backend{Implementation: "memory", Address: ""}
	flag.Var(&backend, "backend", "impl[:address] of the storage backend")
	config := flag.String("config", "", "global configuration YAML file")
	logRequests := flag.Bool("log-requests", false, "log all requests")
	flag.Parse()

	var gConfig map[string]interface{}
	if *config != "" {
		gConfig, err = loadConfigYaml(*config)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err,
			}).Fatal("Could not load YAML configuration")
			return
		}
	}

	coordinate, err := backend.Coordinate()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"err": err,
		}).Fatal("Could not create Coordinate backend")
		return
	}
	coordinate = cache.New(coordinate)

	var reqLogger *logrus.Logger
	if *logRequests {
		stdlog := logrus.StandardLogger()
		reqLogger = &logrus.Logger{
			Out:       stdlog.Out,
			Formatter: stdlog.Formatter,
			Hooks:     stdlog.Hooks,
			Level:     logrus.DebugLevel,
		}
	}

	go ServeCBORRPC(coordinate, gConfig, "tcp", *cborRPCBind, reqLogger)
	go ServeHTTP(coordinate, *httpBind)
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
