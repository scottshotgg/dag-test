package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// var m = `
// 	{
// 		"a": [],

// 		"b": [ "a" ],

// 		"c": [ "a" ],

// 		"d": [ "a", "b", "c" ]
// 	}
// `

var m = `
	{
		"uploadModel1": [],

		"createEndpoint1": [
			"uploadModel1"
		],

		"deployModelToEndpoint1": [
			"createEndpoint1"
		],

		"uploadModel2": [],

		"createEndpoint2": [
			"uploadModel2"
		],

		"deployModelToEndpoint2": [
			"createEndpoint2"
		],

		"inferencePipeline": [
			"deployModelToEndpoint1",
			"deployModelToEndpoint2"
		]
	}
`

func TestParseDAG(t *testing.T) {
	var s, err = ParseDAG(bytes.NewBufferString(m))
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("s:", s)
}

func TestBuildDAG(t *testing.T) {
	var s, err = ParseDAG(bytes.NewBufferString(m))
	if err != nil {
		t.Fatal(err)
	}

	var d = BuildDAG(s)
	fmt.Println("DAG:", d)
}

func TestProcessDAG(t *testing.T) {
	var s, err = ParseDAG(bytes.NewBufferString(m))
	if err != nil {
		t.Fatal(err)
	}

	var d = BuildDAG(s)
	processDAG(context.Background(), d)
}
