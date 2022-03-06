package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

type (
	Spec map[string][]string

	DAG struct {
		SourceV   map[string]*Vertex
		SinkV     map[string]*Vertex
		Verticies map[string]*Vertex
	}
)

func (d DAG) String() string {
	var s string

	s += fmt.Sprintln("\nVerticies:")
	for _, v := range d.Verticies {
		s += fmt.Sprintln("Vertex:", v.ID)
		for _, c := range v.children {
			s += fmt.Sprintln("Child:", c.ID)
		}

		for _, c := range v.parents {
			s += fmt.Sprintln("Parent:", c.ID)
		}

		s += fmt.Sprintf("\n")
	}

	s += fmt.Sprintln("\nSources:")
	for _, v := range d.SourceV {
		s += fmt.Sprintln("source:", v.ID)
	}

	s += fmt.Sprintln("\nSinks:")
	for _, v := range d.SinkV {
		s += fmt.Sprintln("sink:", v.ID)
	}

	return s
}

func ParseDAG(b io.Reader) (Spec, error) {
	var (
		s   Spec
		err = json.NewDecoder(b).Decode(&s)
	)

	if err != nil {
		return nil, err
	}

	return s, nil
}

func BuildDAG(s Spec) *DAG {
	var d = DAG{
		SourceV:   map[string]*Vertex{},
		SinkV:     map[string]*Vertex{},
		Verticies: map[string]*Vertex{},
	}

	for name, parents := range s {
		var fn, ok = fnMap[name]
		if !ok {
			panic("could not find func: " + name)
		}

		child, ok := d.Verticies[name]
		if !ok {
			child = &Vertex{
				ID:       name,
				fn:       fn,
				children: map[string]*Vertex{},
				parents:  map[string]*Vertex{},
				isSink:   true,

				readySignal: make(chan struct{}),
				depSignals:  map[string]chan struct{}{},
			}

			d.SinkV[name] = child
			d.Verticies[name] = child
		}

		if len(parents) == 0 {
			child.isSource = true
			d.SourceV[name] = child

			continue
		}

		for _, n := range parents {
			var fn, ok = fnMap[n]
			if !ok {
				panic("could not find func: " + name)
			}

			parent, ok := d.Verticies[n]
			if !ok {
				parent = &Vertex{
					ID:       n,
					fn:       fn,
					children: map[string]*Vertex{},
					parents:  map[string]*Vertex{},

					readySignal: make(chan struct{}),
					depSignals:  map[string]chan struct{}{},
				}
			}

			parent.isSink = false
			delete(d.SinkV, n)

			child.depSignals[n] = parent.readySignal
			child.parents[n] = parent
			parent.children[name] = child
			d.Verticies[n] = parent
		}
	}

	return &d
}

func processDAG(ctx context.Context, d *DAG) {
	processVerticies(ctx, d.Verticies)
}
