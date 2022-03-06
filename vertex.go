package main

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

type Vertex struct {
	sync.Mutex

	ID string

	running  bool
	done     bool
	isSource bool
	isSink   bool

	fn  workFn
	err error

	readySignal chan struct{}
	depSignals  map[string]chan struct{}

	// TODO(scottshotgg): make children a sub-DAG
	children map[string]*Vertex
	parents  map[string]*Vertex
}

func processVerticies(ctx context.Context, vs map[string]*Vertex) {
	var wg sync.WaitGroup

	// Start from the source nodes
	for _, v := range vs {
		fmt.Println()

		wg.Add(1)
		go func(v *Vertex) {
			defer wg.Done()

			v.start(ctx)
		}(v)
	}

	wg.Wait()
}

func (v *Vertex) start(ctx context.Context) {
	v.Lock()
	defer v.Unlock()

	// If the vertex is already finished or still running
	switch {
	case v.running:
		fmt.Println("Already running:", v.ID)
		return

	case v.done:
		fmt.Println("Already done:", v.ID)
		return
	}

	// When we are done here, signal downstream depencies by closing the channel
	defer close(v.readySignal)

	// If we have dependencies then being waiting on them
	if len(v.depSignals) > 0 {
		v.wait(ctx)
	}

	fmt.Println("Vertex is ready:", v.ID)

	// Do your thing once all of your dependencies have finished

	v.running = true
	fmt.Println("Running:", v.ID)

	processVertex(v)

	v.done = true
	fmt.Println("Done running:", v.ID)

	return
}

func (v *Vertex) wait(ctx context.Context) error {
	var wg errgroup.Group

	// Wait on each dependency
	for _, ch := range v.depSignals {
		wg.Go(groupFunc(ctx, ch))
	}

	return wg.Wait()
}

func processVertex(v *Vertex) {
	fmt.Println("Processing:", v.ID)

	v.err = v.fn(v)
	if v.err != nil {
		fmt.Println("err:", v.err)
	}
}
