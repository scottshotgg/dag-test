package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

func groupFunc(ctx context.Context, ch <-chan struct{}) func() error {
	return func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ch:
			return nil
		}
	}
}

func a(v *Vertex) error {
	fmt.Println("a")

	return nil
}

func b(v *Vertex) error {
	fmt.Println("b")

	time.Sleep(5 * time.Second)

	fmt.Println("done with b")

	return nil
}

func c(v *Vertex) error {
	fmt.Println("c")

	time.Sleep(100 * time.Millisecond)

	fmt.Println("done with c")

	return nil
}

func d(v *Vertex) error {
	fmt.Println("d")

	return nil
}

var randy = rand.New(rand.NewSource(time.Now().Unix()))

func trainModel1(v *Vertex) error {
	fmt.Println("Training model 1 ...")

	time.Sleep(time.Duration(randy.Intn(4000)+6000) * time.Millisecond)

	return nil
}

func trainModel2(v *Vertex) error {
	fmt.Println("Training model 2 ...")

	time.Sleep(time.Duration(randy.Intn(4000)+4000) * time.Millisecond)

	return nil
}

func uploadModel1(v *Vertex) error {
	fmt.Println("Uploading model 1 ...")

	time.Sleep(time.Duration(randy.Intn(2000)+1000) * time.Millisecond)

	return nil
}

func uploadModel2(v *Vertex) error {
	fmt.Println("Uploading model 1 ...")

	time.Sleep(time.Duration(randy.Intn(2000)+4000) * time.Millisecond)

	return nil
}

func createEndpoint1(v *Vertex) error {
	fmt.Println("Creating endpoint 1 ...")

	time.Sleep(time.Duration(randy.Intn(100)) * time.Millisecond)

	return nil
}

func createEndpoint2(v *Vertex) error {
	fmt.Println("Creating endpoint 2 ...")

	time.Sleep(time.Duration(randy.Intn(200)+200) * time.Millisecond)

	return nil
}

func deployModelToEndpoint1(v *Vertex) error {
	fmt.Println("Deploying model 1 ...")

	time.Sleep(time.Duration(randy.Intn(5000)+5000) * time.Millisecond)

	go func() {
		var mux = http.NewServeMux()

		mux.HandleFunc("/blur", func(w http.ResponseWriter, r *http.Request) {
			var logger = log.New(os.Stdout, "BLUR_API: ", log.Flags())

			logger.Println("Blurring ...")

			time.Sleep(time.Duration(randy.Intn(200)+100) * time.Millisecond)

			w.Write([]byte(strconv.Itoa(rand.Intn(10))))
		})

		var err = (&http.Server{
			Addr:    "localhost:8080",
			Handler: mux,
		}).ListenAndServe()

		if err != nil {
			panic(err)
		}
	}()

	return nil
}

func deployModelToEndpoint2(v *Vertex) error {
	fmt.Println("Deploying model 2 ...")

	time.Sleep(time.Duration(randy.Intn(2500)+2500) * time.Millisecond)

	go func() {
		var mux = http.NewServeMux()

		mux.HandleFunc("/danger", func(w http.ResponseWriter, r *http.Request) {
			var logger = log.New(os.Stdout, "DANGER_API: ", log.Flags())

			logger.Println("Inferencing for danger ...")

			var b, err = ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			i, err := strconv.Atoi(string(b))
			if err != nil {
				panic(err)
			}

			var response = "false"
			if i > 7 {
				response = "true"
			}

			time.Sleep(time.Duration(randy.Intn(200)+100) * time.Millisecond)

			w.Write([]byte(response))
		})

		var err = (&http.Server{
			Addr:    "localhost:8081",
			Handler: mux,
		}).ListenAndServe()

		if err != nil {
			panic(err)
		}
	}()

	return nil
}

func inferencePipeline(v *Vertex) error {
	var logger = log.New(os.Stdout, "PIPELINE: ", log.Flags())

	for i := 0; i < 10; i++ {
		logger.Println("Sending blur request ...")

		var res, err = http.Get("http://localhost:8080/blur")
		if err != nil {
			panic(err)
		}

		defer res.Body.Close()

		logger.Println("Sending danger request ...")

		res2, err := http.Post("http://localhost:8081/danger", "application/json", res.Body)
		if err != nil {
			panic(err)
		}

		defer res2.Body.Close()

		b, err := ioutil.ReadAll(res2.Body)
		if err != nil {
			panic(err)
		}

		shouldAlert, err := strconv.ParseBool(string(b))
		if err != nil {
			panic(err)
		}

		if shouldAlert {
			logger.Println("ALERTING: THREAT FOUND!")
		}

		fmt.Printf("\n")
	}

	return nil
}

type workFn func(v *Vertex) error

var fnMap = map[string]workFn{
	"uploadModel1":           uploadModel1,
	"createEndpoint1":        createEndpoint1,
	"deployModelToEndpoint1": deployModelToEndpoint1,
	"uploadModel2":           uploadModel2,
	"createEndpoint2":        createEndpoint2,
	"deployModelToEndpoint2": deployModelToEndpoint2,
	"inferencePipeline":      inferencePipeline,
}

// var fnMap = map[string]workFn{
// 	"a": a,
// 	"b": b,
// 	"c": c,
// 	"d": d,
// }
