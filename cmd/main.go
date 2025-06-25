package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/datastream"
	"github.com/crystal/api/functions"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	source := functions.SourceTransformation[any, string]{
		SourceFunction: func() (string, bool) {
			time.Sleep(time.Second) // simulate data rate
			return "sourceTime: " + time.Now().Local().String(), true
		},
	}

	// transforming stream into something new
	mapper := functions.MapTransformation[string, string]{
		MapFunction: func(input string) (string, bool) {
			stringResult := input + ", transform: " + time.Now().Local().String()
			return stringResult, true
		},
	}

	stream := core.FromSource(streamEnv, source)
	stream = datastream.Map(stream, mapper)
	sinkStream := stream.Print()

	sinkStream.PrintDetails()
	// running the container
	core.Execute(streamEnv, sinkStream)
}
