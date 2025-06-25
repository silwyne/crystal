package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/functions"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	source := functions.SourceTransformation{
		SourceFunction: func() (interface{}, bool) {
			time.Sleep(time.Second) // simulate data rate
			return "sourceTime: " + time.Now().Local().String(), true
		},
	}

	// transforming stream into something new
	mapper := functions.MapTransformation{
		MapFunction: func(input interface{}) (interface{}, bool) {
			stringResult := input.(string) + ", transform: " + time.Now().Local().String()
			return stringResult, true
		},
	}

	stream := streamEnv.FromSource(source)
	stream = stream.Map(mapper)
	stream = stream.Print()

	stream.PrintDetails()
	// running the container
	streamEnv.Execute(stream)
}
