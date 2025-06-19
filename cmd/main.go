package main

import (
	"process-engine/api/core"
	"process-engine/api/datastream"
	"process-engine/api/functions"
	"process-engine/source/datagenerator"

	"time"
)

var (
	streamEnv core.StreamEnvironment
)

func exampleJob() *datastream.DataStream {
	source := datagenerator.NewDataGenerator(func() (interface{}, bool) {
		time.Sleep(time.Second) // simulate data rate
		return "sourceTime: " + time.Now().Local().String(), true
	})

	mapper := functions.MapTransformation{
		Function: func(input interface{}) (interface{}, bool) {
			stringResult := string(input.(string)) + ", transform: " + time.Now().Local().String()
			return stringResult, true
		},
	}

	stream := streamEnv.FromSource(source).SetParallelism(3)
	stream = stream.Map(mapper).SetParallelism(3)
	stream = stream.Print()

	return stream
}

func main() {
	streamEnv = core.NewStreamEnvironment()
	streamEnv.SetParallelism(2)

	// making a container
	stream := exampleJob()
	stream.PrintDetails()

	// running the container
	streamEnv.Execute(stream)
}
