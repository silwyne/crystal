package main

import (
	"process-engine/api/core"
	"process-engine/api/datastream"
	"process-engine/api/functions"
	"process-engine/sink/consolesink"
	"process-engine/source/datagenerator"

	"time"
)

var (
	myDataStack core.DataStack
)

func createContainer() *datastream.DataContainer {
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

	sinker := consolesink.NewConsoleSinker()

	container := myDataStack.FromSource(source).SetParallelism(3)
	container = container.Map(mapper).SetParallelism(3)
	container = container.Sink(sinker).SetParallelism(3)

	return container
}

func main() {
	myDataStack = core.NewDataStack()
	myDataStack.SetParallelism(2)

	// making a container
	container := createContainer()
	container.PrintDetails()

	// running the container
	myDataStack.Execute(container)
}
