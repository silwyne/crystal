package main

import (
	"fmt"
	"process-engine/api/container"
	"process-engine/api/functions"
	"process-engine/api/stack"
	"process-engine/source/datagenerator"

	"time"
)

var (
	myDataStack stack.DataStack
)

func createContainer() container.DataContainer {
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

	sinker := functions.SinkTransformation{
		Function: func(input interface{}) bool {
			fmt.Println(input.(string))
			return true
		},
	}

	container := myDataStack.FromSource(source).Map(mapper).Sink(sinker)

	return container
}

func main() {
	myDataStack = stack.NewDataStack()

	// change Parallelism from 1 to 2
	myDataStack.SetParallelism(2)

	// making a container
	container := createContainer()

	// running the container
	myDataStack.Execute(container)
}
