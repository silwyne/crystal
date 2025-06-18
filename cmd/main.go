package main

import (
	"fmt"
	"process-engine/internals/container"
	"process-engine/internals/functions"
	"process-engine/internals/stack"
	"process-engine/pkg/source/datagenerator"

	"time"
)

var (
	PARALLELISM = 1
	myDataStack = stack.NewDataStack(PARALLELISM)
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
	// making a container
	container := createContainer()

	// running the container
	myDataStack.Execute(container)
}
