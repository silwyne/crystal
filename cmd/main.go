package main

import (
	"fmt"
	"process-engine/pkg/container"
	"process-engine/pkg/stack"
	"time"
)

var (
	PARALLELISM = 1
	myDataStack = stack.NewDataStack(PARALLELISM)
)

func createContainer() container.DataContainer {
	source := func() (interface{}, bool) {
		mySimpleData := "source: " + time.Now().Local().String()
		time.Sleep(time.Second)
		return mySimpleData, true
	}

	mapper := func(input interface{}) (interface{}, bool) {
		stringResult := input.(string) + ", transform: " + time.Now().Local().String()
		return stringResult, true
	}

	sinker := func(input interface{}) bool {
		fmt.Println(input.(string))
		return true
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
