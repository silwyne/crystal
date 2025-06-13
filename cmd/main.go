package main

import (
	"fmt"
	"process-engine/pkg/stack"
	"time"
)

func main() {
	source := func() (interface{}, bool) {
		mySimpleData := "source: " + time.Now().Local().String()
		return mySimpleData, true
	}

	mapper := func(input interface{}) (interface{}, bool) {
		stringResult := input.(string) + ", transform: " + time.Now().Local().String()
		return stringResult, true
	}

	sinker := func(input interface{}) bool {
		fmt.Println(input.(string))
		time.Sleep(time.Second) // for testing
		return true
	}

	container := stack.FromSource(source).Map(mapper).Sink(sinker)
	stack.Execute(container)
}
