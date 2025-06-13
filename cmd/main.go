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

	mapper := func(input interface{}) interface{} {
		stringResult := input.(string) + ", transform: " + time.Now().Local().String()
		return stringResult
	}

	sinker := func(input interface{}) {
		fmt.Println(input.(string))
	}

	container := stack.FromSource(source).Map(mapper).Sink(sinker)

	stack.Execute(container)
}
