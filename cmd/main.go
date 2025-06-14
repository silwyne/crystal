package main

import (
	"fmt"
	"process-engine/internals/container"
	"process-engine/internals/functions"
	"process-engine/internals/stack"
	"process-engine/pkg/kafkaSource"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	PARALLELISM = 1
	myDataStack = stack.NewDataStack(PARALLELISM)
)

func createContainer() container.DataContainer {
	source := kafkaSource.NewKafkaSource([]string{"127.0.0.1:9092"}, "test-0", "test-group")

	mapper := functions.MapTransformation{
		Function: func(input interface{}) (interface{}, bool) {
			stringResult := string(input.(kafka.Message).Value) + ", transform: " + time.Now().Local().String()
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
