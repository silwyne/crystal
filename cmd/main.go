package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"
	"github.com/crystal/kafka/consumer"
	"github.com/twmb/franz-go/pkg/kgo"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	deserializer := func(in *kgo.Record) (row.Row, error) {
		return row.From(string(in.Value)), nil
	}

	source := consumer.NewKafkaSource(
		"localhost:9092",
		"test-0",
		"test-group-id",
		deserializer,
	)

	// transforming stream into something new
	mapper := functions.MapTransformation{
		MapFunction: func(input row.Row) (row.Row, error) {
			str := "transform: " + time.Now().Local().String()
			input.AddColumn(str)
			return input, nil
		},
	}

	stream := streamEnv.FromSource(source)
	stream = stream.Map(mapper)
	stream = stream.Print()

	// running the container
	streamEnv.Execute(stream)
}
