package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"
	"github.com/crystal/sink/kafka_sink"
	"github.com/segmentio/kafka-go"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	source := functions.SourceTransformation{
		SourceFunction: func() (row.Row, error) {
			time.Sleep(100 * time.Millisecond) // simulate data rate
			my_row := row.From("sourceTime: " + time.Now().Local().String())
			return my_row, nil
		},
	}

	// transforming stream into something new
	mapper := functions.MapTransformation{
		MapFunction: func(input row.Row) (row.Row, error) {
			str := "transform: " + time.Now().Local().String()
			input.AddColumn(str)
			return input, nil
		},
	}

	serializer := func(in row.Row) (kafka.Message, error) {
		message := kafka.Message{
			Value: []byte(in.ToString()),
		}
		return message, nil
	}

	sinker := kafka_sink.NewKafkaSink(
		"localhost:9092",
		"test-0",
		serializer,
	)

	stream := streamEnv.FromSource(source)
	stream = stream.Map(mapper)
	stream = stream.Sink(sinker)

	// running the container
	streamEnv.Execute(stream)
}
