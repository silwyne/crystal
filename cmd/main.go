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
		SourceFunction: func() (row.Row, bool) {
			time.Sleep(50 * time.Millisecond) // simulate data rate
			my_row := row.From("sourceTime: " + time.Now().Local().String())
			return my_row, true
		},
	}

	// transforming stream into something new
	mapper := functions.MapTransformation{
		MapFunction: func(input row.Row) (row.Row, bool) {
			str := "transform: " + time.Now().Local().String()
			input.AddColumn(str)
			return input, true
		},
	}

	serializer := func(in row.Row) (kafka.Message, bool) {
		message := kafka.Message{
			Value: []byte(in.ToString()),
		}
		return message, true
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
