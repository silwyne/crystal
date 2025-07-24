package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"
	"github.com/crystal/datagenerator"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	infiniteSource := false
	ratePerSecond := 1
	sourceDuration := 10
	generator := func() row.Row {
		return row.From(time.Now().Local().String())
	}
	source := datagenerator.NewDataGenerator(infiniteSource, ratePerSecond, sourceDuration, generator)

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
