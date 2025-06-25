package main

import (
	"github.com/crystal/api/core"
	"github.com/crystal/api/functions"
	"github.com/crystal/api/row"

	"time"
)

func main() {
	streamEnv := core.NewStreamEnvironment()
	streamEnv.SetParallelism(4)

	source := functions.SourceTransformation{
		SourceFunction: func() (row.Row, bool) {
			time.Sleep(time.Second) // simulate data rate
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

	stream := streamEnv.FromSource(source)
	stream = stream.Map(mapper)
	sinkStream := stream.Print()

	sinkStream.PrintDetails()
	// running the container
	streamEnv.Execute(sinkStream)
}
