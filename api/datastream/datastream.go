package datastream

import (
	"fmt"
	"log"

	"github.com/Silwyne/crystal/api/configuration"
	"github.com/Silwyne/crystal/api/functions"
	"github.com/Silwyne/crystal/api/operation"
	"github.com/Silwyne/crystal/api/row"
)

type DataStream struct {
	configs   configuration.StreamConfig
	Operators []operation.Operator
}

func (ds *DataStream) Map(mapper functions.MapTransformation) *DataStream {
	result := ds.AddTransformation(mapper)
	return result
}

func (ds *DataStream) Sink(sinker operation.Transformation) *DataStream {
	result := ds.AddTransformation(sinker)
	return result
}

func (ds *DataStream) AddTransformation(transformer operation.Transformation) *DataStream {
	operator := operation.NewOperator(transformer, ds.configs.GlobalParallelism)
	return &DataStream{
		Operators: append(ds.Operators, operator),
		configs:   ds.configs,
	}
}

func (ds *DataStream) SetParallelism(parallelism int) *DataStream {
	last_operator := &ds.Operators[len(ds.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return ds
}

func (ds *DataStream) DisableChaining() *DataStream {
	last_operator := &ds.Operators[len(ds.Operators)-1]
	last_operator.SetChaining(false)
	return ds
}

func (ds *DataStream) SetQueueLength(bufferLength int) *DataStream {
	last_operator := &ds.Operators[len(ds.Operators)-1]
	last_operator.GetQueueConfig().SetBufferLength(bufferLength)
	return ds
}

func (ds *DataStream) SetQueueUnbuffered() *DataStream {
	last_operator := &ds.Operators[len(ds.Operators)-1]
	last_operator.GetQueueConfig().SetBuffered(false)
	return ds
}

func (ds *DataStream) SetConfigs(configs configuration.StreamConfig) {
	ds.configs = configs
}

func (ds *DataStream) Print() *DataStream {
	sinker := functions.SinkTransformation{
		SinkFunction: func(i row.Row) {
			fmt.Println("> " + i.ToString())
		},
	}
	stream := ds.Sink(sinker)
	return stream
}

func (ds *DataStream) PrintDetails() {
	for id, operator := range ds.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.GetTransformation().GetName(), operator.GetParallelism())
	}
}
