package datastream

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/functions"
	"process-engine/api/operation"
	"process-engine/sink/console"
)

type DataStream struct {
	configs   configuration.StreamConfig
	Operators []operation.Operator
}

func (ds *DataStream) Map(mapper functions.MapTransformation) *DataStream {
	result := ds.AddTransformation(mapper)
	return result
}

func (ds *DataStream) FlatMap(flatmapper functions.FlatMapTransformation) *DataStream {
	result := ds.AddTransformation(flatmapper)
	return result
}

func (ds *DataStream) Sink(sinker operation.Transformation) *DataStream {
	if sinker.GetTransformationType() != operation.SINK {
		panic("Sink only accepts transformations of type SINK")
	}
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

func (ds *DataStream) SetConfigs(configs configuration.StreamConfig) {
	ds.configs = configs
}

func (ds *DataStream) Print() *DataStream {
	sinker := console.NewConsoleSinker()
	stream := ds.Sink(sinker)
	return stream
}

func (ds *DataStream) PrintDetails() {
	for id, operator := range ds.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.Transformer.GetTransformationType(), operator.Parallelism)
	}
}
