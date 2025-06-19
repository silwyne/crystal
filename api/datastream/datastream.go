package datastream

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/functions"
	"process-engine/api/transformation"
	"process-engine/sink/consolesink"
)

type DataStream struct {
	configs   configuration.StreamConfig
	Operators []Operator
}

type Operator struct {
	Transformer transformation.Transformation
	Parallelism int
}

func (o *Operator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}

func (ds *DataStream) Map(mapper functions.MapTransformation) *DataStream {
	result := ds.AddTransformation(mapper)
	return result
}

func (ds *DataStream) FlatMap(flatmapper functions.FlatMapTransformation) *DataStream {
	result := ds.AddTransformation(flatmapper)
	return result
}

func (ds *DataStream) Sink(sinker transformation.Transformation) *DataStream {
	if sinker.GetTransformationType() != transformation.SINK {
		panic("Sink only accepts transformations of type SINK")
	}
	result := ds.AddTransformation(sinker)
	return result
}

func (ds *DataStream) AddTransformation(transformation transformation.Transformation) *DataStream {
	operator := Operator{
		Transformer: transformation,
		Parallelism: ds.configs.GlobalParallelism,
	}
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
	sinker := consolesink.NewConsoleSinker()
	stream := ds.Sink(sinker)
	return stream
}

func (ds *DataStream) PrintDetails() {
	for id, operator := range ds.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.Transformer.GetTransformationType(), operator.Parallelism)
	}
}
