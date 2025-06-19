package datastream

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/functions"
	"process-engine/api/transformation"
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

func (d *DataStream) Map(mapper functions.MapTransformation) *DataStream {
	result := d.AddTransformation(mapper)
	return result
}

func (d *DataStream) FlatMap(flatmapper functions.FlatMapTransformation) *DataStream {
	result := d.AddTransformation(flatmapper)
	return result
}

func (d *DataStream) Sink(sinker transformation.Transformation) *DataStream {
	if sinker.GetTransformationType() != transformation.SINK {
		panic("Sink only accepts transformations of type SINK")
	}
	result := d.AddTransformation(sinker)
	return result
}

func (d *DataStream) AddTransformation(transformation transformation.Transformation) *DataStream {
	operator := Operator{
		Transformer: transformation,
		Parallelism: d.configs.GlobalParallelism,
	}
	return &DataStream{
		Operators: append(d.Operators, operator),
		configs:   d.configs,
	}
}

func (d *DataStream) SetParallelism(parallelism int) *DataStream {
	last_operator := &d.Operators[len(d.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return d
}

func (d *DataStream) SetConfigs(configs configuration.StreamConfig) {
	d.configs = configs
}

func (d *DataStream) PrintDetails() {
	for id, operator := range d.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.Transformer.GetTransformationType(), operator.Parallelism)
	}
}
