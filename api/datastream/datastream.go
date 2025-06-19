package datastream

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/functions"
	"process-engine/api/transformation"
)

type DataContainer struct {
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

func (d *DataContainer) Map(mapper functions.MapTransformation) *DataContainer {
	result := d.AddTransformation(mapper)
	return result
}

func (d *DataContainer) FlatMap(flatmapper functions.FlatMapTransformation) *DataContainer {
	result := d.AddTransformation(flatmapper)
	return result
}

func (d *DataContainer) Sink(sinker transformation.Transformation) *DataContainer {
	if sinker.GetTransformationType() != transformation.SINK {
		panic("Sink only accepts transformations of type SINK")
	}
	result := d.AddTransformation(sinker)
	return result
}

func (d *DataContainer) AddTransformation(transformation transformation.Transformation) *DataContainer {
	operator := Operator{
		Transformer: transformation,
		Parallelism: d.configs.GlobalParallelism,
	}
	return &DataContainer{
		Operators: append(d.Operators, operator),
		configs:   d.configs,
	}
}

func (d *DataContainer) SetParallelism(parallelism int) *DataContainer {
	last_operator := &d.Operators[len(d.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return d
}

func (d *DataContainer) SetConfigs(configs configuration.StreamConfig) {
	d.configs = configs
}

func (d *DataContainer) PrintDetails() {
	for id, operator := range d.Operators {
		log.Printf("n.%v name: %v, parallelism: %v\n", id, operator.Transformer.GetTransformationType(), operator.Parallelism)
	}
}
