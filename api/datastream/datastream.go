package datastream

import (
	"process-engine/api/functions"
)

type DataContainer struct {
	Operators []Operator
}

type Operator struct {
	Transformer functions.Transformation
	Parallelism int
}

func (o *Operator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}

func (d DataContainer) Map(mapper functions.MapTransformation) DataContainer {
	operator := Operator{
		Transformer: mapper,
		Parallelism: 1,
	}
	return d.AddTransformation(operator)
}

func (d DataContainer) Sink(sinker functions.SinkTransformation) DataContainer {
	operator := Operator{
		Transformer: sinker,
		Parallelism: 1,
	}
	return d.AddTransformation(operator)
}

func (d DataContainer) AddTransformation(operator Operator) DataContainer {
	return DataContainer{
		Operators: append(d.Operators, operator),
	}
}

func (d DataContainer) SetParallelism(parallelism int) DataContainer {
	last_operator := d.Operators[len(d.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return d
}
