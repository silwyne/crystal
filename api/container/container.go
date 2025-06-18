package container

import (
	"process-engine/api/functions"
)

type DataContainer struct {
	Operators []DataOperator
}

func (d DataContainer) Map(mapper functions.MapTransformation) DataContainer {
	operator := DataOperator{
		Transformer: mapper,
		Parallelism: 1,
	}
	return d.AddTransformation(operator)
}

func (d DataContainer) Sink(sinker functions.SinkTransformation) DataContainer {
	operator := DataOperator{
		Transformer: sinker,
		Parallelism: 1,
	}
	return d.AddTransformation(operator)
}

func (d DataContainer) AddTransformation(operator DataOperator) DataContainer {
	return DataContainer{
		Operators: append(d.Operators, operator),
	}
}

func (d DataContainer) SetParallelism(parallelism int) DataContainer {
	last_operator := d.Operators[len(d.Operators)-1]
	last_operator.SetParallelism(parallelism)
	return d
}
