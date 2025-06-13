package container

import (
	"process-engine/pkg/functions"
)

type DataContainer struct {
	Transformations []functions.Transformation
}

func (d DataContainer) Map(mapper functions.MapFunction) DataContainer {
	return d.AddTransformation(functions.MapTransformation{Function: mapper})
}

func (d DataContainer) Sink(sinker functions.SinkFunction) DataContainer {
	return d.AddTransformation(functions.SinkTransformation{Function: sinker})
}

func (d DataContainer) AddTransformation(transformation functions.Transformation) DataContainer {
	return DataContainer{
		Transformations: append(d.Transformations, transformation),
	}
}
