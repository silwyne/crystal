package container

import (
	"process-engine/api/functions"
)

type DataContainer struct {
	Transformations []functions.Transformation
}

func (d DataContainer) Map(mapper functions.MapTransformation) DataContainer {
	return d.AddTransformation(mapper)
}

func (d DataContainer) Sink(sinker functions.SinkTransformation) DataContainer {
	return d.AddTransformation(sinker)
}

func (d DataContainer) AddTransformation(transformation functions.Transformation) DataContainer {
	return DataContainer{
		Transformations: append(d.Transformations, transformation),
	}
}
