package container

import (
	"process-engine/pkg/functions"
	"time"
)

type DataContainer struct {
	Transformations []functions.Transformation
}

func FromSource(source functions.SourceFunction) DataContainer {
	return DataContainer{}.addTransformation(functions.SourceTransformation{Function: source})
}

func (d DataContainer) Map(mapper functions.MapFunction) DataContainer {
	return d.addTransformation(functions.MapTransformation{Function: mapper})
}

func (d DataContainer) Sink(sinker functions.SinkFunction) DataContainer {
	return d.addTransformation(functions.SinkTransformation{Function: sinker})
}

func (d DataContainer) addTransformation(transformation functions.Transformation) DataContainer {
	return DataContainer{
		Transformations: append(d.Transformations, transformation),
	}
}

func (d DataContainer) Run() {
	if len(d.Transformations) == 0 {
		panic("No transformations in pipeline")
	}
	if _, ok := d.Transformations[0].(functions.SourceTransformation); !ok {
		panic("First transformation must be a source function")
	}
	sourceTransformation := d.Transformations[0]

	for {
		data, ok := sourceTransformation.Apply(nil)
		if !ok {
			break
		}
		current := data
		for _, t := range d.Transformations[1:] { // Skip the source
			var cont bool
			current, cont = t.Apply(current)
			if !cont {
				break
			}
		}
		time.Sleep(time.Second)
	}
}
