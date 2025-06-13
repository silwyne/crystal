package container

import (
	"process-engine/pkg/functions"
	"time"
)

type DataContainer struct {
	Transformations []interface{}
}

func FromSource(source functions.SourceFunction) DataContainer {
	return DataContainer{
		Transformations: []interface{}{source},
	}
}

func (d DataContainer) Map(mapper functions.MapFunction) DataContainer {
	return d.addTransformation(mapper)
}

func (d DataContainer) Sink(sinker functions.SinkFunction) DataContainer {
	return d.addTransformation(sinker)
}

func (d DataContainer) addTransformation(transformation interface{}) DataContainer {
	return DataContainer{
		Transformations: append(d.Transformations, transformation),
	}
}

func (d DataContainer) Run() {
	// Find the source function
	if len(d.Transformations) == 0 {
		return
	}
	source, ok := d.Transformations[0].(functions.SourceFunction)
	if !ok {
		panic("First transformation must be a SourceFunction")
	}

	for {
		// Get data from the source
		data, ok := source()
		if !ok {
			break // No more data
		}

		// Pass data through all transformations (except source)
		var current interface{} = data
		for _, t := range d.Transformations[1:] {
			switch fn := t.(type) {
			case functions.MapFunction:
				current = fn(current)
			case functions.SinkFunction:
				fn(current)
			}
		}

		// simulation
		time.Sleep(time.Second)
	}
}
