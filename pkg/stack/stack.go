package stack

import (
	"process-engine/pkg/container"
	"process-engine/pkg/functions"
	"time"
)

func FromSource(source functions.SourceFunction) container.DataContainer {
	return container.DataContainer{}.AddTransformation(functions.SourceTransformation{Function: source})
}

func Execute(container container.DataContainer) {
	transformations := container.Transformations

	if len(transformations) == 0 {
		panic("No transformations in pipeline")
	}
	if _, ok := transformations[0].(functions.SourceTransformation); !ok {
		panic("First transformation must be a source function")
	}
	sourceTransformation := transformations[0]

	for {
		data, ok := sourceTransformation.Apply(nil)
		if !ok {
			break
		}
		current := data
		for _, t := range transformations[1:] { // Skip the source
			var cont bool
			current, cont = t.Apply(current)
			if !cont {
				break
			}
		}
		time.Sleep(time.Second)
	}
}
