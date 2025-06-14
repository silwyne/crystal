package stack

import (
	"process-engine/pkg/container"
	"process-engine/pkg/functions"
	"sync"
)

type DataStack struct {
	parallelism int
}

func NewDataStack(parallelism int) DataStack {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	return DataStack{
		parallelism: parallelism,
	}
}

func (d DataStack) FromSource(source functions.SourceFunction) container.DataContainer {
	return container.DataContainer{}.AddTransformation(functions.SourceTransformation{Function: source})
}

func (d DataStack) Execute(container container.DataContainer) {

	transformations := container.Transformations

	if len(transformations) == 0 {
		panic("No transformations in pipeline")
	}
	if _, ok := transformations[0].(functions.SourceTransformation); !ok {
		panic("First transformation must be a source function")
	}

	d.runChainInParallel(transformations)
}

func (d DataStack) runChainInParallel(transformations []functions.Transformation) {
	var wg sync.WaitGroup
	for i := 0; i < d.parallelism; i++ {
		wg.Add(1)
		go executeTransformationChain(transformations)
	}
	wg.Wait()
}

func executeTransformationChain(transformations []functions.Transformation) {
	for {
		var current interface{}
		for _, t := range transformations {
			var cont bool
			current, cont = t.Apply(current)
			if !cont {
				break
			}
		}
	}
}
