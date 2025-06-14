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

	d.runOperatorInParallel(transformations)
}

func (d DataStack) runOperatorInParallel(transformations []functions.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(d.parallelism)
	for i := 0; i < d.parallelism; i++ {
		go executeTransformations(&wg, transformations, channel_holder)
	}
	wg.Wait()
}

func executeTransformations(
	wg *sync.WaitGroup,
	transformations []functions.Transformation,
	channel_holder []chan interface{}) {

	for id, transformation := range transformations {
		var source_channel chan interface{}
		if id == 0 {
			source_channel = make(chan interface{})
		} else {
			source_channel = channel_holder[id-1]
		}
		result_channel := transformation.ExecuteTransformation(wg, source_channel)
		channel_holder = append(channel_holder, result_channel)
	}
}
