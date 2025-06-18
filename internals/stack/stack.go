package stack

import (
	"process-engine/internals/container"
	"process-engine/internals/functions"
	"sync"
)

const (
	detault_parallelism = 1
)

type DataStack struct {
	parallelism int
}

func NewDataStack() DataStack {
	return DataStack{
		parallelism: detault_parallelism,
	}
}

func (d *DataStack) SetParallelism(parallelism int) *DataStack {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	d.parallelism = parallelism
	return d
}

func (d DataStack) FromSource(transformation functions.Transformation) container.DataContainer {
	return container.DataContainer{}.AddTransformation(transformation)
}

func (d DataStack) Execute(container container.DataContainer) {

	transformations := container.Transformations

	if len(transformations) == 0 {
		panic("No transformations in pipeline")
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
