package stack

import (
	"process-engine/api/container"
	"process-engine/api/functions"
	"sync"
)

const (
	detault_parallelism = 1
)

type DataStack struct {
	configs StackConfig
}

func NewDataStack() DataStack {
	configs := StackConfig{
		Parallelism: detault_parallelism,
	}
	return DataStack{
		configs: configs,
	}
}

func (d *DataStack) SetParallelism(parallelism int) *DataStack {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	d.configs.Parallelism = parallelism
	return d
}

func (d DataStack) FromSource(transformation functions.Transformation) container.DataContainer {
	operator := container.DataOperator{
		Transformer: transformation,
		Parallelism: d.configs.Parallelism,
	}
	return container.DataContainer{}.AddTransformation(operator)
}

func (d DataStack) Execute(container container.DataContainer) {

	Operators := container.Operators

	if len(Operators) == 0 {
		panic("No transformations in pipeline")
	}

	var transformations []functions.Transformation

	for _, operator := range Operators {
		transformations = append(transformations, operator.Transformer)
	}

	d.runOperatorInParallel(transformations)
}

func (d DataStack) runOperatorInParallel(transformations []functions.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(d.configs.Parallelism)
	for i := 0; i < d.configs.Parallelism; i++ {
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
