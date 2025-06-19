package core

import (
	"process-engine/api/configuration"
	"process-engine/api/datastream"
	"process-engine/api/transformation"
	"sync"
)

const (
	DEFAULT_PARALLELISM = 1
)

type DataStack struct {
	configs configuration.StreamConfig
}

func NewDataStack() DataStack {
	configs := configuration.StreamConfig{
		GlobalParallelism: DEFAULT_PARALLELISM,
	}
	return DataStack{
		configs: configs,
	}
}

func (d *DataStack) SetParallelism(parallelism int) *DataStack {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	d.configs.GlobalParallelism = parallelism
	return d
}

func (d DataStack) FromSource(transformer transformation.Transformation) *datastream.DataContainer {
	if transformer.GetTransformationType() != transformation.SOURCE {
		panic("FromSource only accepts Transformation of type SOURCE")
	}
	stream := datastream.DataContainer{}
	stream.SetConfigs(d.configs)
	streamWithTransformation := stream.AddTransformation(transformer)
	return streamWithTransformation
}

func (d DataStack) Execute(container *datastream.DataContainer) {
	operators := container.Operators
	if len(operators) == 0 {
		panic("No transformations in pipeline")
	}

	var transformations []transformation.Transformation
	for _, operator := range operators {
		transformations = append(transformations, operator.Transformer)
	}
	d.runOperatorInParallel(transformations)
}

func (d DataStack) runOperatorInParallel(transformations []transformation.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(d.configs.GlobalParallelism)
	for i := 0; i < d.configs.GlobalParallelism; i++ {
		go executeTransformations(&wg, transformations, channel_holder)
	}
	wg.Wait()
}

func executeTransformations(
	wg *sync.WaitGroup, transformations []transformation.Transformation,
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
