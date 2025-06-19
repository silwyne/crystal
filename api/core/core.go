package core

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/datastream"
	"process-engine/api/transformation"
	"sync"
)

const (
	DEFAULT_PARALLELISM = 1
)

type StreamEnvironment struct {
	configs configuration.StreamConfig
}

func NewStreamEnvironment() StreamEnvironment {
	configs := configuration.StreamConfig{
		GlobalParallelism: DEFAULT_PARALLELISM,
	}
	return StreamEnvironment{
		configs: configs,
	}
}

func (d *StreamEnvironment) SetParallelism(parallelism int) *StreamEnvironment {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	d.configs.GlobalParallelism = parallelism
	return d
}

func (d StreamEnvironment) FromSource(transformer transformation.Transformation) *datastream.DataStream {
	if transformer.GetTransformationType() != transformation.SOURCE {
		panic("FromSource only accepts Transformation of type SOURCE")
	}
	stream := datastream.DataStream{}
	stream.SetConfigs(d.configs)
	streamWithTransformation := stream.AddTransformation(transformer)
	return streamWithTransformation
}

func (d StreamEnvironment) Execute(container *datastream.DataStream) {
	operators := container.Operators
	if len(operators) == 0 {
		panic("No transformations in pipeline")
	}

	var transformations []transformation.Transformation
	for _, operator := range operators {
		transformations = append(transformations, operator.Transformer)
	}
	log.Printf("ignoring operators parallelism using global parallelism %v\n", d.configs.GlobalParallelism)
	d.runOperatorInParallel(transformations)
}

func (d StreamEnvironment) runOperatorInParallel(transformations []transformation.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(d.configs.GlobalParallelism)
	for range d.configs.GlobalParallelism {
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
