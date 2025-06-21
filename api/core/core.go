package core

import (
	"log"
	"process-engine/api/configuration"
	"process-engine/api/datastream"
	"process-engine/api/operation"
	"sync"
)

type StreamEnvironment struct {
	configs configuration.StreamConfig
}

func NewStreamEnvironment() *StreamEnvironment {
	configs := configuration.StreamConfig{
		GlobalParallelism: configuration.DEFAULT_PARALLELISM,
	}
	return &StreamEnvironment{
		configs: configs,
	}
}

func (se *StreamEnvironment) SetParallelism(parallelism int) *StreamEnvironment {
	if parallelism < 1 {
		panic("parallelism can't be less than 1")
	}
	se.configs.GlobalParallelism = parallelism
	return se
}

func (se *StreamEnvironment) FromSource(transformer operation.Transformation) *datastream.DataStream {
	if transformer.GetTransformationType() != operation.SOURCE {
		panic("FromSource only accepts Transformation of type SOURCE")
	}
	stream := datastream.DataStream{}
	stream.SetConfigs(se.configs)
	streamWithTransformation := stream.AddTransformation(transformer)
	return streamWithTransformation
}

func (se *StreamEnvironment) Execute(container *datastream.DataStream) {
	operators := container.Operators
	if len(operators) == 0 {
		panic("No transformations in pipeline")
	}

	for id, operator := range operators {
		if id+1 < len(operators) {
			if operator.Parallelism != operators[id+1].Parallelism {
				panic("only supporting DIRECT_CHAIN between operators so all operators parallelism must be same")
			}
		}

	}

	var transformations []operation.Transformation
	for _, operator := range operators {
		transformations = append(transformations, operator.Transformer)
	}
	log.Printf("ignoring operators parallelism using global parallelism %v\n", se.configs.GlobalParallelism)
	se.runOperatorInParallel(transformations)
}

func (se *StreamEnvironment) runOperatorInParallel(transformations []operation.Transformation) {
	var wg sync.WaitGroup
	var channel_holder []chan interface{}
	wg.Add(se.configs.GlobalParallelism)
	for range se.configs.GlobalParallelism {
		go executeTransformations(&wg, transformations, channel_holder)
	}
	wg.Wait()
}

func executeTransformations(
	wg *sync.WaitGroup, transformations []operation.Transformation,
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
