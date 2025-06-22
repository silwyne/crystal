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

	// currently we only support for DIRECT_CHAIN strategy and this loop only chains operator with that
	for id, operator := range operators {
		if id+1 < len(operators) {
			if operator.Parallelism != operators[id+1].Parallelism {
				panic("only supporting DIRECT_CHAIN between operators so all operators parallelism must be same")
			}
		}
	}

	log.Printf("ignoring operators parallelism using global parallelism %v\n", se.configs.GlobalParallelism)
	se.runLayers(operators)
}

func (se *StreamEnvironment) runLayers(operators []operation.Operator) {

	var wg sync.WaitGroup
	var all_result_channels [][]chan interface{}
	for id, operator := range operators {

		log.Printf("layer %v : %v : Starting\n", id, operator.Transformer.GetTransformationType())
		var source_channels []chan interface{}

		log.Printf("layer %v : %v : Getting source channels\n", id, operator.Transformer.GetTransformationType())
		if id == 0 {
			log.Printf("layer %v : %v : initializing source channels\n", id, operator.Transformer.GetTransformationType())
			for range operator.Parallelism {
				source_channel_initializer := make(chan interface{})
				source_channels = append(source_channels, source_channel_initializer)
			}
		} else {
			log.Printf("layer %v : %v : chaining source channels from last result channels \n", id, operator.Transformer.GetTransformationType())
			// get last result channels
			last_result_channels := all_result_channels[len(all_result_channels)-1]
			// using operator chainer
			// to merge or direct or distribute channels into new parallelism that fits current operator
			log.Printf("layer %v : %v : Executing Operator chainer \n", id, operator.Transformer.GetTransformationType())
			source_channels = operator.Chainer.ExecuteChaining(&wg, last_result_channels, operator.Parallelism)
		}

		log.Printf("layer %v : %v : starting operator parallel instances\n", id, operator.Transformer.GetTransformationType())
		var result_channels []chan interface{}
		for parallel_id := range operator.Parallelism {
			log.Printf("layer %v : %v : starting parallel instance %v", id, operator.Transformer.GetTransformationType(), parallel_id)
			result_channel := operator.Transformer.ExecuteTransformation(&wg, source_channels[parallel_id])
			result_channels = append(result_channels, result_channel)
		}
		all_result_channels = append(all_result_channels, result_channels)
	}
	wg.Wait()

}
