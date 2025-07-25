package core

import (
	"log"
	"sync"

	"github.com/crystal/api/configuration"
	"github.com/crystal/api/core/chainer"
	"github.com/crystal/api/datastream"
	"github.com/crystal/api/operation"
	"github.com/crystal/api/operation/queue"
	"github.com/crystal/api/row"
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

func (se *StreamEnvironment) FromSource(source operation.Transformation) *datastream.DataStream {
	stream := datastream.DataStream{}
	stream.SetConfigs(se.configs)
	streamWithTransformation := stream.AddTransformation(source)
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
			if operator.GetParallelism() != operators[id+1].GetParallelism() {
				panic("only supporting DIRECT_CHAIN between operators so all operators parallelism must be same")
			}
		}
	}

	log.Printf("ignoring operators parallelism using global parallelism %v\n", se.configs.GlobalParallelism)
	se.runLayers(operators)
}

func (se *StreamEnvironment) runLayers(operators []operation.Operator) {
	var wg sync.WaitGroup
	var all_result_channels [][]chan row.Row
	for id, operator := range operators {

		log.Printf("layer %v : %v : Starting\n", id, operator.GetTransformation().GetName())
		log.Printf("layer %v : %v : Getting source channels\n", id, operator.GetTransformation().GetName())
		source_channels := getSourceChannels(&wg, id, &operator, all_result_channels)

		log.Printf("layer %v : %v : starting operator parallel instances\n", id, operator.GetTransformation().GetName())
		result_channels := startLayer(&wg, id, &operator, source_channels)
		all_result_channels = append(all_result_channels, result_channels)
	}
	wg.Wait()

}

func getSourceChannels(wg *sync.WaitGroup, id int, operator *operation.Operator, all_result_channels [][]chan row.Row) []chan row.Row {
	var source_channels []chan row.Row
	if id == 0 {
		log.Printf("layer %v : %v : initializing source channels\n", id, operator.GetTransformation().GetName())
		for range operator.GetParallelism() {
			source_channel_initializer := make(chan row.Row)
			source_channels = append(source_channels, source_channel_initializer)
		}
	} else {
		log.Printf("layer %v : %v : chaining source channels from last result channels \n", id, operator.GetTransformation().GetName())

		last_result_channels := all_result_channels[len(all_result_channels)-1]

		log.Printf("layer %v : %v : Executing Operator chainer \n", id, operator.GetTransformation().GetName())
		chainer := chainer.NewDirectOperatorChainer()
		source_channels = chainer.ExecuteChaining(wg, last_result_channels, operator.GetParallelism())
	}
	return source_channels
}

func startLayer(wg *sync.WaitGroup, id int, operator *operation.Operator, source_channels []chan row.Row) []chan row.Row {
	var result_channels []chan row.Row
	for parallel_id := range operator.GetParallelism() {
		log.Printf("layer %v : %v : starting parallel instance %v", id, operator.GetTransformation().GetName(), parallel_id)
		wg.Add(1)
		queue_config := operator.GetQueueConfig()
		result_channel := queue.MakeQueue(queue_config)
		go func() {
			defer wg.Done()
			operator.GetTransformation().Apply(source_channels[parallel_id], result_channel)
			/* TODO: each Apply(source_chan, result_chan) returns a signal of FAILURE or SUCCESS
			Please Implement of how to handle the signal
			*/
		}()
		result_channels = append(result_channels, result_channel)
	}
	return result_channels
}
