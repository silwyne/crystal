package functions

import (
	"sync"

	"github.com/crystal/api/operation"
)

type FlatMapTransformation struct {
	FlatMapFunction func(interface{}) ([]interface{}, bool)
}

func (fm FlatMapTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	result_channel := make(chan interface{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			flatten_data, ok := fm.FlatMapFunction(input)
			if ok {
				for _, data := range flatten_data {
					result_channel <- data
				}
			}
		}
		close(result_channel)
	}()
	return result_channel
}

func (fm FlatMapTransformation) GetTransformationType() operation.TransformationType {
	return operation.FLATMAP
}
