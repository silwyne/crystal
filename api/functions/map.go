package functions

import (
	"sync"

	"github.com/crystal/api/operation"
)

type MapTransformation struct {
	MapFunction func(interface{}) (interface{}, bool)
}

func (m MapTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	result_channel := make(chan interface{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			data, ok := m.MapFunction(input)
			if ok {
				result_channel <- data
			}
		}
		close(result_channel)
	}()
	return result_channel
}

func (m MapTransformation) GetTransformationType() operation.TransformationType {
	return operation.MAP
}
