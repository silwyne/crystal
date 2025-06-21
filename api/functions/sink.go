package functions

import (
	"process-engine/api/operation"
	"sync"
)

type SinkFunction func(interface{}) bool

type SinkTransformation struct {
	Function SinkFunction
}

func (s SinkTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			ok := s.Function(input)
			if !ok {
				panic("error while sinking")
			}
		}
	}()
	return nil
}

func (s SinkTransformation) GetTransformationType() operation.TransformationType {
	return operation.SINK
}
