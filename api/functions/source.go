package functions

import (
	"sync"

	"github.com/crystal/api/operation"
)

type SourceTransformation struct {
	SourceFunction func() (interface{}, bool)
}

func (s SourceTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			data, ok := s.SourceFunction()
			if ok {
				source_channel <- data
			}
		}
	}()
	return source_channel // sending as result channel
}

func (s SourceTransformation) GetTransformationType() operation.TransformationType {
	return operation.SOURCE
}
