package functions

import (
	"process-engine/api/transformation"
	"sync"
)

type SourceFunction func() (interface{}, bool)

type SourceTransformation struct {
	Function SourceFunction
}

func (s SourceTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			data, ok := s.Function()
			if !ok {
				break
			}
			source_channel <- data
		}
		close(source_channel)
	}()
	return source_channel // sending as result channel
}

func (s SourceTransformation) GetTransformationType() transformation.TransformationType {
	return transformation.SOURCE
}
