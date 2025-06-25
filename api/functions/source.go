package functions

import (
	"sync"
)

type SourceTransformation[IN any, OUT any] struct {
	SourceFunction func() (OUT, bool)
}

func (s SourceTransformation[IN, OUT]) Execute(wg *sync.WaitGroup, source_channel chan IN) chan OUT {
	wg.Add(1)
	result_channel := make(chan OUT)
	go func() {
		defer wg.Done()
		for {
			data, ok := s.SourceFunction()
			if ok {
				result_channel <- data
			}
		}
	}()
	return result_channel
}

func (SourceTransformation[IN, OUT]) GetName() string {
	return "SOURCE"
}
