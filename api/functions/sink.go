package functions

import (
	"sync"
)

type SinkTransformation[IN any, OUT any] struct {
	SinkFunction func(IN)
}

func (s SinkTransformation[IN, OUT]) Execute(wg *sync.WaitGroup, source_channel chan IN) chan OUT {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			s.SinkFunction(input)
		}
	}()
	return nil
}

func (SinkTransformation[IN, OUT]) GetName() string {
	return "SINK"
}
