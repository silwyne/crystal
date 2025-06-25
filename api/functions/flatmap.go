package functions

import (
	"sync"
)

type FlatMapTransformation[IN any, OUT any] struct {
	FlatMapFunction func(IN) ([]OUT, bool)
}

func (fm FlatMapTransformation[IN, OUT]) Execute(wg *sync.WaitGroup, source_channel chan IN) chan OUT {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	result_channel := make(chan OUT)
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

func (FlatMapTransformation[IN, OUT]) GetName() string {
	return "FLAT_MAP"
}
