package functions

import "sync"

type MapFunction func(interface{}) (interface{}, bool)

type MapTransformation struct {
	Function MapFunction
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
			data, ok := m.Function(input)
			if ok {
				result_channel <- data
			}
		}
		close(result_channel)
	}()
	return result_channel
}

func (m MapTransformation) GetName() string {
	return "MapTransformation"
}
