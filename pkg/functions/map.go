package functions

import "sync"

type MapFunction func(interface{}) (interface{}, bool)

type MapTransformation struct {
	Function MapFunction
}

func (m MapTransformation) Apply(data interface{}) (interface{}, bool) {
	result, boolResult := m.Function(data)
	return result, boolResult
}

func (m MapTransformation) GetResultStreamType() DataStreamType {
	return MapStream
}

func (m MapTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	result_channel := make(chan interface{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			data, ok := m.Apply(input)
			if ok {
				result_channel <- data
			}
		}
		close(result_channel)
	}()
	return result_channel
}
