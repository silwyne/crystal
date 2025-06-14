package functions

import "sync"

type SinkFunction func(interface{}) bool

type SinkTransformation struct {
	Function SinkFunction
}

func (s SinkTransformation) Apply(data interface{}) (interface{}, bool) {
	resultBool := s.Function(data)
	return nil, resultBool
}

func (s SinkTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			_, ok := s.Apply(input)
			if !ok {
				panic("error while sinking")
			}
		}
	}()
	return nil
}
