package functions

import "sync"

type SourceFunction func() (interface{}, bool)

type SourceTransformation struct {
	Function SourceFunction
}

func (s SourceTransformation) Apply(data interface{}) (interface{}, bool) {
	result, boolResult := s.Function()
	return result, boolResult
}

func (s SourceTransformation) GetResultStreamType() DataStreamType {
	return SourceStream
}

func (s SourceTransformation) ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{} {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			data, ok := s.Apply(nil)
			if !ok {
				break
			}
			source_channel <- data
		}
		close(source_channel)
	}()
	return source_channel // sending as result channel
}
