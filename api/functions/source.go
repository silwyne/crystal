package functions

import (
	"sync"

	"github.com/crystal/api/row"
)

type SourceTransformation struct {
	SourceFunction func() (row.Row, bool)
}

func (s SourceTransformation) Execute(wg *sync.WaitGroup, source_channel chan row.Row) chan row.Row {
	wg.Add(1)
	result_channel := make(chan row.Row)
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

func (SourceTransformation) IsResultStateless() bool {
	return true
}

func (SourceTransformation) GetName() string {
	return "SOURCE"
}
