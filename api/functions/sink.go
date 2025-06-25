package functions

import (
	"sync"

	"github.com/crystal/api/row"
)

type SinkTransformation struct {
	SinkFunction func(row.Row)
}

func (s SinkTransformation) Execute(wg *sync.WaitGroup, source_channel chan row.Row) chan row.Row {
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

func (SinkTransformation) GetName() string {
	return "SINK"
}
