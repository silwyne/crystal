package functions

import (
	"sync"

	"github.com/crystal/api/row"
)

type MapTransformation struct {
	MapFunction func(row.Row) (row.Row, bool)
}

func (m MapTransformation) Execute(wg *sync.WaitGroup, source_channel chan row.Row) chan row.Row {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	result_channel := make(chan row.Row)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for input := range source_channel {
			data, ok := m.MapFunction(input)
			if ok {
				result_channel <- data
			}
		}
		close(result_channel)
	}()
	return result_channel
}

func (MapTransformation) IsResultStateless() bool {
	return true
}

func (MapTransformation) GetName() string {
	return "MAP"
}
