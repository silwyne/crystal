package functions

import (
	"sync"

	"github.com/crystal/api/row"
)

type FlatMapTransformation struct {
	FlatMapFunction func(row.Row) ([]row.Row, bool)
}

func (fm FlatMapTransformation) Execute(wg *sync.WaitGroup, source_channel chan row.Row) chan row.Row {
	if source_channel == nil {
		panic("source channel can not be null")
	}
	result_channel := make(chan row.Row)
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

func (FlatMapTransformation) GetName() string {
	return "FLAT_MAP"
}
