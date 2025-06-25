package functions

import (
	"github.com/crystal/api/row"
)

type FlatMapTransformation struct {
	FlatMapFunction func(row.Row) ([]row.Row, bool)
}

func (fm FlatMapTransformation) Execute(source_channel chan row.Row, result_channel chan row.Row) {
	for input := range source_channel {
		flatten_data, ok := fm.FlatMapFunction(input)
		if ok {
			for _, data := range flatten_data {
				result_channel <- data
			}
		}
	}
}

func (FlatMapTransformation) IsResultStateless() bool {
	return true
}

func (FlatMapTransformation) GetName() string {
	return "FLAT_MAP"
}
