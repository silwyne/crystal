package functions

import (
	"github.com/crystal/api/row"
)

type MapTransformation struct {
	MapFunction func(row.Row) (row.Row, bool)
}

func (m MapTransformation) Execute(source_channel chan row.Row, result_channel chan row.Row) {
	for input := range source_channel {
		data, ok := m.MapFunction(input)
		if ok {
			result_channel <- data
		}
	}
}

func (MapTransformation) IsResultStateless() bool {
	return true
}

func (MapTransformation) GetName() string {
	return "MAP"
}
