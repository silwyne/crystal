package functions

import (
	"log"

	"github.com/crystal/api/operation/signal"
	"github.com/crystal/api/row"
)

type MapTransformation struct {
	MapFunction func(row.Row) (row.Row, error)
}

func (m MapTransformation) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	for input := range source_channel {
		data, err := m.MapFunction(input)
		if err != nil {
			log.Panicf("Error in MapFunction: %v\n", err)
			return signal.FAILURE
		}
		result_channel <- data
	}
	return signal.SUCCESS
}

func (MapTransformation) IsResultStateless() bool {
	return true
}

func (MapTransformation) GetName() string {
	return "MAP"
}
