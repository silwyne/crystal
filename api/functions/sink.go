package functions

import (
	"github.com/crystal/api/row"
)

type SinkTransformation struct {
	SinkFunction func(row.Row)
}

func (s SinkTransformation) Execute(source_channel chan row.Row, result_channel chan row.Row) {
	for input := range source_channel {
		s.SinkFunction(input)
	}
}

func (SinkTransformation) IsResultStateless() bool {
	return true
}

func (SinkTransformation) GetName() string {
	return "SINK"
}
