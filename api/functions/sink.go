package functions

import (
	"github.com/crystal/api/operation/signal"
	"github.com/crystal/api/row"
)

type SinkTransformation struct {
	SinkFunction func(row.Row)
}

func (s SinkTransformation) Apply(source_channel chan row.Row, result_channel chan row.Row) signal.Signal {
	for input := range source_channel {
		s.SinkFunction(input)
	}
	return signal.SUCCESS
}

func (SinkTransformation) IsResultStateless() bool {
	return true
}

func (SinkTransformation) GetName() string {
	return "SINK"
}
