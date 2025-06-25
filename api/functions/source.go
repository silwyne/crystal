package functions

import (
	"github.com/crystal/api/row"
)

type SourceTransformation struct {
	SourceFunction func() (row.Row, bool)
}

func (s SourceTransformation) Execute(source_channel chan row.Row, result_channel chan row.Row) {
	for {
		data, ok := s.SourceFunction()
		if ok {
			result_channel <- data
		}
	}
}

func (SourceTransformation) IsResultStateless() bool {
	return true
}

func (SourceTransformation) GetName() string {
	return "SOURCE"
}
