package functions

import (
	"github.com/crystal/api/row"
)

type SourceTransformation struct {
	SourceFunction func() (row.Row, bool)
}

func (s SourceTransformation) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	for {
		data, ok := s.SourceFunction()
		if ok {
			result_channel <- data
		}
	}
}

func (s SourceTransformation) IsResultStateless() bool {
	return true
}

func (s SourceTransformation) GetName() string {
	return "SOURCE"
}
