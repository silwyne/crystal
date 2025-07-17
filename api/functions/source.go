package functions

import (
	"github.com/crystal/api/row"
)

type SourceTransformation struct {
	SourceFunction func() (row.Row, error)
}

func (s SourceTransformation) Apply(source_channel chan row.Row, result_channel chan row.Row) {
	for {
		data, err := s.SourceFunction()
		if err != nil {
			panic(err)
		}
		result_channel <- data
	}
}

func (s SourceTransformation) IsResultStateless() bool {
	return true
}

func (s SourceTransformation) GetName() string {
	return "SOURCE"
}
