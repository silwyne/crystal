package chainer

import (
	"sync"

	"github.com/crystal/api/row"
)

type DirectOperatorChainer struct{}

func NewDirectOperatorChainer() DirectOperatorChainer {
	return DirectOperatorChainer{}
}

func (dc DirectOperatorChainer) ExecuteChaining(wg *sync.WaitGroup,
	input_channels []chan row.Row, num_output_channels int) []chan row.Row {
	if len(input_channels) != num_output_channels {
		panic("DirectOperatorChainer can not get used if input channels number is not equal with output channels number")
	}
	return input_channels
}

func (dc DirectOperatorChainer) GetChainingType() ChainingType {
	return DIRECT_CHAIN
}
