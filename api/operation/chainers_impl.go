package operation

import "sync"

type DirectOperatorChainer struct{}

func NewDirectOperatorChainer() DirectOperatorChainer {
	return DirectOperatorChainer{}
}

func (dc DirectOperatorChainer) ExecuteChaining(wg *sync.WaitGroup,
	input_channels []chan interface{}, num_output_channels int) []chan interface{} {
	if len(input_channels) != num_output_channels {
		panic("DirectOperatorChainer can not get used if input channels number is not equal with output channels number")
	}
	return input_channels
}

func (dc DirectOperatorChainer) GetChainingType() ChainingType {
	return DIRECT_CHAIN
}
