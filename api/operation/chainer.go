package operation

type OperatorChainer interface {
	ExecuteChaining(input_channels []chan interface{}, num_output_channels int) []chan interface{}
	GetChainingType() ChainingType
}

type ChainingType string

const (
	DIRECT_CHAIN ChainingType = "DIRECT_CHAIN"
)
