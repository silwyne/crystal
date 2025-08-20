package chainer

import (
	"sync"

	"github.com/Silwyne/crystal/api/row"
)

type OperatorChainer interface {
	ExecuteChaining(wg *sync.WaitGroup, input_channels []chan row.Row, num_output_channels int) []chan row.Row
	GetChainingType() ChainingType
}

type ChainingType string

const (
	DIRECT_CHAIN ChainingType = "DIRECT_CHAIN"
)
