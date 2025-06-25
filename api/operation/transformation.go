package operation

import "sync"

type Transformation[IN any, OUT any] interface {
	Execute(wg *sync.WaitGroup, source chan IN) chan OUT
	GetName() string
}
