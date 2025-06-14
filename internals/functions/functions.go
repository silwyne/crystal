package functions

import "sync"

type Transformation interface {
	ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{}
}
