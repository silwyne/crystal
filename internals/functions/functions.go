package functions

import "sync"

type Transformation interface {
	Apply(data interface{}) (interface{}, bool)
	ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{}
}
