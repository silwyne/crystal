package transformation

import "sync"

type Transformation interface {
	ExecuteTransformation(wg *sync.WaitGroup, source_channel chan interface{}) chan interface{}
	GetTransformationType() TransformationType
}

type TransformationType string

const (
	SOURCE  TransformationType = "SOURCE"
	MAP     TransformationType = "MAP"
	FLATMAP TransformationType = "FLATMAP"
	SINK    TransformationType = "SINK"
)
