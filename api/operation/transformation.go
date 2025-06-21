package operation

import "sync"

type Operator struct {
	Transformer Transformation
	Parallelism int
}

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

func NewOperator(transformer Transformation, parallelism int) Operator {
	return Operator{
		Transformer: transformer,
		Parallelism: parallelism,
	}
}

func (o *Operator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}
