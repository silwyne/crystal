package operation

import (
	"github.com/crystal/api/operation/queue"
)

type Operator struct {
	transformation Transformation
	queueConfig    queue.QueueConfiguration
	parallelism    int
}

func NewOperator(transformation Transformation, parallelism int) Operator {
	return Operator{
		transformation: transformation,
		queueConfig:    queue.NewDefaultQueueConfiguration(),
		parallelism:    parallelism,
	}
}

func (o *Operator) SetParallelism(parallalism int) {
	o.parallelism = parallalism
}

func (o *Operator) GetParallelism() int {
	return o.parallelism
}

func (o *Operator) GetTransformation() Transformation {
	return o.transformation
}

func (o *Operator) GetQueueConfig() *queue.QueueConfiguration {
	return &o.queueConfig
}
