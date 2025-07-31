package operation

import (
	"log"

	"github.com/crystal/api/operation/queue"
	"github.com/crystal/api/operation/signal"
	"github.com/crystal/api/row"
)

type Transformation interface {
	Apply(source chan row.Row, result chan row.Row) signal.Signal
	IsResultStateless() bool
	GetName() string
}

type Operator struct {
	transformation Transformation
	queueConfig    queue.QueueConfiguration
	parallelism    int
	chaining       bool
}

func NewOperator(transformation Transformation, parallelism int) Operator {
	return Operator{
		transformation: transformation,
		queueConfig:    queue.NewDefaultQueueConfiguration(),
		parallelism:    parallelism,
		chaining:       transformation.IsResultStateless(),
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

func (o *Operator) SetChaining(chaining bool) {
	if !o.chaining && chaining {
		log.Printf("The operator can not be chained, request for making it chainable will get ignored")
		return
	}
	o.chaining = chaining
}

func (o *Operator) GetChaining() bool {
	return o.chaining
}
