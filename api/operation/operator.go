package operation

import "github.com/crystal/api/operation/chainer"

type Operator struct {
	transformation Transformation
	queueConfig    QueueConfiguration
	parallelism    int
	chainer        chainer.OperatorChainer
}

func NewOperator(transformation Transformation, parallelism int) Operator {
	theChainer := chainer.NewDirectOperatorChainer()
	return Operator{
		transformation: transformation,
		queueConfig:    NewDefaultQueueConfiguration(),
		parallelism:    parallelism,
		chainer:        theChainer,
	}
}

func (o *Operator) SetParallelism(parallalism int) {
	o.parallelism = parallalism
}

func (o *Operator) GetParallelism() int {
	return o.parallelism
}

func (o *Operator) SetChainer(chainer chainer.OperatorChainer) {
	o.chainer = chainer
}

func (o *Operator) GetChainer() chainer.OperatorChainer {
	return o.chainer
}

func (o *Operator) GetTransformation() Transformation {
	return o.transformation
}

func (o *Operator) GetQueueConfig() *QueueConfiguration {
	return &o.queueConfig
}
