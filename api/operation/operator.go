package operation

import "github.com/crystal/api/operation/chainer"

type Operator struct {
	Transformer Transformation[any, any]
	Parallelism int
	Chainer     chainer.OperatorChainer
}

func NewOperator(transformer Transformation[any, any], parallelism int) Operator {
	theChainer := chainer.NewDirectOperatorChainer()
	return Operator{
		Transformer: transformer,
		Parallelism: parallelism,
		Chainer:     theChainer,
	}
}

func (o *Operator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}

func (o *Operator) SetChainer(chainer chainer.OperatorChainer) {
	o.Chainer = chainer
}
