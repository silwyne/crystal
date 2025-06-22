package operation

type Operator struct {
	Transformer Transformation
	Parallelism int
	Chainer     OperatorChainer
}

func NewOperator(transformer Transformation, parallelism int) Operator {
	theChainer := NewDirectOperatorChainer()
	return Operator{
		Transformer: transformer,
		Parallelism: parallelism,
		Chainer:     theChainer,
	}
}

func (o *Operator) SetParallelism(parallalism int) {
	o.Parallelism = parallalism
}

func (o *Operator) SetChainer(chainer OperatorChainer) {
	o.Chainer = chainer
}
