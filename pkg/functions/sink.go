package functions

type SinkFunction func(interface{})

type SinkTransformation struct {
	Function SinkFunction
}

func (s SinkTransformation) Apply(data interface{}) (interface{}, bool) {
	s.Function(data)
	return nil, true
}
